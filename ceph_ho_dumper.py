#!/usr/bin/env python3.5
import logging
import os
import sys
import bz2

import math
import stat
import json
import time
import zlib
import heapq
import struct
import socket
import os.path
import argparse
import datetime
import subprocess
from enum import Enum
from struct import Struct
import concurrent.futures
from functools import partial
from collections import defaultdict

from typing.io import BinaryIO, TextIO
from typing import List, Dict, Tuple, Iterator, Any, Optional, Union, Callable, Set


logger = logging.getLogger('ops dumper')

# ----------- CONSTANTS ------------------------------------------------------------------------------------------------

MKS_TO_MS = 1000
S_TO_MS = 1000

MAX_SRC_VL = 100000
MIN_SRC_VL = 1
MAX_TARGET_VL = 254
MIN_TARGET_VL = 1
TARGET_UNDERVAL = 0
TARGET_OVERVAL = 255
MAX_PG_VAL = (2 ** 16 - 1)
MAX_POOL_VAL = 63
UNDISCRETIZE_UNDERVAL = 0
UNDISCRETIZE_OVERVAL = MAX_SRC_VL

assert MIN_SRC_VL >= 1
assert MAX_SRC_VL > MIN_SRC_VL
assert TARGET_OVERVAL > MAX_TARGET_VL > MIN_TARGET_VL > TARGET_UNDERVAL
assert UNDISCRETIZE_UNDERVAL <= MIN_SRC_VL < MAX_SRC_VL <= UNDISCRETIZE_OVERVAL

SCALE_COEF = (MAX_TARGET_VL - MIN_TARGET_VL) / (math.log10(MAX_SRC_VL) - math.log10(MIN_SRC_VL))


class OP(Enum):
    READ = 0
    WRITE_PRIMARY = 1
    WRITE_SECONDARY = 2


class REC_ID(Enum):
    OPS = 1
    POOLS = 2
    CLUSTER_INFO = 3


HEADER = b"OSD OPS LOG v1.1\0"


class NoPoolFound(Exception):
    pass


# ----------- UTILS ----------------------------------------------------------------------------------------------------


def discretize(vl: float) -> int:
    if vl > MAX_SRC_VL:
        return TARGET_OVERVAL
    if vl < MIN_SRC_VL:
        return TARGET_UNDERVAL
    assert vl >= 0
    res = round(math.log10(vl) * SCALE_COEF) + MIN_TARGET_VL
    assert MAX_TARGET_VL >= res >= MIN_TARGET_VL
    return res


def undiscretize(vl: int) -> float:
    if vl == TARGET_UNDERVAL:
        return UNDISCRETIZE_UNDERVAL
    if vl == TARGET_OVERVAL:
        return UNDISCRETIZE_OVERVAL
    return 10 ** ((vl - MIN_TARGET_VL) / SCALE_COEF)


undiscretize_map = {vl: undiscretize(vl) for vl in range(256)}  # type: Dict[int, float]


def to_unix_ms(dtm: str) -> int:
    # "2019-02-03 20:53:47.429996"
    date, tm = dtm.split()
    y, m, d = date.split("-")
    h, min, smks = tm.split(':')
    s, mks = smks.split(".")
    dt = datetime.datetime(int(y), int(m), int(d), int(h), int(min), int(s))
    return int(time.mktime(dt.timetuple()) * S_TO_MS) + int(mks) // MKS_TO_MS


# --------- CEPH UTILS -------------------------------------------------------------------------------------------------


def get_timings(op: Dict[str, Any]) -> Tuple[int, int, int, int, int, int, int]:
    initiated_at = to_unix_ms(op['initiated_at'])
    stages = {evt["event"]: to_unix_ms(evt["time"]) - initiated_at
              for evt in op["type_data"]["events"] if evt["event"] != "initiated"}

    qpg_at = stages.get("queued_for_pg", -1)

    try:
        started = stages["started"]
    except KeyError:
        started = stages.get("reached_pg", -1)

    try:
        local_done = stages["op_applied"] if 'op_applied' in stages else stages["done"]
    except KeyError:
        local_done = -1

    replicas_waiting = -1
    subop = -1
    for evt, tm in stages.items():
        if evt.startswith("waiting for subops from"):
            replicas_waiting = tm
        elif evt.startswith("sub_op_commit_rec from"):
            subop = max(tm, subop)

    op_duration = int(op['duration']) // MKS_TO_MS
    return op_duration, initiated_at, qpg_at, started, local_done, replicas_waiting, subop


def get_ids(expected_class: str = None) -> Set[int]:
    hostname = socket.gethostname()
    osds = set()
    in_host = False

    for line in subprocess.check_output("ceph osd tree".split()).decode("utf8").split("\n"):
        if 'host ' + hostname in line:
            in_host = True
            continue
        if in_host and ' host ' in line:
            break

        if in_host and line.strip():
            osd_id, maybe_class = line.split()[:2]
            try:
                float(maybe_class)
                maybe_class = None
            except ValueError:
                pass

            if expected_class and expected_class != maybe_class:
                continue

            osds.add(int(osd_id))

    return osds


def set_size_duration(osd_ids: Set[int], size: int, duration: int) -> Set[int]:
    not_inited_osd = set()
    for osd_id in osd_ids:
        try:
            cmd = "ceph daemon osd.{} config set osd_op_history_duration {}"
            out = subprocess.check_output(cmd.format(osd_id, duration).split(), stderr=subprocess.STDOUT).decode("utf8")
            assert "success" in out
            cmd = "ceph daemon osd.{} config set osd_op_history_size {}"
            out = subprocess.check_output(cmd.format(osd_id, size).split(), stderr=subprocess.STDOUT).decode("utf8")
            assert "success" in out
        except subprocess.CalledProcessError:
            not_inited_osd.add(osd_id)
    return not_inited_osd


def get_type(op: Dict[str, Any]) -> Optional[OP]:
    description = op['description']
    op_type_s, _ = description.split("(", 1)

    is_read = is_write = False

    if '+write+' in description:
        is_write = True
    elif '+read+' in description:
        is_read = True

    if op_type_s == 'osd_op':
        if is_write:
            return OP.WRITE_PRIMARY
        if is_read:
            return OP.READ
    elif op_type_s == 'osd_repop':
        assert not (is_read or is_write)
        return OP.WRITE_SECONDARY
    return None


def get_pool_pg(op: Dict[str, Any]):
    pool_s, pg_s = op['description'].split()[1].split(".")
    return int(pool_s), int(pg_s, 16)


OPRecortWP = Struct("!BHBBBBB")
OPRecortWS = Struct("!BHBBBB")
OPRecortR = Struct("!BHBBB")


def pack_to_bytes(op: Dict[str, Any], pools_map: Dict[int, int]) -> bytes:
    op_type = get_type(op)
    if op_type is None:
        return b""

    pool, pg = get_pool_pg(op)

    try:
        pool_log_id = pools_map[pool]
    except KeyError:
        raise NoPoolFound()

    assert 0 <= pool_log_id <= MAX_POOL_VAL

    # overflow pg
    pg = min(MAX_PG_VAL, pg)

    _, _, qpg_at, started, local_done, replicas_waiting, subop = get_timings(op)
    flags_and_pool = (op_type.value << 6) + pool_log_id
    assert flags_and_pool < 256
    wait_pg = started - qpg_at
    assert wait_pg >= 0
    local_io = local_done - started
    assert local_io >= 0

    duration = op['duration'] * S_TO_MS

    if op_type in (OP.WRITE_PRIMARY, OP.WRITE_SECONDARY):
        dload = qpg_at
        if op_type == OP.WRITE_PRIMARY:
            remote_io = subop - replicas_waiting
            assert remote_io >= 0
            assert dload >= 0
            assert remote_io >= 0
            return OPRecortWP.pack(flags_and_pool, pg,
                                   discretize(duration),
                                   discretize(wait_pg),
                                   discretize(dload),
                                   discretize(local_io),
                                   discretize(remote_io))
        else:
            return OPRecortWS.pack(flags_and_pool, pg,
                                   discretize(duration),
                                   discretize(wait_pg),
                                   discretize(dload),
                                   discretize(local_io))
    else:
        assert op_type == OP.READ
        return OPRecortR.pack(flags_and_pool, pg,
                              discretize(duration),
                              discretize(wait_pg),
                              discretize(local_io))


def unpack_from_bytes(data: bytes, offset: int, pool_map: Dict[int, Tuple[str, int]]) -> Tuple:
    undiscretize_l = undiscretize_map.__getitem__
    flags_and_pool = data[offset]
    op_type = OP(flags_and_pool >> 6)
    pool = pool_map[flags_and_pool & 0x3F]

    if op_type == OP.WRITE_PRIMARY:
        _, pg, dura, wait_pg, dload, local_io, remote_io = OPRecortWP.unpack(data[offset: offset + OPRecortWP.size])
        return op_type, pool, pg, undiscretize_l(dura), undiscretize_l(wait_pg), \
               undiscretize_l(dload), undiscretize_l(local_io), undiscretize_l(remote_io), \
               offset + OPRecortWP.size

    if op_type == OP.WRITE_SECONDARY:
        _, pg, dura, wait_pg, dload, local_io = OPRecortWS.unpack(data[offset: offset + OPRecortWS.size])
        return op_type, pool, pg, undiscretize_l(dura), undiscretize_l(wait_pg), \
               undiscretize_l(dload), undiscretize_l(local_io), None, offset + OPRecortWS.size

    assert op_type == OP.READ, str(op_type)

    _, pg, dura, wait_pg, local_io = OPRecortR.unpack(data[offset: offset + OPRecortR.size])
    return op_type, pool, pg, undiscretize_l(dura), undiscretize_l(wait_pg), \
           None, undiscretize_l(local_io), None, \
           offset + OPRecortR.size


def get_historic(osd_id: int, timeout: float = 15) -> str:
    cmd = "ceph daemon osd.{} dump_historic_ops".format(osd_id).split()
    return subprocess.check_output(cmd, timeout=timeout).decode("utf8")


def open_to_append(fname: str, is_bin: bool = False) -> Union[BinaryIO, TextIO]:
    if os.path.exists(fname):
        fd = open(fname, "rb+" if is_bin else "r+")
        fd.seek(0, os.SEEK_END)
    else:
        fd = open(fname, "wb" if is_bin else "w")
        os.chmod(fname, stat.S_IRGRP | stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH)
    return fd


RADOS_DF = 'rados df -f json'
PG_DUMP = 'ceph pg dump -f json 2>/dev/null'
CEPH_DF = 'ceph df -f json'
CEPH_S = 'ceph -s -f json'


# start them also for SIGUSR1
# how to guarantee that file would not be corrupted in case of daemon stopped during record
# при старте сканировать файл до конца последней полной записи
# при парсинге игнорировать частичную запись


def dump_cluster_info(commands: List[str], timeout: float = 15) -> Iterator[Tuple[REC_ID, bytes]]:
    output = {}
    for cmd in commands:
        try:
            output[cmd] = json.loads(subprocess.check_output(cmd, shell=True, timeout=timeout).decode('utf8'))
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass

    if output:
        yield REC_ID.CLUSTER_INFO, bz2.compress(json.dumps(output).encode('utf8'))


def write_record(fd: BinaryIO, rec_type: REC_ID, data: bytes):
    record = struct.pack("!B", rec_type.value) + data
    fd.write(struct.pack("!IL", zlib.adler32(record), len(data)))
    fd.write(record)

    try:
        fd.flush()
    except AttributeError:
        pass


def get_bytes(fd: BinaryIO, size: int) -> bytes:
    dt = fd.read(size)
    if len(dt) != size:
        raise UnexpectedEndOfFile()
    return dt


def read_header(fd: BinaryIO) -> bool:
    offset = fd.tell()
    fd.seek(0, os.SEEK_END)
    if fd.tell() == 0:
        return False

    fd.seek(offset, os.SEEK_SET)
    try:
        hdr = get_bytes(fd, len(HEADER))
    except UnexpectedEndOfFile:
        fd.seek(0)
        raise

    assert hdr == HEADER, "Header corrupted"
    return True


def iter_records(fd: BinaryIO) -> Iterator[Tuple[REC_ID, bytes]]:
    rec_header = Struct("!IL")

    offset = fd.tell()
    fd.seek(0, os.SEEK_END)
    size = fd.tell()
    fd.seek(offset, os.SEEK_SET)

    try:
        while offset < size:
            data = get_bytes(fd, rec_header.size)
            checksum, data_size = rec_header.unpack(data)
            data = get_bytes(fd, data_size + 1)
            assert checksum == zlib.adler32(data), "record corrupted at offset {}".format(offset)
            yield REC_ID(data[0]), data[1:]
            offset += rec_header.size + data_size + 1
    except:
        fd.seek(offset, os.SEEK_SET)
        raise


class CephDumper:
    def __init__(self, osd_ids: Set[int], size: int, duration: int, cmd_tout: int = 15) -> None:
        self.osd_ids = osd_ids
        self.not_inited_osd = osd_ids.copy()
        self.pools_map = {}  # type: Dict[int, Tuple[str, int]]
        self.pools_map_no_name = {}   # type: Dict[int, int]
        self.size = size
        self.duration = duration
        self.cmd_tout = cmd_tout
        self.last_time_ops = defaultdict(set)  # type: Dict[int, Set[str]]

    def reload_pools(self):
        self.pools_map.clear()
        self.pools_map_no_name.clear()

        output = subprocess.check_output("ceph osd lspools -f json".split(), timeout=self.cmd_tout)
        data = json.loads(output.decode("utf8"))

        for idx, pool in enumerate(sorted(data, key=lambda x: x['poolname'])):
            pool['idx'] = idx
            self.pools_map[pool['poolnum']] = (pool['poolname'], idx)
            self.pools_map_no_name[pool['poolnum']] = idx

    def dump_historic(self) -> Iterator[Tuple[REC_ID, bytes]]:
        if self.not_inited_osd:
            self.not_inited_osd = set_size_duration(self.not_inited_osd, self.size, self.duration)

        for osd_id in self.osd_ids:
            if osd_id not in self.not_inited_osd:
                try:
                    data = get_historic(osd_id)
                    # data = get_historic_fast(osd_id)
                except (subprocess.CalledProcessError, OSError):
                    self.not_inited_osd.add(osd_id)
                    continue

                try:
                    parsed = json.loads(data)
                except:
                    raise Exception(repr(data))

                if self.size != parsed['size'] or self.duration != parsed['duration']:
                    self.not_inited_osd.add(osd_id)
                    continue

                ops = [op for op in parsed['ops'] if op['description'] not in self.last_time_ops[osd_id]]
                self.last_time_ops[osd_id] = {op['description'] for op in ops}
                for rec_id, rec_data in self.pack_ops(ops, osd_id):
                    yield rec_id, rec_data

    op_header = Struct("!HI")

    def pack_ops(self, ops: List, osd_id: int) -> Iterator[Tuple[REC_ID, bytes]]:
        try:
            packed = [pack_to_bytes(op, self.pools_map_no_name) for op in ops]
        except NoPoolFound:
            self.reload_pools()
            yield REC_ID.POOLS, json.dumps(self.pools_map).encode('utf8')
            packed = [pack_to_bytes(op, self.pools_map_no_name) for op in ops]

        packed_s = b"".join(packed)
        if packed_s:
            yield REC_ID.OPS, struct.pack("!HI", osd_id, int(time.time())) + packed_s

    @classmethod
    def unpack_ops_header(cls, data: bytes) -> Tuple[int, int, int]:
        return (*cls.op_header.unpack(data[:cls.op_header.size]), cls.op_header.size)  # type: ignore


InfoFunc = Callable[[], Iterator[Tuple[REC_ID, bytes]]]


def to_list(handler: InfoFunc) -> List[Tuple[REC_ID, bytes]]:
    return list(handler())


def dump_loop(opts: Any, osd_ids: Set[int], fd: BinaryIO):
    handlers = []  # type: List[Tuple[str, InfoFunc, float]]

    if opts.record_cluster != 0:
        func = partial(dump_cluster_info, (RADOS_DF, CEPH_DF, CEPH_S), opts.timeout)
        handlers.append(("cluster_info", func, opts.record_cluster))

    if opts.record_pg_dump != 0:
        func = partial(dump_cluster_info, (PG_DUMP,), opts.timeout)
        handlers.append(("pg_dump", func, opts.record_pg_dump))

    dumper = CephDumper(osd_ids, opts.size, opts.duration, opts.timeout)

    handlers.append(("historic", dumper.dump_historic, opts.duration))

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(handlers))
    futures = {}  # type: Dict[concurrent.futures.Future, Tuple[int, float, float]]

    ctime = time.time()
    handlers_q = [(ctime, idx) for idx in range(len(handlers))]
    heapq.heapify(handlers_q)
    while True:
        stime = handlers_q[0][0] - ctime if handlers_q else 0.5

        if futures:
            done, _ = concurrent.futures.wait(list(futures.keys()),
                                              timeout=stime,
                                              return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in done:
                idx, next_time, prev_time = futures.pop(fut)
                name, _, tout = handlers[idx]
                for rec_id, data in fut.result(timeout=0):
                    logger.debug("Handler %s provides %s bytes of data of type %s", name, len(data), rec_id)
                    write_record(fd, rec_id, data)

                ctime = time.time()
                if next_time - ctime < ctime - prev_time:
                    next_time = ctime + tout
                heapq.heappush(handlers_q, (next_time, idx))
        else:
            time.sleep(stime)

        ctime = time.time()
        if handlers_q and handlers_q[0][0] <= ctime:
            stime, idx = heapq.heappop(handlers_q)
            _, func, tout = handlers[idx]
            future = executor.submit(to_list, func)
            futures[future] = (idx, stime, stime + tout)


def seek_to_last_valid_record(fd: BinaryIO) -> bool:
    try:
        has_header = read_header(fd)
    except (UnexpectedEndOfFile, AssertionError, ValueError):
        logger.warning("File header corrupted. Clear it")
        fd.seek(0, os.SEEK_SET)
        fd.truncate()
        return False

    if not has_header:
        return False

    try:
        for _ in iter_records(fd):
            pass
    except (UnexpectedEndOfFile, AssertionError, ValueError):
        logger.warning("File corrupted after offset %d. Truncate to last valid offset", fd.tell())
        fd.truncate()

    return True


def record_to_file(opts: Any) -> int:
    logger.info("Start recording with opts = %s", " ".join(sys.argv))
    try:
        osd_ids = get_ids()
        logger.info("osds = %s", osd_ids)

        fd = open_to_append(opts.output_file, True)

        with fd:
            fd.seek(0, os.SEEK_SET)
            has_header = seek_to_last_valid_record(fd)
            if not has_header:
                fd.write(HEADER)
                fd.flush()

            dump_loop(opts, osd_ids, fd)
    except Exception as exc:
        logger.exception("During recording")
        if isinstance(exc, OSError):
            return exc.errno
        return 1
    return 0


class UnexpectedEndOfFile(Exception): pass


def parse(fd: BinaryIO) -> Iterator[Tuple]:
    pools_map = {}  # type: Dict[int, Tuple[str, int]]

    if not read_header(fd):
        return

    for rec_type, data in iter_records(fd):

        if rec_type == REC_ID.POOLS:
            pools_map = {pool_id: (name, orig_id_s)
                         for orig_id_s, (name, pool_id) in json.loads(data.decode('utf8')).items()}
        elif rec_type == REC_ID.CLUSTER_INFO:
            data_decompressed = bz2.decompress(data).decode('utf8')
            result = json.loads(data_decompressed)
            print("Ceph info:", ", ".join(result))
        else:
            assert rec_type == REC_ID.OPS, "Unknown rec type {} at offset {}".format(rec_type, fd.tell())
            osd_id, stime, offset = CephDumper.unpack_ops_header(data)
            while offset != len(data):
                *params, offset = unpack_from_bytes(data, offset, pools_map)
                yield (osd_id, *params)


def format_op(osd_id: int, op_type: OP, pool_name: str, pg: int,
              dura: float, wait_pg: float, dload: float, local_io: float, remote_io: float):
    if op_type == OP.WRITE_PRIMARY:
        assert wait_pg is not None
        assert dload is not None
        assert local_io is not None
        assert remote_io is not None
        return (("WRITE_PRIMARY     {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   wait_pg={:>5d}   local_io={:>5d}" +
                 "   dload={:>5d}   remote_io={:>5d}").
                 format(pool_name, pg, osd_id, int(dura), int(wait_pg), int(dload), int(local_io), int(remote_io)))
    elif op_type == OP.WRITE_SECONDARY:
        assert wait_pg is not None
        assert dload is not None
        assert local_io is not None
        assert remote_io is None
        return (("WRITE_SECONDARY   {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   " +
                 "wait_pg={:>5d}   local_io={:>5d}   dload={:>5d}").
                format(pool_name, pg, osd_id, int(dura), int(wait_pg), int(dload), int(local_io)))
    elif op_type == OP.READ:
        assert wait_pg is not None
        assert dload is None
        assert local_io is not None
        assert remote_io is None
        return ("READ              {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   wait_pg={:>5d}   local_io={:>5d}".
                format(pool_name, pg, osd_id, int(dura), int(wait_pg), int(local_io)))
    else:
        assert False, "Unknown op"


def print_records_from_file(file: str, limit: Optional[int]):
    with open(file, "rb") as fd:
        for idx, (osd_id, op_type, (pool_name, _), pg, *times) in enumerate(parse(fd)):
            print(format_op(osd_id, op_type, pool_name, pg, *times))
            if limit is not None and idx == limit:
                break


ALLOWED_LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR']


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=ALLOWED_LOG_LEVELS, help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    set_parser = subparsers.add_parser('set', help="config osd's historic ops")
    set_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    set_parser.add_argument("--size", required=True, type=int, help="Num request to keep")

    record_parser = subparsers.add_parser('record', help="Dump osd's requests periodically")
    record_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    record_parser.add_argument("--size", required=True, type=int, help="Num request to keep")
    record_parser.add_argument("--timeout", type=int, default=30, help="Timeout to run cli cmds")
    record_parser.add_argument("--record-cluster", type=int, help="Record cluster info every SECONDS seconds",
                               metavar='SECONDS', default=0)
    record_parser.add_argument("--record-pg-dump", type=int, help="Record cluster pg dump info every SECONDS seconds",
                               metavar='SECONDS', default=0)
    record_parser.add_argument("output_file", help="Filename to append requests logs to it")

    parse_parser = subparsers.add_parser('parse', help="Parse records from file")
    parse_parser.add_argument("-l", "--limit", default=None, type=int, metavar="COUNT", help="Parse only COUNT records")
    parse_parser.add_argument("file", help="Log file")

    return parser.parse_args(argv[1:])


def setup_logger(opts):
    assert opts.log_level in ALLOWED_LOG_LEVELS
    logger.setLevel(getattr(logging, opts.log_level))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, opts.log_level))
    ch.setFormatter(formatter)

    if opts.log:
        fh = logging.FileHandler(opts.log)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logger.addHandler(ch)


def main(argv: List[str]):
    opts = parse_args(argv)
    setup_logger(opts)

    if opts.subparser_name == 'parse':
        print_records_from_file(opts.file, opts.limit)
        return 0

    if opts.subparser_name == 'set':
        set_size_duration(get_ids(), duration=opts.duration, size=opts.size)
        return 0

    assert opts.subparser_name == 'record'
    return record_to_file(opts)


if __name__ == "__main__":
    exit(main(sys.argv))
