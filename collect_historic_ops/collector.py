#!/usr/bin/env python3.5
import os
import re
import sys
import bz2
import abc
import stat
import json
import time
import zlib
import http
import bisect
import socket
import signal
import pprint
import asyncio
import os.path
import logging
import argparse
import datetime
import traceback
import subprocess
from enum import Enum
from io import BytesIO
from struct import Struct
from functools import partial
from collections import defaultdict
from typing.io import BinaryIO, TextIO
from typing import List, Dict, Tuple, Iterator, Any, Optional, Union, Callable, Set, Type, cast, NewType, NamedTuple, \
    Iterable, Awaitable, Match

logger = logging.getLogger('ops dumper')

# ----------- CONSTANTS ------------------------------------------------------------------------------------------------

MKS_TO_MS = 1000
S_TO_MS = 1000

DEFAULT_DURATION = 600
DEFAULT_SIZE = 20

MAX_PG_VAL = (2 ** 16 - 1)
MAX_POOL_VAL = 63

MAX_SLEEP_TIME = 0.01


compress = bz2.compress
decompress = bz2.decompress


class OpType(Enum):
    read = 0
    write_primary = 1
    write_secondary = 2


class RecId(Enum):
    ops = 1
    pools = 2
    cluster_info = 3
    params = 4
    packed = 5


class ParseResult(Enum):
    ok = 0
    failed = 1
    ignored = 2
    unknown = 3


HEADER_V11 = b"OSD OPS LOG v1.1\0"
HEADER_V12 = b"OSD OPS LOG v1.2\0"
HEADER_LAST = HEADER_V12
HEADER_LAST_NAME = cast(bytes, HEADER_LAST[:-1]).decode("ascii")
ALL_SUPPORTED_HEADERS = [HEADER_V11, HEADER_V12]
assert HEADER_LAST in ALL_SUPPORTED_HEADERS
HEADER_LEN = len(HEADER_LAST)
assert all(len(hdr) == HEADER_LEN for hdr in ALL_SUPPORTED_HEADERS), "All headers must have the same size"


class NoPoolFound(Exception):
    pass


class UnexpectedEOF(ValueError):
    pass


class UTExit(Exception):
    pass


# ----------- UTILS ----------------------------------------------------------------------------------------------------


class DiscretizerExt:
    # discretization constants
    overfloat = 255
    step_coef = 1.0372259

    val = 0
    table = [val]
    for _ in range(254):
        val = max(round(step_coef * val), val + 1)
        table.append(val)

    @classmethod
    def discretize(cls, vl: float) -> int:
        return min(cls.overfloat, bisect.bisect_left(cls.table, round(vl)))

    @classmethod
    def undiscretize(cls, vl: int) -> int:
        return cls.table[vl]


def to_unix_ms(dtm: str) -> int:
    # "2019-02-03 20:53:47.429996"
    date, tm = dtm.split()
    y, m, d = date.split("-")
    h, minute, smks = tm.split(':')
    s, mks = smks.split(".")
    dt = datetime.datetime(int(y), int(m), int(d), int(h), int(minute), int(s))
    return int(time.mktime(dt.timetuple()) * S_TO_MS) + int(mks) // MKS_TO_MS


def open_to_append(fname: str, is_bin: bool = False) -> Union[BinaryIO, TextIO]:
    if os.path.exists(fname):
        fd = open(fname, "rb+" if is_bin else "r+")
        fd.seek(0, os.SEEK_END)
    else:
        fd = open(fname, "wb" if is_bin else "w")
        os.chmod(fname, stat.S_IRGRP | stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH)
    return fd


# ----------------------  file records operating functions -------------------------------------------------------------


class RecordFile:
    rec_header = Struct("!II")

    def __init__(self, fd: BinaryIO, pack_each: int = 2 ** 20,
                 packer: Tuple[Callable[[bytes], bytes], Callable[[bytes], bytes]] = (compress, decompress)) -> None:
        if pack_each != 0:
            assert fd.seekable()

        self.fd = fd
        self.pack_each = pack_each
        self.compress, self.decompress = packer
        self.cached = []
        self.cache_size = 0
        self.unpacked_offset = 0

    def tell(self) -> int:
        return self.fd.tell()

    def read_file_header(self) -> Optional[bytes]:
        """
        read header from file, return fd positioned to first byte after the header
        check that header is in supported headers, fail otherwise
        must be called from offset 0
        """
        assert self.fd.seekable()
        assert self.fd.tell() == 0, "read_file_header must be called from beginning of the file"
        self.fd.seek(0, os.SEEK_END)
        size = self.fd.tell()
        if size == 0:
            return None

        assert self.fd.readable()
        self.fd.seek(0, os.SEEK_SET)
        assert size >= HEADER_LEN, "Incorrect header"
        hdr = self.fd.read(HEADER_LEN)
        assert hdr in ALL_SUPPORTED_HEADERS, "Unknown header {!r}".format(hdr)
        return hdr

    def make_header_for_rec(self, rec_type: RecId, data: bytes):
        id_bt = bytes((rec_type.value,))
        checksum = zlib.adler32(data, zlib.adler32(id_bt))
        return self.rec_header.pack(checksum, len(data) + 1) + id_bt

    def write_record(self, rec_type: RecId, data: bytes, flush: bool = True) -> None:
        header = self.make_header_for_rec(rec_type, data)
        truncate = False

        if self.pack_each != 0:
            if self.cache_size == 0:
                self.unpacked_offset = self.fd.tell()

            self.cached.extend([header, data])
            self.cache_size += len(header) + len(data)

            if self.cache_size >= self.pack_each:
                data = self.compress(b"".join(self.cached))
                header = self.make_header_for_rec(RecId.packed, data)
                logger.debug("Repack data orig size=%sKiB new_size=%sKiB",
                             self.cache_size // 1024, (len(header) + len(data)) // 1024)
                self.cached = []
                self.cache_size = 0
                self.fd.seek(self.unpacked_offset)
                truncate = True
                self.unpacked_offset = self.fd.tell()

        self.fd.write(header)
        self.fd.write(data)
        if truncate:
            self.fd.truncate()

        if flush:
            self.fd.flush()

    def iter_records(self) -> Iterator[Tuple[RecId, bytes]]:
        """
        iterate over records in output file, written with write_record function
        """

        rec_size = self.rec_header.size
        unpack = self.rec_header.unpack

        offset = self.fd.tell()
        self.fd.seek(0, os.SEEK_END)
        size = self.fd.tell()
        self.fd.seek(offset, os.SEEK_SET)

        try:
            while offset < size:
                data = self.fd.read(rec_size)
                if len(data) != rec_size:
                    raise UnexpectedEOF()
                checksum, data_size = unpack(data)
                data = self.fd.read(data_size)
                if len(data) != data_size:
                    raise UnexpectedEOF()
                assert checksum == zlib.adler32(data), "record corrupted at offset {}".format(offset)

                rec_id = RecId(data[0])
                data = data[1:]

                if rec_id == RecId.packed:
                    yield from RecordFile(BytesIO(self.decompress(data))).iter_records()  # type: ignore
                else:
                    yield rec_id, data

                offset += rec_size + data_size
        except Exception:
            self.fd.seek(offset, os.SEEK_SET)
            raise

    def seek_to_last_valid_record(self) -> Optional[bytes]:
        header = self.read_file_header()

        if header is None:
            return None

        try:
            for _ in self.iter_records():
                pass
        except (UnexpectedEOF, AssertionError, ValueError):
            logger.warning("File corrupted after offset %d. Truncate to last valid offset", self.fd.tell())
            self.fd.truncate()

        return header


# -------------------------   historic ops helpers ---------------------------------------------------------------------


OpRec = NewType('OpRec', Dict[str, Any])


def parse_events(op: OpRec, initiated_at: int) -> List[Tuple[str, int]]:
    return [(evt["event"], to_unix_ms(evt["time"]) - initiated_at)
            for evt in op["type_data"]["events"] if evt["event"] != "initiated"]


OpDescription = NamedTuple('OpDescription', [("type", OpType),
                                             ("client", str),
                                             ("pool_id", int),
                                             ("pg", int),
                                             ("obj_name", str),
                                             ("op_size", Optional[int])])


client_re = r"(?P<client>[^ ]*)"
pool_pg_re = r"(?P<pool>\d+)\.(?P<pg>[0-9abcdef]+)"
obj_name_re = r"(?P<obj_name>(?P=pool):[^ ]*)"

osd_op_re = re.compile(
    (r"osd_op\({client} {pool_pg} {obj_name} \[.*?\b(?P<op>(read|write|writefull)) " +
     r"(?P<start_offset>\d+)~(?P<end_offset>\d+).*?\] ").format(client=client_re,
                                                                  pool_pg=pool_pg_re,
                                                                  obj_name=obj_name_re))

osd_repop_re = re.compile(
    r"osd_repop\({client} {pool_pg} [^ ]* {obj_name} ".format(client=client_re,
                                                              pool_pg=pool_pg_re,
                                                              obj_name=obj_name_re))


def get_pool_pg(rr: Match) -> Tuple[int, int]:
    """
    returns pool and pg for op
    """
    return int(rr.group('pool')), int(rr.group('pg'), 16)


ignored_tps = {'delete', 'call', 'watch', 'stat', 'notify-ack'}
ignored_types = {"osd_repop_reply", "pg_info", "replica scrub", "rep_scrubmap", "pg_update_log_missing",
                 "MOSDScrubReserve"}


def parse_description(descr: str) -> Tuple[ParseResult, Optional[OpDescription]]:
    """
    Get type for operation
    """

    rr = osd_op_re.match(descr)
    if rr:
        pool, pg = get_pool_pg(rr)
        if rr.group('op') == 'read':
            tp = OpType.read
        else:
            assert rr.group('op') in ('write', 'writefull')
            tp = OpType.write_primary
        client = rr.group('client')
        object = rr.group('obj_name')
        size = int(rr.group('end_offset')) - int(rr.group('start_offset'))
        return ParseResult.ok, OpDescription(tp, client, pool, pg, object, size)

    rr = osd_repop_re.match(descr)
    if rr:
        pool, pg = get_pool_pg(rr)
        return ParseResult.ok, \
            OpDescription(OpType.write_secondary, rr.group('client'), pool, pg, rr.group('obj_name'), None)

    raw_tp = descr.split("(", 1)[0]
    if raw_tp in ignored_types:
        return ParseResult.ignored, None
    elif raw_tp == 'osd_op':
        for info in descr.split("[", 1)[1].split("]", 1)[0].split(","):
            if info.split()[0] in ignored_tps.intersection():
                return ParseResult.ignored, None

    return ParseResult.unknown, None


# ----------------------------------------------------------------------------------------------------------------------


HLTimings = NamedTuple('HLTimings', [("download", int),
                                     ("wait_for_pg", int),
                                     ("local_io", int),
                                     ("wait_for_replica", int)])


def get_hl_timings(tp: OpType, evt_map: Dict[str, int]) -> HLTimings:
    qpg_at = evt_map.get("queued_for_pg", -1)
    started = evt_map.get("started", evt_map.get("reached_pg", -1))

    try:
        if tp == OpType.write_secondary:
            local_done = evt_map["sub_op_applied"]
        elif tp == OpType.write_primary:
            local_done = evt_map["op_applied"]
        else:
            local_done = evt_map["done"]
    except KeyError:
        local_done = -1

    last_replica_done = -1
    subop = -1
    for evt, tm in evt_map.items():
        if evt.startswith("waiting for subops from") or evt == "wait_for_subop":
            last_replica_done = tm
        elif evt.startswith("sub_op_commit_rec from") or evt == "sub_op_commit_rec":
            subop = max(tm, subop)

    wait_for_pg = started - qpg_at
    assert wait_for_pg >= 0
    local_io = local_done - started
    assert local_io >= 0

    wait_for_replica = -1
    if tp in (OpType.write_primary, OpType.write_secondary):
        download = qpg_at
        if tp == OpType.write_primary:
            assert subop != -1
            assert last_replica_done != -1
            wait_for_replica = subop - last_replica_done
    else:
        download = -1

    return HLTimings(download=download, wait_for_pg=wait_for_pg, local_io=local_io,
                     wait_for_replica=wait_for_replica)


class CephOp:
    def __init__(self,
                 raw_data: OpRec,
                 description: str,
                 initiated_at: int,
                 obj_name: str,
                 client: str,
                 op_size: Optional[int],
                 tp: Optional[OpType],
                 duration: int,
                 pool_id: int,
                 pg: int,
                 events: List[Tuple[str, int]]) -> None:
        self.raw_data = raw_data
        self.description = description
        self.initiated_at = initiated_at
        self.tp = tp
        self.duration = duration
        self.pool_id = pool_id
        self.pack_pool_id = None  # type: Optional[int]
        self.pg = pg
        self.events = events
        self.evt_map = dict(events)
        self.client = client
        self.op_size = op_size
        self.obj_name = obj_name

    @classmethod
    def parse_op(cls, op: OpRec) -> Tuple[ParseResult, Optional['CephOp']]:
        try:
            res, descr = parse_description(op['description'])
        except AssertionError:
            descr = None
            res = ParseResult.failed

        if descr is None:
            return res, None

        initiated_at = to_unix_ms(op['initiated_at'])
        return res, cls(
            raw_data=op,
            description=op['description'],
            initiated_at=initiated_at,
            tp=descr.type,
            duration=int(op['duration'] * 1000),
            pool_id=descr.pool_id,
            pg=descr.pg,
            obj_name=descr.obj_name,
            client=descr.client,
            op_size=descr.op_size,
            events=parse_events(op, initiated_at))

    def get_hl_timings(self) -> HLTimings:
        assert self.tp is not None
        return get_hl_timings(self.tp, self.evt_map)

    def short_descr(self) -> str:
        return "{0.tp.name} pg={0.pool_id}.{0.pg:x} size={0.op_size} duration={0.duration}".format(self)

    def __str__(self) -> str:
        return ("{description}\n    initiated_at: {initiated_at}\n    tp: {tp}\n    duration: {duration}\n    " +
                "pool_id: {pool_id}\n    pack_pool_id: {pack_pool_id}\n    pg: {pg}\n    events:\n        "
                ).format(**self.__dict__) + \
                "\n        ".join("{}: {}".format(name, tm) for name, tm in sorted(self.evt_map.items()))


# ----------------------------------------------------------------------------------------------------------------------


class IPacker(metaclass=abc.ABCMeta):
    """
    Abstract base class to back ceph operations to bytes
    """
    name = None  # type: str

    @classmethod
    @abc.abstractmethod
    def pack_op(cls, op: CephOp) -> bytes:
        pass

    @classmethod
    @abc.abstractmethod
    def unpack_op(cls, data: bytes, offset: int) -> Tuple[Dict[str, Any], int]:
        pass

    @staticmethod
    @abc.abstractmethod
    def format_op(op: Dict[str, Any]) -> str:
        pass

    @classmethod
    def unpack(cls, rec_tp: RecId, data: bytes) -> Any:
        if rec_tp in (RecId.pools, RecId.params, RecId.cluster_info):
            return json.loads(data.decode('utf8'))
        elif rec_tp == RecId.ops:
            osd_id, ctime = cls.op_header.unpack(data[:cls.op_header.size])
            offset = cls.op_header.size
            ops = []
            while offset < len(data):
                params, offset = cls.unpack_op(data, offset)
                params.update({"osd_id": osd_id, "time": ctime})
                ops.append(params)
            return ops
        else:
            raise AssertionError("Unknown record type {}".format(rec_tp))

    op_header = Struct("!HI")

    @classmethod
    def pack_iter(cls, data_iter: Iterable[Tuple[RecId, Any]]) -> Iterator[Tuple[RecId, bytes]]:
        for rec_tp, data in data_iter:
            if rec_tp in (RecId.pools, RecId.cluster_info):
                assert isinstance(data, dict)
                yield rec_tp, json.dumps(data).encode('utf8')
            elif rec_tp == RecId.ops:
                osd_id, ctime, ops = data
                assert isinstance(osd_id, int)
                assert isinstance(ctime, int)
                assert isinstance(ops, list)
                assert all(isinstance(rec, CephOp) for rec in ops)
                packed = []  # type: List[bytes]
                for op in ops:
                    try:
                        packed.append(cls.pack_op(op))
                    except Exception:
                        logger.exception("Failed to pack op:\n{}".format(pprint.pformat(op.raw_data)))
                packed_b = b"".join(packed)
                if packed:
                    yield RecId.ops, cls.op_header.pack(osd_id, ctime) + packed_b
            else:
                raise AssertionError("Unknown record type {}".format(rec_tp))


class CompactPacker(IPacker):
    """
    Compact packer - pack op to 6-8 bytes with timings for high-level stages - downlaod, wait pg, local io, remote io
    """

    name = 'compact'

    OPRecortWP = Struct("!BHBBBBB")
    OPRecortWS = Struct("!BHBBBB")
    OPRecortR = Struct("!BHBBB")

    @classmethod
    def pack_op(cls, op: CephOp) -> bytes:
        assert op.pack_pool_id is not None
        assert 0 <= op.pack_pool_id <= MAX_POOL_VAL
        assert op.tp is not None

        # overflow pg
        if op.pg > MAX_PG_VAL:
            logger.debug("Too large pg = %d", op.pg)

        pg = min(MAX_PG_VAL, op.pg)

        timings = op.get_hl_timings()
        flags_and_pool = (cast(int, op.tp.value) << 6) + op.pack_pool_id
        assert flags_and_pool < 256

        if op.tp == OpType.write_primary:
            return cls.OPRecortWP.pack(flags_and_pool, pg,
                                       DiscretizerExt.discretize(op.duration),
                                       DiscretizerExt.discretize(timings.wait_for_pg),
                                       DiscretizerExt.discretize(timings.download),
                                       DiscretizerExt.discretize(timings.local_io),
                                       DiscretizerExt.discretize(timings.wait_for_replica))

        if op.tp == OpType.write_secondary:
            return cls.OPRecortWS.pack(flags_and_pool, pg,
                                       DiscretizerExt.discretize(op.duration),
                                       DiscretizerExt.discretize(timings.wait_for_pg),
                                       DiscretizerExt.discretize(timings.download),
                                       DiscretizerExt.discretize(timings.local_io))

        assert op.tp == OpType.read, "Unknown op type {}".format(op.tp)
        return cls.OPRecortR.pack(flags_and_pool, pg,
                                  DiscretizerExt.discretize(op.duration),
                                  DiscretizerExt.discretize(timings.wait_for_pg),
                                  DiscretizerExt.discretize(timings.download))

    @classmethod
    def unpack_op(cls, data: bytes, offset: int) -> Tuple[Dict[str, Any], int]:

        undiscretize_l = DiscretizerExt.table.__getitem__  # type: Callable[[int], float]
        flags_and_pool = data[offset]
        op_type = OpType(flags_and_pool >> 6)
        pool = flags_and_pool & 0x3F

        if op_type == OpType.write_primary:
            _, pg, duration, wait_for_pg, download, local_io, wait_for_replica = \
                cls.OPRecortWP.unpack(data[offset: offset + cls.OPRecortWP.size])

            return {'tp': op_type,
                    'pack_pool_id': pool,
                    'pg': pg,
                    'duration': undiscretize_l(duration),
                    'wait_for_pg': undiscretize_l(wait_for_pg),
                    'local_io': undiscretize_l(local_io),
                    'wait_for_replica': undiscretize_l(wait_for_replica),
                    'download': undiscretize_l(download),
                    'packer': cls.name}, offset + cls.OPRecortWP.size

        if op_type == OpType.write_secondary:
            _, pg, duration, wait_for_pg, download, local_io = \
                cls.OPRecortWS.unpack(data[offset: offset + cls.OPRecortWS.size])
            return {'tp': op_type,
                    'pack_pool_id': pool,
                    'pg': pg,
                    'duration': undiscretize_l(duration),
                    'wait_for_pg': undiscretize_l(wait_for_pg),
                    'local_io': undiscretize_l(local_io),
                    'download': undiscretize_l(download),
                    'packer': cls.name}, offset + cls.OPRecortWS.size

        assert op_type == OpType.read, "Unknown op type {}".format(op_type)
        _, pg, duration, wait_for_pg, local_io = cls.OPRecortR.unpack(data[offset: offset + cls.OPRecortR.size])
        return {'tp': op_type,
                'pack_pool_id': pool,
                'pg': pg,
                'duration': undiscretize_l(duration),
                'wait_for_pg': undiscretize_l(wait_for_pg),
                'local_io': undiscretize_l(local_io),
                'packer': cls.name}, offset + cls.OPRecortR.size

    @staticmethod
    def format_op(op: Dict[str, Any]) -> str:
        if op['tp'] == OpType.write_primary:
            assert 'wait_for_pg' in op
            assert 'download' in op
            assert 'local_io' in op
            assert 'wait_for_replica' in op
            return (("WRITE_PRIMARY     {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   dload={:>5d}" +
                     "   wait_pg={:>5d}   local_io={:>5d}   remote_io={:>5d}").
                    format(op['pool_name'], op['pg'], op['osd_id'], int(op['duration']), int(op['download']),
                           int(op['wait_for_pg']), int(op['local_io']), int(op['wait_for_replica'])))
        elif op['tp'] == OpType.write_secondary:
            assert 'wait_for_pg' in op
            assert 'download' in op
            assert 'local_io' in op
            assert 'wait_for_replica' not in op
            return (("WRITE_SECONDARY   {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   " +
                     "dload={:>5d}   wait_pg={:>5d}   local_io={:>5d}").
                    format(op['pool_name'], op['pg'], op['osd_id'], int(op['duration']),
                           int(op['download']), int(op['wait_for_pg']), int(op['local_io'])))
        elif op['tp'] == OpType.read:
            assert 'wait_for_pg' in op
            assert 'download' not in op
            assert 'local_io' in op
            assert 'wait_for_replica' not in op
            return (("READ              {:>25s}:{:<5x} osd_id={:>4d}" +
                     "   duration={:>5d}                 wait_pg={:>5d}   local_io={:>5d}").
                    format(op['pool_name'], op['pg'], op['osd_id'], int(op['duration']), int(op['wait_for_pg']),
                           int(op['local_io'])))
        else:
            assert False, "Unknown op {}".format(op['tp'])


class RawPacker(IPacker):
    """
    Compact packer - pack op to 6-8 bytes with timings for high-level stages - downlaod, wait pg, local io, remote io
    """

    name = 'raw'

    OPRecortWP = Struct("!BH" + 'B' * 10)
    OPRecortWS = Struct("!BH" + 'B' * 7)

    @classmethod
    def pack_op(cls, op: CephOp) -> bytes:
        assert op.pack_pool_id is not None
        assert 0 <= op.pack_pool_id <= MAX_POOL_VAL
        assert op.tp is not None

        # overflow pg
        if op.pg > MAX_PG_VAL:
            logger.debug("Too large pg = %d", op.pg)

        pg = min(MAX_PG_VAL, op.pg)

        assert op.tp in (OpType.write_primary, OpType.write_secondary, OpType.read), "Unknown op type {}".format(op.tp)

        queued_for_pg = -1
        reached_pg = -1
        sub_op_commit_rec = -1
        wait_for_subop = -1

        for evt, tm in op.events:
            if evt == 'queued_for_pg' and queued_for_pg == -1:
                queued_for_pg = tm
            elif evt == 'reached_pg':
                reached_pg = tm
            elif evt.startswith("sub_op_commit_rec from "):
                sub_op_commit_rec = tm
            elif evt.startswith("waiting for subops from "):
                wait_for_subop = tm

        assert reached_pg != -1
        assert queued_for_pg != -1

        if op.tp == OpType.write_primary:
            assert sub_op_commit_rec != -1
            assert wait_for_subop != -1

        flags_and_pool = (cast(int, op.tp.value) << 6) + op.pack_pool_id
        assert flags_and_pool < 256

        try:
            if op.tp == OpType.write_primary:
                # first queued_for_pg
                # last reached_pg
                # started
                # wait_for_subop
                # op_commit
                # op_applied
                # last sub_op_commit_rec
                # commit_sent
                # done
                return cls.OPRecortWP.pack(flags_and_pool, pg,
                                           DiscretizerExt.discretize(op.duration),
                                           DiscretizerExt.discretize(queued_for_pg),
                                           DiscretizerExt.discretize(reached_pg),
                                           DiscretizerExt.discretize(op.evt_map['started']),
                                           DiscretizerExt.discretize(wait_for_subop),
                                           DiscretizerExt.discretize(op.evt_map['op_commit']),
                                           DiscretizerExt.discretize(op.evt_map['op_applied']),
                                           DiscretizerExt.discretize(sub_op_commit_rec),
                                           DiscretizerExt.discretize(op.evt_map['commit_sent']),
                                           DiscretizerExt.discretize(op.evt_map['done']))

            if op.tp == OpType.write_secondary:
                # first queued_for_pg
                # last reached_pg
                # started
                # commit_send
                # sub_op_applied
                # done
                return cls.OPRecortWS.pack(flags_and_pool, pg,
                                           DiscretizerExt.discretize(op.duration),
                                           DiscretizerExt.discretize(queued_for_pg),
                                           DiscretizerExt.discretize(reached_pg),
                                           DiscretizerExt.discretize(op.evt_map['started']),
                                           DiscretizerExt.discretize(op.evt_map['commit_sent']),
                                           DiscretizerExt.discretize(op.evt_map['sub_op_applied']),
                                           DiscretizerExt.discretize(op.evt_map['done']))
        except KeyError:
            logger.error(pprint.pprint(op.evt_map))
            raise
        assert op.tp == OpType.read, "Unknown op type {}".format(op.tp)
        return b""

    @classmethod
    def unpack_op(cls, data: bytes, offset: int) -> Tuple[Dict[str, Any], int]:

        undiscretize_l = DiscretizerExt.table.__getitem__  # type: Callable[[int], float]
        flags_and_pool = data[offset]
        op_type = OpType(flags_and_pool >> 6)
        pool = flags_and_pool & 0x3F

        if op_type == OpType.write_primary:
            _, pg, duration, queued_for_pg, reached_pg, started, wait_for_subop, \
                op_commit, op_applied, sub_op_commit_rec, commit_sent, done = \
                cls.OPRecortWP.unpack(data[offset: offset + cls.OPRecortWP.size])

            return {'tp': op_type,
                    'pack_pool_id': pool,
                    'pg': pg,
                    'duration': undiscretize_l(duration),
                    'queued_for_pg': undiscretize_l(queued_for_pg),
                    'reached_pg': undiscretize_l(reached_pg),
                    'started': undiscretize_l(started),
                    'wait_for_subop': undiscretize_l(wait_for_subop),
                    'op_commit': undiscretize_l(op_commit),
                    'op_applied': undiscretize_l(op_applied),
                    'sub_op_commit_rec': undiscretize_l(sub_op_commit_rec),
                    'commit_sent': undiscretize_l(commit_sent),
                    'done': undiscretize_l(done),
                    'packer': cls.name}, offset + cls.OPRecortWP.size

        if op_type == OpType.write_secondary:
            _, pg, duration, queued_for_pg, reached_pg, started, commit_send, sub_op_applied, done = \
                cls.OPRecortWS.unpack(data[offset: offset + cls.OPRecortWS.size])
            return {'tp': op_type,
                    'pack_pool_id': pool,
                    'pg': pg,
                    'duration': undiscretize_l(duration),
                    'queued_for_pg': undiscretize_l(queued_for_pg),
                    'reached_pg': undiscretize_l(reached_pg),
                    'started': undiscretize_l(started),
                    'commit_send': undiscretize_l(commit_send),
                    'sub_op_applied': undiscretize_l(sub_op_applied),
                    'done': undiscretize_l(done),
                    'packer': cls.name}, offset + cls.OPRecortWS.size

        assert False, "Unknown op type {}".format(op_type)

    @staticmethod
    def format_op(op: Dict[str, Any]) -> str:
        if op['tp'] == OpType.write_primary:
            return (("WRITE_PRIMARY     {:>25s}:{:<5x} osd_id={:>4d}   q_for_pg={:>5d}  reached_pg={:>5d}" +
                     "   started={:>5d}   subop={:>5d}   commit={:>5d}   applied={:>5d} subop_ready={:>5d}" +
                     "  com_send={:>5d} done={:>5d}").
                    format(op['pool_name'], op['pg'], op['osd_id'], int(op['queued_for_pg']),
                           int(op['reached_pg']),
                           int(op['started']),
                           int(op['wait_for_subop']),
                           int(op['op_commit']),
                           int(op['op_applied']),
                           int(op['sub_op_commit_rec']),
                           int(op['commit_sent']),
                           int(op['done']),
                           ))
        elif op['tp'] == OpType.write_secondary:
            return (("WRITE_SECONDARY   {:>25s}:{:<5x} osd_id={:>4d}   q_for_pg={:>5d}  reached_pg={:>5d}" +
                     "   started={:>5d}                                applied={:>5d}" +
                     "                    com_send={:>5d} done={:>5d}").
                    format(op['pool_name'], op['pg'], op['osd_id'], int(op['queued_for_pg']),
                           int(op['reached_pg']),
                           int(op['started']),
                           int(op['commit_send']),
                           int(op['sub_op_applied']),
                           int(op['done']),
                           ))
        else:
            assert False, "Unknown op {}".format(op['tp'])


ALL_PACKERS = [CompactPacker, RawPacker]


def get_packer(name: str) -> Type[IPacker]:
    for packer_cls in ALL_PACKERS:
        if packer_cls.name == name:
            return packer_cls
    raise AssertionError("Unknown packer {}".format(name))


# --------- CEPH UTILS -------------------------------------------------------------------------------------------------


async def subprocess_output(cmd: str, shell: bool = False, timeout: float = 15) -> Awaitable[str]:
    if shell:
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE,
                                                     stderr=asyncio.subprocess.STDOUT)
    else:
        proc = await asyncio.create_subprocess_exec(*cmd.split(), stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.STDOUT)

    fut = proc.communicate()
    try:
        data, stderr = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error("Cmd: %r hang for %s seconds. Terminating it", cmd, timeout)
        proc.terminate()
        try:
            data, stderr = await asyncio.wait_for(fut, timeout=1)
        except asyncio.TimeoutError:
            logger.error("Cmd: %r hang for %s seconds. Killing it", cmd, timeout + 1)
            proc.kill()
            raise subprocess.CalledProcessError(-9, cmd)

    assert proc.returncode is not None
    assert stderr is None

    data_s = data.decode("utf8")
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=data_s)

    return data_s


def get_all_child_osds(node: Dict, crush_nodes: Dict[int, Dict], target_class: str = None) -> Iterator[int]:
    # workaround for incorrect node classes on some prod clusters
    if node['type'] == 'osd' or re.match("osd\.\d+", node['name']):
        if target_class is None or node.get('device_class') == target_class:
            yield node['id']
        return

    for ch_id in node['children']:
        yield from get_all_child_osds(crush_nodes[ch_id], crush_nodes)


async def get_local_osds(target_class: str = None, timeout: int = 15) -> Awaitable[Set[int]]:
    """
    Get OSD id's for current node from ceph osd tree for selected osd class (all classes by default)
    Search by hostname, as returned from socket.gethostname
    In case if method above failed - search by osd cluster/public ip address
    """

    # find by node name
    hostnames = {socket.gethostname(), socket.getfqdn()}

    try:
        osd_nodes_s = await subprocess_output("ceph node ls osd -f json", timeout=timeout)
    except subprocess.SubprocessError:
        osd_nodes_s = None

    all_osds_by_node_name = None
    if osd_nodes_s:
        osd_nodes = json.loads(osd_nodes_s)
        for name in hostnames:
            if name in osd_nodes:
                all_osds_by_node_name = osd_nodes[name]

    if all_osds_by_node_name is not None:
        return all_osds_by_node_name

    tree_js = await subprocess_output("ceph osd tree -f json", timeout=timeout)
    nodes = {node['id']: node for node in json.loads(tree_js)['nodes']}

    for node in nodes.values():
        if node['type'] == 'host' and node['name'] in hostnames:
            assert all_osds_by_node_name is None, \
                "Current node with names {} found two times in osd tree".format(hostnames)
            all_osds_by_node_name = set(get_all_child_osds(node, nodes, target_class))

    if all_osds_by_node_name is not None:
        return all_osds_by_node_name

    all_osds_by_node_ip = set()

    # find by node ips
    all_ips = (await subprocess_output("hostname -I", timeout=timeout)).split()
    osds_js = await subprocess_output("ceph osd dump -f json", timeout=timeout)
    for osd in json.loads(osds_js)['osds']:
        public_ip = osd['public_addr'].split(":", 1)[0]
        cluster_ip = osd['cluster_addr'].split(":", 1)[0]
        if public_ip in all_ips or cluster_ip in all_ips:
            if target_class is None or target_class == nodes[osd['id']].get('device_class'):
                all_osds_by_node_ip.add(osd['id'])

    return all_osds_by_node_ip


async def set_size_duration(osd_ids: Set[int],
                            size: int,
                            duration: int,
                            timeout: int = 15):
    """
    Set size and duration for historic_ops log
    """
    not_inited_osd = set()
    for osd_id in osd_ids:
        try:
            for set_part in ["osd_op_history_duration {}".format(duration), "osd_op_history_size {}".format(size)]:
                cmd = "ceph daemon osd.{} config set {}"
                out = await subprocess_output(cmd.format(osd_id, set_part), timeout=timeout)
                assert "success" in out
        except subprocess.SubprocessError:
            not_inited_osd.add(osd_id)
    return not_inited_osd


async def get_historic(osd_id: int, timeout: int = 15):
    """
    Get historic ops from osd
    """
    return await subprocess_output("ceph daemon osd.{} dump_historic_ops".format(osd_id), timeout=timeout)


# TODO: start them also for SIGUSR1

RADOS_DF = 'rados df -f json'
PG_DUMP = 'ceph pg dump -f json 2>/dev/null'
CEPH_DF = 'ceph df -f json'
CEPH_S = 'ceph -s -f json'


FileRec = Tuple[RecId, Any]
BinaryFileRec = Tuple[RecId, bytes]


async def dump_cluster_info(commands: List[str], timeout: float = 15) -> Awaitable[List[FileRec]]:
    """
    make a message with provided cmd outputs
    """
    output = {'time': int(time.time())}
    for cmd in commands:
        try:
            output[cmd] = json.loads(await subprocess_output(cmd, shell=True, timeout=timeout))
        except subprocess.SubprocessError:
            pass
    if output:
        return [(RecId.cluster_info, output)]

    return []


class CephDumper:
    def __init__(self, osd_ids: Set[int], size: int, duration: int, cmd_tout: int = 15, min_diration: int = 0,
                 dump_unparsed_headers: bool = False) -> None:
        self.osd_ids = osd_ids
        self.not_inited_osd = osd_ids.copy()
        self.pools_map = {}  # type: Dict[int, Tuple[str, int]]
        self.pools_map_no_name = {}   # type: Dict[int, int]
        self.size = size
        self.duration = duration
        self.cmd_tout = cmd_tout
        self.min_duration = min_diration
        self.last_time_ops = defaultdict(set)  # type: Dict[int, Set[str]]
        self.first_cycle = True
        self.dump_unparsed_headers = dump_unparsed_headers

    async def reload_pools(self) -> Awaitable[bool]:
        data = json.loads(await subprocess_output("ceph osd lspools -f json", timeout=self.cmd_tout))

        new_pools_map = {}
        for idx, pool in enumerate(sorted(data, key=lambda x: x['poolname'])):
            new_pools_map[pool['poolnum']] = (pool['poolname'], idx)

        if new_pools_map != self.pools_map:
            self.pools_map = new_pools_map
            self.pools_map_no_name = {num: idx for num, (_, idx) in new_pools_map.items()}
            return True
        return False

    async def dump_historic(self) -> Awaitable[List[FileRec]]:
        try:
            if self.not_inited_osd:
                self.not_inited_osd = await set_size_duration(self.not_inited_osd, self.size, self.duration,
                                                              timeout=self.cmd_tout)

            ctime = int(time.time())
            osd_ops = {}  # type: Dict[int, str]

            for osd_id in self.osd_ids:
                if osd_id not in self.not_inited_osd:
                    try:
                        osd_ops[osd_id] = await get_historic(osd_id)
                        # data = get_historic_fast(osd_id)
                    except (subprocess.CalledProcessError, OSError):
                        self.not_inited_osd.add(osd_id)
                        continue

            result = []  # type: List[FileRec]
            if await self.reload_pools():
                if self.first_cycle:
                    result.append((RecId.pools, self.pools_map))
                else:
                    # pools updated - skip this cycle, as different ops may came from pools before and after update
                    return []

            for osd_id, data in osd_ops.items():
                try:
                    parsed = json.loads(data)
                except Exception:
                    raise Exception(repr(data))

                if self.size != parsed['size'] or self.duration != parsed['duration']:
                    self.not_inited_osd.add(osd_id)
                    continue

                ops = []
                for op in parsed['ops']:
                    if self.min_duration and int(op.get('duration') * 1000) < self.min_duration:
                        continue
                    try:
                        parse_res, ceph_op = CephOp.parse_op(op)
                        if ceph_op:
                            ops.append(ceph_op)
                        elif parse_res == ParseResult.unknown:
                            logger.info("UNKNOWN: %s", op['description'])
                    except Exception:
                        parse_res = ParseResult.failed

                    if parse_res == ParseResult.failed:
                        logger.exception("Failed to parse op: {}".format(pprint.pformat(op)))

                ops = [op for op in ops if op.tp is not None and op.description not in self.last_time_ops[osd_id]]
                if self.min_duration:
                    ops = [op for op in ops if op.duration >= self.min_duration]
                self.last_time_ops[osd_id] = {op.description for op in ops}

                for op in ops:
                    assert op.pack_pool_id is None
                    op.pack_pool_id = self.pools_map_no_name[op.pool_id]

                result.append((RecId.ops, (osd_id, ctime, ops)))

            return result
        except Exception:
            logger.exception("In dump_historic")


BinInfoFunc = Callable[[], List[BinaryFileRec]]

responce = """HTTP/1.1 {code} {msg}
Content-Length: {lenght}
Content-Type: {content_type}
Encoding: utf8

"""


def make_responce(code: http.HTTPStatus, data: str = None, content_type: str = None) -> bytes:
    if content_type is None:
        content_type = 'text/json' if code == http.HTTPStatus.OK else 'text/txt'
    data_b = b"" if data is None else data.encode("utf8")
    return responce.format(code=code, msg=code.phrase, content_type=content_type,
                           lenght=len(data_b)).encode("utf8") + data_b


resp_not_found = make_responce(http.HTTPStatus.NOT_FOUND)
resp_bad_request = make_responce(http.HTTPStatus.BAD_REQUEST)


def dict2str_helper(dct: Dict[str, Any], prefix: str) -> List[str]:
    res = []
    for k, v in dct.items():
        assert isinstance(k, str)
        if isinstance(v, dict):
            res.extend(dict2str_helper(v, prefix + k + "::"))
        else:
            res.append("{}{} {}".format(prefix, k, v))
    return res


def dict2str(dct: Dict[str, Any]) -> str:
    return "\n".join(dict2str_helper(dct, ""))


class DumpLoop:
    sigusr_requested_handlers = ["cluster_info", "pg_dump"]

    def __init__(self, loop: asyncio.AbstractEventLoop, opts: Any, osd_ids: Set[int], fd: RecordFile) -> None:
        self.opts = opts
        self.osd_ids = osd_ids
        self.fd = fd
        self.loop = loop
        self.packer = get_packer(opts.packer)

        # name => (func, timout, next_call)
        self.handlers = {}  # type: Dict[str, Tuple[BinInfoFunc, float, float]]
        self.fill_handlers()

        self.running_handlers = set()  # type: Set[str]
        loop.add_signal_handler(signal.SIGUSR1, self.sigusr1_handler)
        self.server = None
        self.status = {
            'last_handler_run_at': {}
        }  # type: Dict[str, Any]

    async def close(self):
        if self.server:
            self.server.close()
            self.loop.run_until_complete(self.server.wait_closed())
            self.server = None

    async def handle_conn(self, reader: asyncio.StreamReader, writer):
        addr = writer.get_extra_info('peername')
        logger.debug("Get new conn from %s", addr)
        cmd = (await reader.readline()).decode('utf8').strip()
        logger.debug("Get cmd %s from %s", cmd, addr)
        if cmd == 'info':
            data = self.status.copy()
            data.update({"error": 0, "message": "Success"})
            writer.write(json.dumps(data).encode('utf8'))
            await writer.drain()
            logger.debug("Successfully send respond to %s", addr)
        else:
            logger.warning("Unknown cmd %s from %s", cmd, addr)
            data = json.dumps({"error": 1, "message": "Unknown cmd {!r}".format(cmd)})
            writer.write(data.encode('utf8'))
            await writer.drain()

        writer.close()

    async def handle_http(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logger.debug("Get new conn from %s", addr)

        try:
            req = []
            while True:
                line = (await reader.readline()).decode("utf8")
                if line.strip() == '':
                    break
                req.append(line)

            if not req:
                logger.warning("Get empty request from %s", addr)
                writer.write(resp_bad_request)
            else:
                try:
                    get, path, *extra = req[0].split()
                except ValueError:
                    logger.warning("Get incorrect request from %s: %s", addr, req[0])
                    writer.write(resp_bad_request)
                else:
                    logger.debug("Get cmd %s from %s", path, addr)
                    if path == '/status.json':
                        writer.write(make_responce(http.HTTPStatus.OK, json.dumps(self.status)))
                    elif path == '/status.txt':
                        writer.write(make_responce(http.HTTPStatus.OK, dict2str(self.status) + "\n"))
                    else:
                        writer.write(resp_not_found)
        except:
            writer.write(make_responce(code=http.HTTPStatus.INTERNAL_SERVER_ERROR, data=traceback.format_exc()))
            raise
        finally:
            await writer.drain()
            writer.close()

    async def start_info_server(self, addr: str):
        ip, port_s = addr.split(":")
        logger.info("Start server on addr %s", addr)
        port = int(port_s)
        self.server = await asyncio.start_server(self.handle_http, ip, port, loop=self.loop)

    async def start(self):
        for name in self.handlers:
            self.start_handler(name, repeat=True)

        if self.opts.http_server_addr:
            await self.start_info_server(self.opts.http_server_addr)

    def fill_handlers(self) -> None:
        ctime = time.time()
        if self.opts.record_cluster != 0:
            func = partial(dump_cluster_info, (RADOS_DF, CEPH_DF, CEPH_S), self.opts.timeout)
            self.handlers["cluster_info"] = func, self.opts.record_cluster, ctime

        if self.opts.record_pg_dump != 0:
            func = partial(dump_cluster_info, (PG_DUMP,), self.opts.timeout)
            self.handlers["pg_dump"] = func, self.opts.record_pg_dump, ctime

        dumper = CephDumper(self.osd_ids, self.opts.size, self.opts.duration,
                            self.opts.timeout, self.opts.min_duration, self.opts.dump_unparsed_headers)
        self.handlers["historic"] = dumper.dump_historic, self.opts.duration, ctime

    def start_handler(self, name: str, repeat: bool = False):
        self.loop.create_task(self.run_handler(name, repeat))

    async def run_handler(self, name: str, repeat: bool = False, one_short_wait: int = 60):
        run = True
        if name in self.running_handlers:
            if repeat:
                run = False
            else:
                for i in range(one_short_wait):
                    await asyncio.sleep(1.0)
                    if name not in self.running_handlers:
                        run = True
                        break

        if run:
            handler, _, _ = self.handlers[name]
            self.running_handlers.add(name)
            data = await handler()
            self.running_handlers.remove(name)
            self.status['last_handler_run_at'][name] = int(time.time())

            for rec_id, packed in self.packer.pack_iter(data):
                logger.debug("Handler %s provides %s bytes of data of type %s", name, len(packed), rec_id)
                self.fd.write_record(rec_id, packed)

        if repeat:
            handler, tout, next_time = self.handlers[name]
            next_time += tout
            curr_time = time.time()
            sleep_time = next_time - curr_time

            if sleep_time <= 0:
                delta = (int(-sleep_time / tout) + 1) * tout
                next_time += delta
                sleep_time += delta

            assert sleep_time > 0
            self.handlers[name] = handler, tout, next_time
            self.loop.call_later(sleep_time, self.start_handler, name, True)

    def sigusr1_handler(self) -> None:
        logger.info("Get SIGUSR1, will dump data")
        for name in self.sigusr_requested_handlers:
            if name in self.handlers:
                self.start_handler(name)


async def record_to_file(loop: asyncio.AbstractEventLoop, opts: Any) -> Awaitable[int]:
    logger.info("Start recording with opts = %s", " ".join(sys.argv))
    params = {'packer': opts.packer, 'cmd': sys.argv,
              'node': [socket.gethostname(), socket.getfqdn()],
              'time': time.time(),
              'date': str(datetime.datetime.now()),
              'tz_offset': time.localtime().tm_gmtoff}
    try:
        osd_ids = await get_local_osds()
        logger.info("osds = %s", osd_ids)

        with cast(BinaryIO, open_to_append(opts.output_file, True)) as os_fd:
            os_fd.seek(0, os.SEEK_SET)
            fd = RecordFile(os_fd, pack_each=opts.compress_each * 1024)
            header = fd.seek_to_last_valid_record()

            if header is None:
                os_fd.seek(0, os.SEEK_SET)
                os_fd.write(HEADER_LAST)
            else:
                assert header == HEADER_LAST, "Can only append to file with {} version".format(HEADER_LAST_NAME)

            fd.write_record(RecId.params, json.dumps(params).encode("utf8"))

            dl = DumpLoop(loop, opts, osd_ids, fd)
            await dl.start()
            try:
                while True:
                    await asyncio.sleep(0.1)
            finally:
                # await dl.close()
                raise

    except UTExit:
        raise
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as exc:
        logger.exception("During recording")
        if isinstance(exc, OSError):
            return exc.errno
        return 1
    return 0


def parse(os_fd: BinaryIO) -> Iterator[Tuple[RecId, Any]]:
    fd = RecordFile(os_fd)
    header = fd.read_file_header()
    if header is None:
        return

    packer = None  # type: Optional[Type[IPacker]]
    if header == HEADER_V11:
        packer = CompactPacker

    riter = fd.iter_records()
    pools_map = None  # type: Optional[Dict[int, Tuple[str, int]]]

    for rec_type, data in riter:
        if rec_type in (RecId.ops, RecId.cluster_info, RecId.pools):
            assert packer is not None, "No 'params' record found in file"
            res = packer.unpack(rec_type, data)
            if rec_type == RecId.ops:
                assert pools_map is not None, "No 'pools' record found in file"
                for op in res:
                    op['pool_name'], op['pool'] = pools_map[op['pack_pool_id']]
            elif rec_type == RecId.pools:
                # int(...) is a workaround for json issue - it only allows string to be keys
                pools_map = {pack_id: (name, int(real_id)) for real_id, (name, pack_id) in res.items()}
            yield rec_type, res
        elif rec_type == RecId.params:
            params = json.loads(data.decode("utf8"))
            packer = get_packer(params['packer'])
            yield rec_type, params
        else:
            raise AssertionError("Unknown rec type {} at offset {}".format(rec_type, fd.tell()))


def print_records_from_file(file: str, limit: Optional[int]) -> None:
    with open(file, "rb") as fd:
        idx = 0
        for tp, val in parse(fd):
            if tp == RecId.ops:
                for op in val:
                    idx += 1
                    if op['packer'] == 'compact':
                        print(CompactPacker.format_op(op))
                    if op['packer'] == 'raw':
                        print(RawPacker.format_op(op))
                    if limit is not None and idx == limit:
                        return


ALLOWED_LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR']


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=ALLOWED_LOG_LEVELS, help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    set_parser = subparsers.add_parser('set', help="config osd's historic ops")
    set_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    set_parser.add_argument("--size", required=True, type=int, help="Num request to keep")

    subparsers.add_parser('set_default', help="config osd's historic ops to default 20/600")

    record_parser = subparsers.add_parser('record', help="Dump osd's requests periodically")
    record_parser.add_argument("--http-server-addr", default=None, help="Addr for status http server")
    record_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    record_parser.add_argument("--size", required=True, type=int, help="Num request to keep")
    record_parser.add_argument("--timeout", type=int, default=30, help="Timeout to run cli cmds")
    assert CompactPacker in ALL_PACKERS
    record_parser.add_argument("--packer", default=CompactPacker.name,
                               choices=[packer.name for packer in ALL_PACKERS], help="Select command packer")
    record_parser.add_argument("--min-duration", type=int, default=30,
                               help="Minimal duration in ms for op to be recorded")
    record_parser.add_argument("--record-cluster", type=int, help="Record cluster info every SECONDS seconds",
                               metavar='SECONDS', default=0)
    record_parser.add_argument("--record-pg-dump", type=int, help="Record cluster pg dump info every SECONDS seconds",
                               metavar='SECONDS', default=0)
    record_parser.add_argument("--compress-each", type=int, help="Compress each KB kilobytes of record file",
                               metavar='KiB', default=1024)
    record_parser.add_argument("--dump-unparsed-headers", action='store_true')
    record_parser.add_argument("output_file", help="Filename to append requests logs to it")

    parse_parser = subparsers.add_parser('parse', help="Parse records from file")
    parse_parser.add_argument("-l", "--limit", default=None, type=int, metavar="COUNT", help="Parse only COUNT records")
    parse_parser.add_argument("file", help="Log file")

    return parser.parse_args(argv[1:])


def setup_logger(configurable_logger: logging.Logger, log_level: str, log: str = None) -> None:
    assert log_level in ALLOWED_LOG_LEVELS
    configurable_logger.setLevel(getattr(logging, log_level))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, log_level))
    ch.setFormatter(formatter)

    if log:
        fh = logging.FileHandler(log)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        configurable_logger.addHandler(fh)

    configurable_logger.addHandler(ch)


async def async_main(loop: asyncio.AbstractEventLoop, opts: Any) -> Awaitable[int]:
    if opts.subparser_name in ('set', 'set_default'):
        if opts.subparser_name == 'set':
            duration = opts.duration
            size = opts.size
        else:
            duration = DEFAULT_DURATION
            size = DEFAULT_SIZE
        osd_ids = await get_local_osds()
        failed_osds = await set_size_duration(osd_ids, duration=duration, size=size)
        if failed_osds:
            logger.error("Fail to set time/duration for next osds: %s", " ,".join(map(str, osd_ids)))
            return 1
        return 0
    else:
        assert opts.subparser_name == 'record'
        return await record_to_file(loop, opts)


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    setup_logger(logger, opts.log_level, opts.log)

    if opts.subparser_name == 'parse':
        print_records_from_file(opts.file, opts.limit)
        return 0

    loop = asyncio.get_event_loop()
    try:
        return loop.run_until_complete(async_main(loop, opts))
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    exit(main(sys.argv))
