#!/usr/bin/env python2
import errno
import threading

import math
import sys
import json
import time
import struct
import socket
import os.path
import argparse
import datetime
import subprocess
from struct import Struct


MKS_TO_MS = 1000
S_TO_MS = 1000

MAX_SRC_VL = 100000
MIN_SRC_VL = 1
MAX_TARGET_VL = 254
MIN_TARGET_VL = 1
TARGET_UNDERVAL = 0
TARGET_OVERVAL = 255
UNDISCRETIZE_UNDERVAL = 0
UNDISCRETIZE_OVERVAL = MAX_SRC_VL

assert MIN_SRC_VL >= 1
assert MAX_SRC_VL > MIN_SRC_VL
assert TARGET_OVERVAL > MAX_TARGET_VL > MIN_TARGET_VL > TARGET_UNDERVAL
assert UNDISCRETIZE_UNDERVAL <= MIN_SRC_VL < MAX_SRC_VL <= UNDISCRETIZE_OVERVAL

SCALE_COEF = (MAX_TARGET_VL - MIN_TARGET_VL) / (math.log10(MAX_SRC_VL) - math.log10(MIN_SRC_VL))

OP_READ = 0
OP_WRITE_PRIMARY = 1
OP_WRITE_SECONDARY = 2


OPS_RECORD_ID = 1
POOLS_RECORD_ID = 2
HEADER = b"OSD OPS LOG v1\0"


class NoPoolFound(Exception):
    pass


def discretize(vl):
    if vl > MAX_SRC_VL:
        return TARGET_OVERVAL
    if vl < MIN_SRC_VL:
        return TARGET_UNDERVAL
    assert vl >= 0
    res = round(math.log10(vl) * SCALE_COEF) + MIN_TARGET_VL
    assert MAX_TARGET_VL >= res >= MIN_TARGET_VL
    return res


def undiscretize(vl):
    if vl == TARGET_UNDERVAL:
        return UNDISCRETIZE_UNDERVAL
    if vl == TARGET_OVERVAL:
        return UNDISCRETIZE_OVERVAL
    return 10 ** ((vl - MIN_TARGET_VL) / SCALE_COEF)


undiscretize_map = {vl: undiscretize(vl) for vl in range(256)}


def to_unix_ms(dtm):
    # "2019-02-03 20:53:47.429996"
    date, tm = dtm.split()
    y, m, d = date.split("-")
    h, min, smks = tm.split(':')
    s, mks = smks.split(".")
    d = datetime.datetime(int(y), int(m), int(d), int(h), int(min), int(s))
    return int(time.mktime(d.timetuple()) * S_TO_MS) + int(mks) // MKS_TO_MS


def pipe_thread(cmd_q, path="/var/run/ceph-ho-dumper"):
    try:
        os.mkfifo(path)
    except OSError as oe:
        if oe.errno != errno.EEXIST:
            raise

    with open(path) as fd:
        while True:
            cmd_q.put(fd.readline())


def get_timings(op):
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


def get_ids(expected_class=None):
    hostname = socket.gethostname()
    osds = set()
    in_host = False

    for line in subprocess.check_output("ceph osd tree".split()).split("\n"):
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


def set_size_duration(osd_ids, size, duration):
    not_inited_osd = set()
    for osd_id in osd_ids:
        try:
            cmd = "ceph daemon osd.{} config set osd_op_history_duration {}"
            out = subprocess.check_output(cmd.format(osd_id, duration).split(), stderr=subprocess.STDOUT)
            assert "success" in out
            cmd = "ceph daemon osd.{} config set osd_op_history_size {}"
            out = subprocess.check_output(cmd.format(osd_id, size).split(), stderr=subprocess.STDOUT)
            assert "success" in out
        except subprocess.CalledProcessError:
            not_inited_osd.add(osd_id)
    return not_inited_osd


def get_type(op):
    description = op['description']
    op_type_s, _ = description.split("(", 1)

    is_read = is_write = False

    if '+write+' in description:
        is_write = True
    elif '+read+' in description:
        is_read = True

    if op_type_s == 'osd_op':
        if is_write:
            return OP_WRITE_PRIMARY
        if is_read:
            return OP_READ
    elif op_type_s == 'osd_repop':
        assert not (is_read or is_write)
        return OP_WRITE_SECONDARY
    return None


def get_pool_pg(op):
    pool_s, pg_s = op['description'].split()[1].split(".")
    return int(pool_s), int(pg_s, 16)


OPRecortWP = Struct("!BHBBBBB")
OPRecortWS = Struct("!BHBBBB")
OPRecortR = Struct("!BHBBB")


def pack_to_str(op, pools_map):
    op_type = get_type(op)
    if op_type is None:
        return ""

    pool, pg = get_pool_pg(op)

    try:
        pool_log_id = pools_map[pool]
    except KeyError:
        raise NoPoolFound()

    assert 0 <= pool_log_id < 64
    assert pg < 2 ** 16

    _, _, qpg_at, started, local_done, replicas_waiting, subop = get_timings(op)
    flags_and_pool = (op_type << 6) + pool_log_id
    assert flags_and_pool < 256
    wait_pg = started - qpg_at
    assert wait_pg >= 0
    local_io = local_done - started
    assert local_io >= 0

    duration = op['duration'] * S_TO_MS

    if op_type in (OP_WRITE_PRIMARY, OP_WRITE_SECONDARY):
        dload = qpg_at
        if op_type == OP_WRITE_PRIMARY:
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
        assert op_type == OP_READ
        return OPRecortR.pack(flags_and_pool, pg,
                              discretize(duration),
                              discretize(wait_pg),
                              discretize(local_io))


def unpack_from_str(data, offset, pool_map):
    undiscretize_l = undiscretize_map.__getitem__
    flags_and_pool = ord(data[offset])
    op_type = flags_and_pool >> 6
    pool = pool_map[flags_and_pool & 0x3F]

    if op_type == OP_WRITE_PRIMARY:
        _, pg, dura, wait_pg, dload, local_io, remote_io = OPRecortWP.unpack(data[offset: offset + OPRecortWP.size])
        return op_type, pool, pg, undiscretize_l(dura), undiscretize_l(wait_pg), \
               undiscretize_l(dload), undiscretize_l(local_io), undiscretize_l(remote_io), \
               offset + OPRecortWP.size

    if op_type == OP_WRITE_SECONDARY:
        _, pg, dura, wait_pg, dload, local_io = OPRecortWS.unpack(data[offset: offset + OPRecortWS.size])
        return op_type, pool, pg, undiscretize_l(dura), undiscretize_l(wait_pg), \
               undiscretize_l(dload), undiscretize_l(local_io), None, offset + OPRecortWS.size

    assert op_type == OP_READ

    _, pg, dura, wait_pg, local_io = OPRecortR.unpack(data[offset: offset + OPRecortR.size])
    return op_type, pool, pg, undiscretize_l(dura), undiscretize_l(wait_pg), \
           None, undiscretize_l(local_io), None, \
           offset + OPRecortR.size


def pack_pools_record(pool_map):
    pools_map_js = json.dumps(pool_map)
    return struct.pack("!BH", POOLS_RECORD_ID, len(pools_map_js)) + pools_map_js


class OPPacker(object):
    def __init__(self):
        self.last_time_ops = set()
        self.pools_map = {}
        self.pools_map_no_name = {}

    def reload_pools(self):
        self.pools_map = {}
        self.pools_map_no_name = {}
        data = json.loads(subprocess.check_output("ceph osd lspools -f json".split()))
        for idx, pool in enumerate(sorted(data, key=lambda x: x['poolname'])):
            pool['idx'] = idx
            self.pools_map[pool['poolnum']] = (pool['poolname'], idx)
            self.pools_map_no_name[pool['poolnum']] = idx

    def pack_ops(self, ops, osd_id):
        pools_rec = b""
        try:
            packed = [pack_to_str(op, self.pools_map_no_name) for op in ops]
        except NoPoolFound:
            self.reload_pools()
            pools_rec = pack_pools_record(self.pools_map)
            packed = [pack_to_str(op, self.pools_map_no_name) for op in ops]

        packed_s = b"".join(packed)
        if packed_s:
            return pools_rec + struct.pack("!BHHI", OPS_RECORD_ID, osd_id, len(packed_s), int(time.time())) + packed_s
        else:
            return pools_rec


def get_historic(osd_id):
    return subprocess.check_output("ceph daemon osd.{} dump_historic_ops".format(osd_id).split())


def get_historic_fast(osd_id):
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect("/var/run/ceph/ceph-osd.{}.asok".format(osd_id))
        sock.send(b'{"prefix": "dump_historic_ops"}\0')

        response = ""
        data = "\0"
        while data:
            data = sock.recv(2 ** 12)
            response += data
        return "{" + response.split("{", 1)[1]
    except (IndexError, socket.error):
        raise subprocess.SubprocessError("failed to get ceph historic ops via socket")


def open_to_append(fname, is_bin=False):
    if os.path.exists(fname):
        fd = open(fname, "rb+" if is_bin else "r+")
        fd.seek(0, os.SEEK_END)
    else:
        fd = open(fname, "wb" if is_bin else "w")
    return fd


def dump_loop(opts, osd_ids, fd):
    packer = OPPacker()
    not_inited_osd = osd_ids.copy()
    prev_ids = set()

    # spawn pipe listen thread
    import Queue
    cmd_q = Queue.Queue()
    th = threading.Thread(target=pipe_thread, args=(cmd_q,))
    th.daemon = True
    th.start()

    while True:
        start_time = time.time()
        if not_inited_osd:
            not_inited_osd = set_size_duration(not_inited_osd, opts.size, opts.duration)

        for osd_id in osd_ids:
            if osd_id not in not_inited_osd:
                try:
                    data = get_historic(osd_id)
                    # data = get_historic_fast(osd_id)
                except (subprocess.CalledProcessError, OSError):
                    not_inited_osd.add(osd_id)
                    continue

                try:
                    parsed = json.loads(data)
                except:
                    raise Exception(repr(data))

                if opts.size != parsed['size'] or opts.duration != parsed['duration']:
                    not_inited_osd.add(osd_id)
                    continue

                ops = [op for op in parsed['ops'] if op['description'] not in prev_ids]
                prev_ids = [op['description'] for op in ops]
                fd.write(packer.pack_ops(ops, osd_id))

        fd.flush()

        while True:
            stime = start_time + opts.duration - time.time()
            if stime < 0:
                stime = 0

            try:
                cmd = cmd_q.get(True, stime)
            except Queue.Empty:
                break



class UnexpectedEndOfFile(Exception): pass


def get_bytes(fd, size):
    dt = fd.read(size)
    if len(dt) != size:
        raise UnexpectedEndOfFile()
    return dt


def parse(fd):
    pools_map = {}
    assert fd.read(len(HEADER)) in (HEADER, b""), "Output file corrupted"
    while True:
        try:
            rec_type = ord(get_bytes(fd, 1))
        except UnexpectedEndOfFile:
            return

        if rec_type == POOLS_RECORD_ID:
            sz, = struct.unpack("!H", get_bytes(fd, 2))
            pools_map = {pool_id: (name, orig_id_s)
                         for orig_id_s, (name, pool_id) in json.loads(get_bytes(fd, sz)).items()}
        else:
            assert rec_type == OPS_RECORD_ID, "File corrupted near offset {}".format(fd.tell())
            osd_id, recsize, _ = struct.unpack("!HHI", get_bytes(fd, 8))
            data = get_bytes(fd, recsize)
            offset = 0
            while offset < len(data):
                op_offset = unpack_from_str(data, offset, pools_map)
                offset = op_offset[-1]
                yield (osd_id,) + op_offset[:-1]


def format_op(osd_id, op_type, pool_name, pg, dura, wait_pg, dload, local_io, remote_io):
    if op_type == OP_WRITE_PRIMARY:
        assert wait_pg is not None
        assert dload is not None
        assert local_io is not None
        assert remote_io is not None
        return (("WRITE_PRIMARY     {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   wait_pg={:>5d}   local_io={:>5d}" +
                 "   dload={:>5d}   remote_io={:>5d}").
                 format(pool_name, pg, osd_id, int(dura), int(wait_pg), int(dload), int(local_io), int(remote_io)))
    elif op_type == OP_WRITE_SECONDARY:
        assert wait_pg is not None
        assert dload is not None
        assert local_io is not None
        assert remote_io is None
        return ("WRITE_SECONDARY   {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   wait_pg={:>5d}   local_io={:>5d}   dload={:>5d}".
                format(pool_name, pg, osd_id, int(dura), int(wait_pg), int(dload), int(local_io)))
    elif op_type == OP_READ:
        assert wait_pg is not None
        assert dload is None
        assert local_io is not None
        assert remote_io is None
        return ("READ              {:>25s}:{:<5x} osd_id={:>4d}   duration={:>5d}   wait_pg={:>5d}   local_io={:>5d}".
                format(pool_name, pg, osd_id, int(dura), int(wait_pg), int(local_io)))
    else:
        assert False, "Unknown op"


def parse_args(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subparser_name')

    set_parser = subparsers.add_parser('set', help="config osd's historic ops")
    set_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    set_parser.add_argument("--size", required=True, type=int, help="Num request to keep")

    set_parser = subparsers.add_parser('record', help="Dump osd's requests periodically")
    set_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    set_parser.add_argument("--size", required=True, type=int, help="Num request to keep")
    set_parser.add_argument("--log", required=True, help="File to log messages to")
    set_parser.add_argument("output_file", help="Filename to append requests logs to it")

    set_parser = subparsers.add_parser('parse', help="Parse records from file")
    set_parser.add_argument("-l", "--limit", default=None, type=int, metavar="COUNT", help="Parse only COUNT records")
    set_parser.add_argument("file", help="Log file")

    return parser.parse_args(argv[1:])


def main(argv):
    opts = parse_args(argv)
    osd_ids = get_ids()

    if opts.subparser_name == 'set':
        set_size_duration(osd_ids, duration=opts.duration, size=opts.size)
        return 0

    if opts.subparser_name == 'parse':
        with open(opts.file, "rb") as fd:
            for idx, (osd_id, op_type, (pool_name, _), pg, dura, wait_pg, dload, local_io, remote_io) in enumerate(parse(fd)):
                print(format_op(osd_id, op_type, pool_name, pg, dura, wait_pg, dload, local_io, remote_io))
                if opts.limit is not None and idx == opts.limit:
                    break

        return 0

    assert opts.subparser_name == 'record'

    log_file_fd = open_to_append(opts.log)
    log_file_fd.write("Find next osds = {}\n".format(osd_ids))
    log_file_fd.flush()

    try:
        fd = open_to_append(opts.output_file, True)

        with fd:

            if fd.tell() > 0:
                assert fd.tell() >= len(HEADER), "Output file corrupted"
                fd.seek(0)
                assert fd.read(len(HEADER)) == HEADER, "Output file corrupted"
                fd.seek(0, os.SEEK_END)
            else:
                fd.write(HEADER)
            fd.flush()

            dump_loop(opts, osd_ids, fd)
    except:
        import traceback
        log_file_fd.write(traceback.format_exc() + "\n")
        log_file_fd.flush()
        raise

if __name__ == "__main__":
    exit(main(sys.argv))
