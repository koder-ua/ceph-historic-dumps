#!/usr/bin/env python2
import sys
import json
import time
import socket
import os.path
import datetime
import subprocess
from struct import Struct

OSD_OP_I = 0
OSD_REPOP_I = 1

IO_WRITE_I = 0
IO_READ_I = 1
IO_UNKNOWN_I = 2

MKS_TO_MS = 1000
S_TO_MS = 1000


OPRecort = Struct("!HBBLQlllllBH")


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

            osds.add(osd_id)

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


def to_unix_ms(dtm):
    # "2019-02-03 20:53:47.429996"
    date, tm = dtm.split()
    y, m, d = date.split("-")
    h, min, smks = tm.split(':')
    s, mks = smks.split(".")
    d = datetime.datetime(int(y), int(m), int(d), int(h), int(min), int(s))
    return int(time.mktime(d.timetuple()) * S_TO_MS) + int(mks) // MKS_TO_MS


def pack_to_str(op, osd_id):
    description = op['description']
    op_type_s, _ = description.split("(", 1)

    if op_type_s == 'osd_op':
        op_type = OSD_OP_I
    elif op_type_s == 'osd_repop':
        op_type = OSD_REPOP_I
    else:
        return ""

    if '+write+' in description:
        io_type = IO_WRITE_I
    elif '+read+' in description:
        io_type = IO_READ_I
    else:
        io_type = IO_UNKNOWN_I

    initiated_at = to_unix_ms(op['initiated_at'])
    stages = {evt["event"]: to_unix_ms(evt["time"]) - initiated_at
              for evt in op["type_data"]["events"] if evt["event"] != "initiated"}

    try:
        qpg_at = stages["queued_for_pg"]
    except KeyError:
        qpg_at = -1

    try:
        started = stages["started"]
    except KeyError:
        started = -1

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
            subop = tm

    pool_s, pg_s = description.split()[1].split(".")
    pool = int(pool_s)
    pg = int(pg_s, 16)

    op_duration = int(op['duration']) // MKS_TO_MS

    print([repr(i) for i in (osd_id, op_type, io_type, op_duration, initiated_at,
                       qpg_at, started, local_done, replicas_waiting, subop, pool, pg)])
    return OPRecort.pack(osd_id, op_type, io_type, op_duration, initiated_at,
                         qpg_at, started, local_done, replicas_waiting, subop, pool, pg)


def main(argv):
    osd_ids = get_ids()

    if argv[1] == 'set':
        set_size_duration(osd_ids, duration=int(argv[2]), size=int(argv[3]))
        return 1

    assert argv[1] == 'dump'

    duration = int(argv[3])
    size = int(argv[4])
    not_inited_osd = osd_ids
    output_fd = argv[2]

    if os.path.exists(argv[5]):
        log_file_fd = open(argv[5], "r+")
        log_file_fd.seek(0, os.SEEK_END)
    else:
        log_file_fd = open(argv[5], "w")

    log_file_fd.write("Find next osds = {}\n".format(osd_ids))
    log_file_fd.flush()

    mode = "rb+" if os.path.exists(output_fd) else "wb"
    with open(output_fd, mode) as fd:
        while True:
            start_time = time.time()
            if not_inited_osd:
                not_inited_osd = set_size_duration(not_inited_osd, size, duration)

            for osd_id in osd_ids:
                if osd_id not in not_inited_osd:
                    try:
                        cmd = "ceph daemon osd.{} dump_historic_ops".format(osd_id).split()
                        data = subprocess.check_output(cmd)
                    except ValueError:
                        not_inited_osd.add(osd_id)
                        continue

                    parsed = json.loads(data)
                    if size != parsed['size'] or duration != parsed['duration']:
                        not_inited_osd.add(osd_id)
                        continue

                    fd.write("".join(pack_to_str(op, int(osd_id)) for op in parsed['ops']))

            fd.flush()

            stime = start_time + duration - time.time()
            if stime > 0:
                time.sleep(stime)


if __name__ == "__main__":
    exit(main(sys.argv))
