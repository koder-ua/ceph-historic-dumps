#!/usr/bin/env python2
import sys
import json
import time
import socket
import os.path
import datetime
import subprocess


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


def to_unix_mks(dtm):
    # "2019-02-03 20:53:47.429996"
    date, tm = dtm.split()
    y, m, d = date.split("-")
    h, min, smks = tm.split(':')
    s, mks = smks.split(".")
    d = datetime.datetime(int(y), int(m), int(d), int(h), int(min), int(s))
    return int(time.mktime(d.timetuple()) * 1000000) + int(mks)


def main(argv):
    osd_ids = get_ids()

    if argv[1] == 'set':
        set_size_duration(osd_ids, int(argv[2]), int(argv[3]))
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

    mode = "r+" if os.path.exists(output_fd) else "w"
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

                    for op in parsed['ops']:
                        description = op['description']
                        initiated_at = op['initiated_at']
                        initiated_at_mks = to_unix_mks(initiated_at)
                        op_duration = op['duration']
                        evts = ["{} {}".format(evt["event"], to_unix_mks(evt["time"]) - initiated_at_mks)
                                for evt in op["type_data"]["events"] if evt["event"] != "initiated"]
                        data = "{}|{}|{}|{}|{}\n".format(osd_id, initiated_at,
                                                         op_duration, "|".join(evts), description)
                        fd.write(data)
            fd.flush()
            stime = start_time + duration - time.time()
            if stime > 0:
                time.sleep(stime)


if __name__ == "__main__":
    exit(main(sys.argv))
