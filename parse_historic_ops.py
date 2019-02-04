import bisect
import math
import sys
import json
from collections import Counter, defaultdict
from typing import Dict, Iterator, Tuple, Iterable, List, Set

import numpy
import pandas
import matplotlib
from matplotlib import pyplot
from dataclasses import dataclass

from ceph_ho_dumper import OSD_OP_I, OSD_REPOP_I, IO_WRITE_I, IO_UNKNOWN_I, IO_READ_I


matplotlib.rcParams.update({'font.size': 22})


@dataclass
class OP:
    description: str
    pg: str
    op_type: str
    io_type: str
    duration: int
    stages: Dict[str, int]

    def dload_time(self) -> int:
        return self.stages["queued_for_pg"] - self.stages["initiated"]

    def waiting_for_pg(self) -> int:
        return self.stages["started"] - self.stages["queued_for_pg"]

    def waiting_for_subop(self) -> int:
        st = None
        repls = []
        for op, tm in self.stages.items():
            if op.startswith("waiting "):
                st = tm
            elif op.startswith("sub_op_commit_rec "):
                repls.append(tm)
        return max(repls) - st

    def local_time(self) -> int:
        if 'op_applied' in self.stages:
            return self.stages["op_applied"] - self.stages["started"]
        else:
            return self.stages["done"] - self.stages["started"]

    def to_tuple(self) -> Tuple[str, str, str, str, int, Dict[str, int]]:
        return ("", self.pg, self.op_type, self.io_type, self.duration, self.stages)


def time_to_ms(dt: str) -> int:
    _, tm = dt.split()
    hms, mks = tm.split(".")
    h, m, s = hms.split(":")
    return int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(mks) // 1000


def parse_op(op: Dict) -> OP:
    descr = op['description']
    op_type, _ = descr.split("(", 1)
    pg = descr.split()[1]
    if '+write+' in descr:
        io_type = IO_WRITE
    elif '+read+' in descr:
        io_type = IO_READ
    else:
        io_type = IO_UNKNOWN

    duration = int(float(op['duration']) * 1000)
    stages = {evt['event']: time_to_ms(evt['time']) for evt in op['type_data']['events']}
    return OP(descr, pg, op_type, io_type, duration, stages)


def parse_file(fname: str) -> Iterator[OP]:
    curr_data = ""
    for line in open(fname):
        if line == '}\n':
            curr_data += line
            yield from map(parse_op, json.loads(curr_data)["ops"])
            curr_data = ""
        else:
            curr_data += line


def show_histo(df: pandas.DataFrame):
    for name, selector in iter_classes_selector(df):
        writes = (df['io_type'] == IO_WRITE_I) | (df['io_type'] == IO_UNKNOWN_I)
        osd_ops = df['op_type'] == OSD_OP_I
        bins = numpy.array([100 * i for i in range(10)] + [1000 * i for i in range(1, 10)] + [10000, 20000, 30000, 100000])
        times = df['durations'][osd_ops & writes & selector] // 1000
        res, _ = numpy.histogram(times, bins)
        res[-2] += res[-1]
        print(f"\n-----------------------\nfor class {name}")
        for start, stop, res in zip(bins[1:-2], bins[2:-1], res[1:-1]):
            if res != 0:
                print(f"{start:>5d}ms ~ {stop:>5d}ms  {res:>8d}")


def top_slow_pgs(ops: Iterable[OP]):
    res = Counter()
    for op in ops:
        res[op.pg] += 1

    for pg, cnt in sorted(res.items(), key=lambda x: -x[1])[:20]:
        print(f"{pg:>8s}  {cnt:>5d}")


def top_slow_osds(ops: Iterable[OP], pg_map: Dict[str, List[int]], is_read: bool = False):
    res = Counter()
    for op in ops:
        if is_read:
            res[pg_map[op.pg][0]] += 1
        else:
            for osd in pg_map[op.pg]:
                res[osd] += 1

    for osd, cnt in sorted(res.items(), key=lambda x: -x[1])[:20]:
        print(f"{osd:>8d}  {cnt:>5d}")


def show_per_pg_stat(ops: Iterable[OP]):
    per_pg = Counter()
    total_per_pool = Counter()
    all_pg = set()

    for op in ops:
        if op.io_type == IO_WRITE:
            per_pg[op.pg] += 1
            pool, _ = op.pg.split(".")
            total_per_pool[pool] += 1
            all_pg.add(op.pg)

    total_117_pg = len([pg for pg in all_pg if pg.startswith("117.")])

    print(f"total pg 117 = {total_117_pg}")
    tt = 0
    for pg, count in sorted(((pg, cnt) for pg, cnt in per_pg.items() if pg.startswith("117.")), key=lambda x: -x[1])[:20]:
        pool, _ = pg.split(".")
        print(f"{pg:>8s}  {count:>8d}   {round(count * 1000 / total_per_pool[pool]) / 10:1.1f}%")
        tt += count
    print(f"Total for first 20 = {tt / total_per_pool['117'] * 100:.1f}%")


def convert(target: str, files: List[str]):
    all_ops = []
    for fname in sorted(files):
        print(fname)
        for op in parse_file(fname):
            all_ops.append(op.to_tuple())

    print("Serializing")
    with open(target, 'w') as fd:
        json.dump(all_ops, fd)


def filter_duration(ops: Iterable[OP], min_duration: int = 0, max_duration: int = 1e8) -> Iterable[OP]:
    return (op for op in ops if max_duration >= op.duration >= min_duration)


def filter_iotype(ops: Iterable[OP], io_types: Set[str]) -> Iterable[OP]:
    return (op for op in ops if op.io_type in io_types)


def filter_optype(ops: Iterable[OP], op_types: Set[str]) -> Iterable[OP]:
    return (op for op in ops if op.op_type in op_types)


def per_PG_OSD_stat(all_ops: List[OP], pg_map: Dict):
    longer_100ms = list(filter_duration(all_ops, 100))
    # longer_1s = list(filter_duration(all_ops, 1000, 10000))
    # longer_10s = list(filter_duration(all_ops, 10000, 100000))

    print("Reads most slow OSDS")
    top_slow_osds(filter_iotype(longer_100ms, {IO_READ}), pg_map, True)

    print("Writes most slow OSDS")
    print("------------- 100ms -----------------")
    top_slow_osds(filter_iotype(longer_100ms, {IO_WRITE}), pg_map)

    print("Reads most slow PGS")
    print("------------- 100ms -----------------")
    top_slow_pgs(filter_iotype(longer_100ms, {IO_READ}))

    print("Writes most slow PGS")
    print("------------- 100ms -----------------")
    top_slow_pgs(filter_iotype(longer_100ms, {IO_WRITE}))


def stages_stat(ops: List[OP]):
    longer_100ms = list(filter_duration(ops, 0, 450))
    longer_300ms = list(filter_duration(ops, 450, 800))
    longer_1s = list(filter_duration(ops, 800))

    print("OP")

    for lst in (longer_100ms, longer_300ms, longer_1s):
        stage_times = defaultdict(int)
        for op in filter_optype(filter_iotype(lst, {IO_WRITE}), {OSD_OP}):
            try:
                stage_times["dload"] += op.dload_time()
                stage_times["waiting_for_pg"] += op.waiting_for_pg()
                stage_times["waiting_for_subop"] += op.waiting_for_subop()
                stage_times["local"] += op.local_time()
            except:
                # print(op)
                pass

        for stage in ("dload", "waiting_for_pg", "waiting_for_subop", "local"):
            print(f"{stage:>20s}  {stage_times[stage] // 1000:>6d}")
        print()

    print("REPOP")

    for lst in (longer_100ms, longer_300ms, longer_1s):
        stage_times = defaultdict(int)
        for op in filter_optype(lst, {OSD_REPOP}):
            try:
                stage_times["dload"] += op.dload_time()
                stage_times["waiting_for_pg"] += op.waiting_for_pg()
                stage_times["local"] += op.local_time()
            except:
                print(op)
                pass

        for stage in ("dload", "waiting_for_pg", "local"):
            print(f"{stage:>20s}  {stage_times[stage] // 1000:>6d}")
        print()


def plot_op_time_distribution(df: pandas.DataFrame, min_time: int = 100, max_time: int = 30000, bins: int = 40):
    filter = (df['io_type'] == IO_WRITE_I) & ((df['pools'] == 115) | (df['pools'] == 117))
    times = df['durations'][filter] / 1000
    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    bins = [0] + list(bins) + [max(times)]
    vals, _ = numpy.histogram(times, bins)
    vals[-2] += vals[-1]
    vals = vals[1:-1]
    bins = numpy.array(bins[1:-1])
    bins_centers = (bins[1:] + bins[:-1]) / 2
    vals = numpy.clip(vals, 0, 2000)
    pyplot.plot(bins_centers, vals, linestyle='--', marker='o', color='b')
    pyplot.xlabel('Request latency, ms')
    pyplot.ylabel('Request count, clipped on 2000 ')
    pyplot.show()


def plot_stages_part_distribution(df: pandas.DataFrame, min_time: int = 100, max_time: int = 30000, bins: int = 20):
    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)

    # io_selector = (df['op_type'] == OSD_OP_I) & (df['io_type'] == IO_WRITE_I)
    io_selector = df['op_type'] == OSD_REPOP_I
    pool_selector = (df['pools'] == 115) | (df['pools'] == 117)
    cselector = io_selector & pool_selector

    disk_vals = []
    net_vals = []
    pg_vals = []

    for min_v, max_v in zip(bins[:-1], bins[1:]):
        selector = (df['durations'] >= min_v) & (df['durations'] < max_v) & cselector

        disk = df["disk"][(df["disk"] != -1) & selector].sum()
        pg = df["wait_for_pg"][(df["wait_for_pg"] != -1) & selector].sum()
        net = df["dload"][(df["dload"] != -1) & selector].sum()

        sum = float(disk) + float(pg) + float(net)

        if sum < 1.0:
            disk_vals.append(0)
            net_vals.append(0)
            pg_vals.append(0)
        else:
            disk_vals.append(float(disk) / sum)
            net_vals.append(float(net) / sum)
            pg_vals.append(float(pg) / sum)

    pyplot.plot(range(len(disk_vals)), disk_vals, linestyle='--', marker='o', label="disk io")
    pyplot.plot(range(len(net_vals)), net_vals, linestyle='--', marker='o', label="net io")
    pyplot.plot(range(len(pg_vals)), pg_vals, linestyle='--', marker='o', label="pg lock")

    pyplot.xlabel('Request latency, logscale')
    pyplot.ylabel('Time part, consumed by different stages')
    pyplot.legend()
    pyplot.show()


def stat_by_slowness(all_ops: List[OP]):
    slow_due_to = {'net': [], 'pg': [], 'disk': []}
    for op in all_ops:
        try:
            disk = op.local_time()
            net = op.dload_time()
            pg = op.waiting_for_pg()
        except:
            continue

        if disk >= net and disk >= pg:
            slow_due_to['disk'].append(op.duration)
        elif net >= disk and net >= pg:
            slow_due_to['net'].append(op.duration)
        else:
            assert pg >= disk and pg >= net
            slow_due_to['pg'].append(op.duration)

    for key, durations in slow_due_to.items():
        durations.sort()

        ld = len(durations)
        avg = int(sum(durations) / ld)
        p50 = durations[ld // 2]
        p05 = durations[int(ld * 0.05)]
        p95 = durations[int(ld * 0.95)]

        print(f"{key:>8s} {len(durations):>8d} {int(sum(durations) / 1000):>6d}s  {avg:>6d}ms {p05:>6d} {p50:>6d} {p95:>6d}")


def iter_classes_selector(df: pandas.DataFrame) -> Iterable[Tuple[str, numpy.ndarray]]:
    for name, numbers in [("sata2", {115, 117}), ("ssd", {116}), ("sata", set(df['pools']) - {115, 116, 117})]:
        nn = list(numbers)
        selector = df['pools'] == nn[0]
        for num in nn[1:]:
            selector |= df['pools'] == num
        yield name, selector


def stat_by_slowness_pd(df: pandas.DataFrame):
    writes = (df['io_type'] == IO_WRITE_I) | (df['io_type'] == IO_UNKNOWN_I)
    at_least_one_valid = (df['dload'] != -1) | (df['wait_for_pg'] != -1) | (df['disk'] != -1)
    net_slowest = (df['dload'] > df['wait_for_pg']) & (df['dload'] > df['disk']) & writes & at_least_one_valid
    disk_slowest = (df['wait_for_pg'] > df['dload']) & (df['wait_for_pg'] > df['disk']) & writes & at_least_one_valid
    pg_slowest = (df['disk'] > df['wait_for_pg']) & (df['disk'] > df['dload']) & writes & at_least_one_valid

    for name, selector in iter_classes_selector(df):

        total_time = int(df['durations'][selector].sum() // 1000000)
        print(f"Class {name:>6s} has {selector.sum():>10d} slow requests with total time {total_time:>10d}s")
        c_net_slowest = net_slowest & selector
        c_disk_slowest = disk_slowest & selector
        c_pg_slowest = pg_slowest & selector

        print(f"  Net slowest in:  {c_net_slowest.sum():>8d} total time {df['dload'][selector].sum() // 1000000:>10d}")
        print(f"  Disk slowest in: {c_disk_slowest.sum():>8d} total time {df['wait_for_pg'][selector].sum() // 1000000:>10d}")
        print(f"  PG slowest in:   {c_pg_slowest.sum():>8d} total time {df['disk'][selector].sum() // 1000000:>10d}")

    print()
    print(f"Slowness       Count       Time,s     Avg,ms     5pc,ms      50pc,ms     95pc,ms")
    for key, durations in [('disk', df['durations'][disk_slowest]), ('net', df['durations'][net_slowest]), ('pg', df['durations'][pg_slowest])]:
        p05, p50, p95 = map(int, numpy.percentile(durations, [5, 50, 95]))
        avg = int(numpy.average(durations) / 1000)
        print(f"{key:>8s}    {len(durations):>8d}   {int(durations.sum() / 1000000):>7d}        {avg:>6d}     {p05 // 1000:>6d}       {p50 // 1000:>6d}      {p95 // 1000:>6d}")


def load_into_pandas(ops_iterator: Iterable[Tuple[int, str, int, Dict[str, str], str]]) -> pandas.DataFrame:
    qpg_at = []
    started = []
    local_done = []
    osd_ids = []
    durations = []
    op_types = []
    pools = []
    pgs = []
    io_type = []

    for osd_id, start_time, duration, stages, descr in ops_iterator:
        op_type, _ = descr.split("(", 1)

        if op_type == OSD_OP:
            op_types.append(OSD_OP_I)
        elif op_type == OSD_REPOP:
            op_types.append(OSD_REPOP_I)
        else:
            continue

        pool, pg = descr.split()[1].split(".")
        pools.append(int(pool))
        pgs.append(int(pg, 16))

        if '+write+' in descr:
            io_type.append(IO_WRITE_I)
        elif '+read+' in descr:
            io_type.append(IO_READ_I)
        else:
            io_type.append(IO_UNKNOWN_I)

        try:
            qpg_at.append(stages["queued_for_pg"])
        except KeyError:
            qpg_at.append(-1)

        try:
            started.append(stages["started"])
        except KeyError:
            started.append(-1)

        try:
            local_done.append(stages["op_applied"] if 'op_applied' in stages else stages["done"])
        except KeyError:
            local_done.append(-1)

        osd_ids.append(osd_id)
        durations.append(duration)

    started_arr = numpy.array(list(map(int, started)), dtype=numpy.int32)
    qpg_at_arr = numpy.array(list(map(int, qpg_at)), dtype=numpy.int32)
    local_arr = numpy.array(list(map(int, local_done)), dtype=numpy.int32)

    dload = qpg_at_arr

    wait_for_pg = started_arr - qpg_at_arr
    wait_for_pg[(started_arr == -1) | (qpg_at_arr == -1)] = -1

    disk = local_arr - started_arr
    disk[(local_arr == -1) | (started_arr == -1)] = -1

    return pandas.DataFrame({
        'dload': dload,
        'wait_for_pg': wait_for_pg,
        'disk': disk,
        'osd_ids': numpy.array(osd_ids, dtype=numpy.uint16),
        'durations': numpy.array(durations, dtype=numpy.uint32),
        'op_type': numpy.array(op_types, dtype=numpy.uint8),
        'io_type': numpy.array(io_type, dtype=numpy.uint8),
        'pools': numpy.array(pools, dtype=numpy.uint8),
        'pgs': numpy.array(pgs, dtype=numpy.uint32)
    })


def to_iter(fnames: List[str], limit: int = None) -> Iterator[Tuple[int, str, int, Dict[str, str], str]]:
    count = 0
    for fname in fnames:
        print(f"Start processing {fname}")
        with open(fname) as fd:
            for line in fd:
                if line.strip():
                    osd, init_at, duration, *evts, descr = line.split("|")
                    yield int(osd), init_at, int(float(duration) * 1000000), dict(evt.rsplit(" ", 1) for evt in evts), descr

                    count += 1

                    if limit and count == limit:
                        return

                    if count % 1000000 == 0:
                        print(f"Processed {count // 1000000} mlns of events")
    print(f"Total {count} events")


def convert_to_hdfs(target: str, fnames: List[str]):
    df = load_into_pandas(to_iter(fnames))
    with pandas.HDFStore(target) as fd:
        fd['load'] = df
    print(len(df))


def load_json(fname: str) -> List[OP]:
    all_ops = []
    with open(fname, 'r') as fd:
        for op_tp in json.load(fd):
            all_ops.append(OP(*op_tp))
    return all_ops


def load_hdf(fname: str) -> pandas.DataFrame:
    with pandas.HDFStore(fname) as fd:
        return fd['load']


def main(argv):
    # convert_to_hdfs(argv[1], argv[2:])
    # convert(argv[1], argv[2:])

    df = load_hdf(argv[1])
    # stat_by_slowness_pd(df)
    # show_histo(df)

    # all_ops = load_json(argv[1])
    # pg_map = json.load(open("/home/koder/workspace/support/wob2/pg_mapping.json"))
    # per_PG_OSD_stat(all_ops, pg_map)
    # stages_stat(all_ops)

    # plot_stages_part_distribution(df)
    plot_op_time_distribution(df)

    # stat_by_slowness(all_ops)
    # show_histo(all_ops)
    # p10, p100, p1000, p10000, p100000 = make_histo(all_ops)
    # print(f" 10ms: {p10:>10d}\n100ms: {p100:>10d}\n   1s: {p1000:>10d}\n  10s: {p10000:>10d}\n 100s: {p100000:>10d}")

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
