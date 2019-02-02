import bisect
import math
import sys
import json
from collections import Counter, defaultdict
from typing import Dict, Iterator, Tuple, Iterable, List, Set

import numpy
import seaborn
from matplotlib import pyplot
from dataclasses import dataclass


IO_WRITE = "write"
IO_READ = "read"
IO_UNKNOWN = "unknown"

OSD_OP = "osd_op"
OSD_REPOP = "osd_repop"


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


def make_histo(ops: Iterable[OP]) ->Tuple[int, int, int, int, int]:
    res = [0, 0, 0, 0, 0]
    for op in ops:
        if op.op_type == OSD_REPOP:
            if op.duration < 10:
                res[0] += 1
            elif op.duration >= 10000:
                res[4] += 1
            elif op.duration < 100:
                res[1] += 1
            elif op.duration < 1000:
                res[2] += 1
            else:
                assert op.duration < 10000
                res[3] += 1
    return tuple(res)


def top_slow_pgs(ops: Iterable[OP]):
    res = Counter()
    for op in ops:
        res[op.pg] += 1

    for pg, cnt in sorted(res.items(), key=lambda x: -x[1])[:10]:
        print(f"{pg:>8s}  {cnt:>5d}")


def top_slow_osds(ops: Iterable[OP], pg_map: Dict[str, List[int]]):
    res = Counter()
    for op in ops:
        for osd in pg_map[op.pg]:
            res[osd] += 1

    for osd, cnt in sorted(res.items(), key=lambda x: -x[1])[:15]:
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


def per_PG_OSD_stat(all_ops: List[OP]):
    pg_map = json.load(open("pg_mapping.json"))
    longer_100ms = list(filter_duration(all_ops, 100, 1000))
    longer_1s = list(filter_duration(all_ops, 1000, 10000))
    longer_10s = list(filter_duration(all_ops, 10000, 100000))

    print("Reads most slow OSDS")
    print("------------- 100ms -----------------")
    top_slow_osds(filter_iotype(longer_100ms, {IO_READ}), pg_map)
    print("------------- 1s --------------------")
    top_slow_osds(filter_iotype(longer_1s, {IO_READ}), pg_map)
    print("------------- 10s -------------------")
    top_slow_osds(filter_iotype(longer_10s, {IO_READ}), pg_map)

    print("Writes most slow OSDS")
    print("------------- 100ms -----------------")
    top_slow_osds(filter_iotype(longer_100ms, {IO_WRITE}), pg_map)
    print("------------- 1s --------------------")
    top_slow_osds(filter_iotype(longer_1s, {IO_WRITE}), pg_map)
    print("------------- 10s -------------------")
    top_slow_osds(filter_iotype(longer_10s, {IO_WRITE}), pg_map)

    print("Reads most slow PGS")
    print("------------- 100ms -----------------")
    top_slow_pgs(filter_iotype(longer_100ms, {IO_READ}))
    print("------------- 1s --------------------")
    top_slow_pgs(filter_iotype(longer_1s, {IO_READ}))
    print("------------- 10s -------------------")
    top_slow_pgs(filter_iotype(longer_10s, {IO_READ}))

    print("Writes most slow PGS")
    print("------------- 100ms -----------------")
    top_slow_pgs(filter_iotype(longer_100ms, {IO_WRITE}))
    print("------------- 1s --------------------")
    top_slow_pgs(filter_iotype(longer_1s, {IO_WRITE}))
    print("------------- 10s -------------------")
    top_slow_pgs(filter_iotype(longer_10s, {IO_WRITE}))


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


def plot_op_time_distribution(all_ops: List[OP], min_time: int = 100, max_time: int = 30000, bins: int = 40):
    write_ops = filter_optype(filter_iotype(all_ops, {IO_WRITE}), {OSD_OP})
    times = [op.duration for op in write_ops]
    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    bins = [0] + list(bins) + [max(times)]
    vals, _ = numpy.histogram(times, bins)
    vals[-2] += vals[-1]
    vals = vals[1:-1]
    bins = numpy.array(bins[1:-1])
    bins_centers = (bins[1:] + bins[:-1]) / 2
    vals = numpy.clip(vals, 0, 200)
    pyplot.plot(bins_centers, vals, linestyle='--', marker='o', color='b')
    pyplot.show()


def plot_stages_part_distribution(all_ops: List[OP], min_time: int = 100, max_time: int = 30000, bins: int = 40):
    write_ops = filter_iotype(all_ops, {IO_WRITE})
    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    per_stage = [defaultdict(int) for _ in bins]
    for op in write_ops:
        if op.duration < min_time:
            continue
        if op.duration > max_time:
            idx = len(per_stage) - 1
        else:
            idx = bisect.bisect_left(bins, op.duration)
        stage_times = per_stage[idx]
        try:
            stage_times["dload"] += op.dload_time()
            stage_times["waiting_for_pg"] += op.waiting_for_pg()
            stage_times["local"] += op.local_time()
        except:
            pass

    scaled = []
    for dct in per_stage:
        cum = sum(dct.values())
        if cum != 0:
            dct = {k: v / cum for k, v in dct.items()}
        scaled.append(dct)

    for stage in ("dload", "waiting_for_pg", "local"):
        vals = [dct.get(stage, 0) for dct in scaled]
        pyplot.plot(range(len(vals)), vals, linestyle='--', marker='o', label=stage)

    pyplot.legend()
    pyplot.show()


def main(argv):
    # convert(argv[1], argv[2:])
    all_ops = []
    with open(argv[1], 'r') as fd:
        for op_tp in json.load(fd):
            all_ops.append(OP(*op_tp))

    # per_PG_OSD_stat(all_ops)
    # stages_stat(all_ops)

    plot_stages_part_distribution(all_ops)

    # p10, p100, p1000, p10000, p100000 = make_histo(all_ops)
    # print(f" 10ms: {p10:>10d}\n100ms: {p100:>10d}\n   1s: {p1000:>10d}\n  10s: {p10000:>10d}\n 100s: {p100000:>10d}")

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
