import argparse
import sys
import math
import mmap
import json
import logging
from collections import Counter
from typing import Iterator, Tuple, Iterable, List, Dict, Set, Any, cast

import numpy
import pandas
import matplotlib
from dataclasses import dataclass
from matplotlib import pyplot

import ceph_ho_dumper


matplotlib.rcParams.update({'font.size': 30, 'lines.linewidth': 5})


logger = logging.getLogger()

#
#
# def show_per_pg_stat(ops: Iterable[OP]):
#     per_pg = Counter()
#     total_per_pool = Counter()
#     all_pg = set()
#
#     for op in ops:
#         if op.io_type == IO_WRITE:
#             per_pg[op.pg] += 1
#             pool, _ = op.pg.split(".")
#             total_per_pool[pool] += 1
#             all_pg.add(op.pg)
#
#     total_117_pg = len([pg for pg in all_pg if pg.startswith("117.")])
#
#     print(f"total pg 117 = {total_117_pg}")
#     tt = 0
#     for pg, count in sorted(((pg, cnt) for pg, cnt in per_pg.items() if pg.startswith("117.")), key=lambda x: -x[1])[:20]:
#         pool, _ = pg.split(".")
#         print(f"{pg:>8s}  {count:>8d}   {round(count * 1000 / total_per_pool[pool]) / 10:1.1f}%")
#         tt += count
#     print(f"Total for first 20 = {tt / total_per_pool['117'] * 100:.1f}%")
#
#
# def per_PG_OSD_stat(all_ops: List[OP], pg_map: Dict):
#     longer_100ms = list(filter_duration(all_ops, 100))
#     # longer_1s = list(filter_duration(all_ops, 1000, 10000))
#     # longer_10s = list(filter_duration(all_ops, 10000, 100000))
#
#     print("Reads most slow OSDS")
#     top_slow_osds(filter_iotype(longer_100ms, {IO_READ}), pg_map, True)
#
#     print("Writes most slow OSDS")
#     print("------------- 100ms -----------------")
#     top_slow_osds(filter_iotype(longer_100ms, {IO_WRITE}), pg_map)
#
#     print("Reads most slow PGS")
#     print("------------- 100ms -----------------")
#     top_slow_pgs(filter_iotype(longer_100ms, {IO_READ}))
#
#     print("Writes most slow PGS")
#     print("------------- 100ms -----------------")
#     top_slow_pgs(filter_iotype(longer_100ms, {IO_WRITE}))
#
#
# def stages_stat(ops: List[OP]):
#     longer_100ms = list(filter_duration(ops, 0, 450))
#     longer_300ms = list(filter_duration(ops, 450, 800))
#     longer_1s = list(filter_duration(ops, 800))
#
#     print("OP")
#
#     for lst in (longer_100ms, longer_300ms, longer_1s):
#         stage_times = defaultdict(int)
#         for op in filter_optype(filter_iotype(lst, {IO_WRITE}), {OSD_OP}):
#             try:
#                 stage_times["dload"] += op.dload_time()
#                 stage_times["waiting_for_pg"] += op.waiting_for_pg()
#                 stage_times["waiting_for_subop"] += op.waiting_for_subop()
#                 stage_times["local"] += op.local_time()
#             except:
#                 # print(op)
#                 pass
#
#         for stage in ("dload", "waiting_for_pg", "waiting_for_subop", "local"):
#             print(f"{stage:>20s}  {stage_times[stage] // 1000:>6d}")
#         print()
#
#     print("REPOP")
#
#     for lst in (longer_100ms, longer_300ms, longer_1s):
#         stage_times = defaultdict(int)
#         for op in filter_optype(lst, {OSD_REPOP}):
#             try:
#                 stage_times["dload"] += op.dload_time()
#                 stage_times["waiting_for_pg"] += op.waiting_for_pg()
#                 stage_times["local"] += op.local_time()
#             except:
#                 print(op)
#                 pass
#
#         for stage in ("dload", "waiting_for_pg", "local"):
#             print(f"{stage:>20s}  {stage_times[stage] // 1000:>6d}")
#         print()
#
#
# def stat_by_slowness(all_ops: List[OP]):
#     slow_due_to = {'net': [], 'pg': [], 'disk': []}
#     for op in all_ops:
#         try:
#             disk = op.local_time()
#             net = op.dload_time()
#             pg = op.waiting_for_pg()
#         except:
#             continue
#
#         if disk >= net and disk >= pg:
#             slow_due_to['disk'].append(op.duration)
#         elif net >= disk and net >= pg:
#             slow_due_to['net'].append(op.duration)
#         else:
#             assert pg >= disk and pg >= net
#             slow_due_to['pg'].append(op.duration)
#
#     for key, durations in slow_due_to.items():
#         durations.sort()
#
#         ld = len(durations)
#         avg = int(sum(durations) / ld)
#         p50 = durations[ld // 2]
#         p05 = durations[int(ld * 0.05)]
#         p95 = durations[int(ld * 0.95)]
#
#         print(f"{key:>8s} {len(durations):>8d} {int(sum(durations) / 1000):>6d}s  {avg:>6d}ms {p05:>6d} {p50:>6d} {p95:>6d}")
#
#
# def load_json(fname: str) -> List[OP]:
#     all_ops = []
#     with open(fname, 'r') as fd:
#         for op_tp in json.load(fd):
#             all_ops.append(OP(*op_tp))
#     return all_ops


@dataclass
class OpsData:
    pg_before: Dict
    pg_after: Dict
    all_df: pandas.DataFrame
    slow_df: pandas.DataFrame
    pools2classes: Dict[str, Set[int]]


def top_slow_pgs(df: pandas.DataFrame):
    pgmax = df['pg'].max()
    pg_sorted = (df['pool'].astype('uint64') * pgmax + df['pg'])[df['duration'] > 100].value_counts().sort_values(ascending=False)
    for pool_pg, cnt in pg_sorted[:30].items():
        pool_pg = f"{pool_pg // pgmax}.{pool_pg % pgmax:x}"
        print(f"{pool_pg:>8}  {cnt:>5d}")


def top_slow_osds(df: pandas.DataFrame):
    print("OSD with most slow requests")
    for osd_id, cnt in df['osd_id'][df['duration'] > 100].value_counts().sort_values(ascending=False)[:30].items():
        print(f"{osd_id:>6}  {cnt:>5d}")


def show_histo(df: pandas.DataFrame):
    for name, selector in iter_classes_selector(df):
        for is_read in (True, False):
            if is_read:
                io_selector = df['op_type'] == ceph_ho_dumper.OP_READ
            else:
                io_selector = (df['op_type'] == ceph_ho_dumper.OP_WRITE_SECONDARY) | \
                              (df['io_type'] == ceph_ho_dumper.OP_WRITE_PRIMARY)

            bins = numpy.array([25, 50, 100, 200, 300, 500, 700] +
                               [1000, 3000, 5000, 10000, 20000, 30000, 100000])
            times = df['duration'][io_selector & selector]
            res, _ = numpy.histogram(times, bins)
            res[-2] += res[-1]
            print(f"\n-----------------------\n{'read' if is_read else 'write'} for class {name}")
            for start, stop, res in zip(bins[1:-2], bins[2:-1], res[1:-1]):
                if res != 0:
                    print(f"{start:>5d}ms ~ {stop:>5d}ms  {res:>8d}")


def plot_op_time_distribution(df: pandas.DataFrame,
                              selector: Any,
                              min_time: int = 100,
                              max_time: int = 30000,
                              bins: int = 40):
    times = df['duration'][selector]
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


def plot_stages_part_distribution(df: pandas.DataFrame,
                                  cselector: Any,
                                  min_time: int = 100,
                                  max_time: int = 30000,
                                  bins: int = 20,
                                  xticks: Tuple[int, ...]=(100, 200, 300, 500, 700, 1000, 1500,
                                                           2000, 3000, 5000, 10000, 20000, 30000)):
    disk_vals = []
    net_vals = []
    pg_vals = []

    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    for min_v, max_v in zip(bins[:-1], bins[1:]):
        selector = (df['duration'] >= min_v) & (df['duration'] < max_v) & cselector

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

    pyplot.plot(range(len(disk_vals)), disk_vals, marker='o', label="disk io")
    pyplot.plot(range(len(net_vals)), net_vals, marker='o', label="net io")
    pyplot.plot(range(len(pg_vals)), pg_vals, marker='o', label="pg lock")

    pyplot.xlabel('Request latency, logscale')
    pyplot.ylabel('Time part, consumed by different stages')

    xticks_pos = [math.log10(vl / 100) * ((len(disk_vals) - 1) / math.log10(30000 / 100)) for vl in xticks]
    pyplot.xticks(xticks_pos, list(map(str, xticks)))
    pyplot.legend()
    pyplot.show()


def iter_classes_selector(df: pandas.DataFrame) -> Iterable[Tuple[str, numpy.ndarray]]:
    for name, numbers in [("sata2", {115, 117}), ("ssd", {116}), ("sata", set(df['pool']) - {115, 116, 117})]:
        nn = list(numbers)
        selector = df['pool'] == nn[0]
        for num in nn[1:]:
            selector |= df['pool'] == num
        yield name, selector


def stat_by_slowness_pd(df: pandas.DataFrame, selector: Any):
    at_least_one_valid = (df['download'] != -1) | (df['wait_for_pg'] != -1) | (df['local_io'] != -1)
    net_slowest = (df['download'] > df['wait_for_pg']) & (df['download'] > df['local_io']) & selector & at_least_one_valid
    disk_slowest = (df['wait_for_pg'] > df['download']) & (df['wait_for_pg'] > df['local_io']) & selector & at_least_one_valid
    pg_slowest = (df['download'] > df['wait_for_pg']) & (df['local_io'] > df['download']) & selector & at_least_one_valid

    MS2S = 1000

    # for name, selector in iter_classes_selector(df):
    #
    #     total_time = int(df['duration'][selector].sum() // MS2S)
    #     print(f"Class {name:>6s} has {selector.sum():>10d} slow requests with total time {total_time:>10d}s")
    #     c_net_slowest = net_slowest & selector
    #     c_disk_slowest = disk_slowest & selector
    #     c_pg_slowest = pg_slowest & selector
    #
    #     print(f"  Net slowest in:  {c_net_slowest.sum():>8d} total time {df['download'][selector].sum() // MS2S:>10d}")
    #     print(f"  Disk slowest in: {c_disk_slowest.sum():>8d} total time {df['wait_for_pg'][selector].sum() // MS2S:>10d}")
    #     print(f"  PG slowest in:   {c_pg_slowest.sum():>8d} total time {df['local_io'][selector].sum() // MS2S:>10d}")

    # print()
    print(f"Slowness       Count       Time,s     Avg,ms     5pc,ms      50pc,ms     95pc,ms")
    for key, durations in [('disk', df['duration'][disk_slowest]), ('net', df['duration'][net_slowest]), ('pg', df['duration'][pg_slowest])]:
        p05, p50, p95 = map(int, numpy.percentile(durations, [5, 50, 95]))
        avg = int(numpy.average(durations))
        print(f"{key:>8s}    {len(durations):>8d}   {int(durations.sum() / MS2S):>7d}        {avg:>6d}     {p05:>6d}       {p50:>6d}      {p95:>6d}")


def parse_logs_info_PD(ops_iterator: Iterable[Tuple[ceph_ho_dumper.RecId, Any]]) -> pandas.DataFrame:
    osd_ids = []
    durations = []
    op_types = []
    pools = []
    pgs = []

    local_io = []
    wait_for_pg = []
    download = []
    wait_for_replica = []

    supported_types = (ceph_ho_dumper.OpType.read,
                       ceph_ho_dumper.OpType.write_secondary,
                       ceph_ho_dumper.OpType.write_primary)

    pool_ids = {}

    for op_tp, params in ops_iterator:
        if op_tp == ceph_ho_dumper.RecId.ops:
            for op in cast(List[Dict[str, Any]], params):
                tp = op['tp']
                if tp not in supported_types:
                    continue

                assert 'pool' in op, str(op)

                if op['packer'] == ceph_ho_dumper.RawPacker.name:
                    download_tm, wait_for_pg_tm, local_io_tm, wait_for_replica_tm = \
                        ceph_ho_dumper.get_hl_timings(tp, op)
                else:
                    download_tm = op.get('download', -1)
                    wait_for_replica_tm = op.get('wait_for_replica', -1)
                    local_io_tm = op['local_io']
                    wait_for_pg_tm = op['wait_for_pg']

                if op['pool'] in pool_ids:
                    assert pool_ids[op['pool']] == op['pool_name'], "Pool mapping changed, this case don't handled"
                else:
                    pool_ids[op['pool']] = op['pool_name']

                pools.append(op['pool'])
                pgs.append(op['pg'])
                osd_ids.append(op['osd_id'])
                durations.append(op['duration'])
                op_types.append(tp.value)
                local_io.append(local_io_tm)
                wait_for_pg.append(wait_for_pg_tm)
                download.append(download_tm)
                wait_for_replica.append(wait_for_replica_tm)

    return pandas.DataFrame({
        'download': numpy.array(download, dtype=numpy.uint16),
        'wait_for_pg': numpy.array(wait_for_pg, dtype=numpy.uint16),
        'local_io': numpy.array(local_io, dtype=numpy.uint16),
        'wait_for_replica': numpy.array(wait_for_replica, dtype=numpy.uint16),
        'osd_id': numpy.array(osd_ids, dtype=numpy.uint16),
        'duration': numpy.array(durations, dtype=numpy.uint32),
        'op_type': numpy.array(op_types, dtype=numpy.uint8),
        'pool': numpy.array(pools, dtype=numpy.uint8),
        'pg': numpy.array(pgs, dtype=numpy.uint32)
    })


def iterate_op_records(fnames: List[str]) -> Iterator[Tuple[ceph_ho_dumper.RecId, Any]]:
    for fname in fnames:
        logger.info(f"Start processing {fname}")
        with open(fname, 'rb') as fd:
            yield from ceph_ho_dumper.parse(fd)


def convert_to_hdfs(hdf5_target: str, fnames: List[str]):
    df = parse_logs_info_PD(iterate_op_records(fnames))
    with pandas.HDFStore(hdf5_target) as fd:
        fd['load'] = df


def load_hdf(fname: str) -> pandas.DataFrame:
    with pandas.HDFStore(fname) as fd:
        return fd['load']


def analyze_pgs(pg1_path: str, pg2_path: str):
    pg_dump1 = json.load(open(pg1_path))
    pg_dump2 = json.load(open(pg2_path))

    stats1 = {pg["pgid"]: pg for pg in pg_dump1['pg_stats']}
    wdiff = []
    rdiff = []
    wdiff_osd = Counter()
    rdiff_osd = Counter()
    for pg2 in pg_dump2['pg_stats']:
        if pg2['pgid'].split(".")[0] in ("117", "115"):
            wd = pg2["stat_sum"]["num_write"] - stats1[pg2['pgid']]["stat_sum"]["num_write"]
            wdiff.append(wd)
            for osd_id in pg2["up"]:
                wdiff_osd[osd_id] += wd

            rd = pg2["stat_sum"]["num_read"] - stats1[pg2['pgid']]["stat_sum"]["num_read"]
            rdiff.append(rd)
            rdiff_osd[pg2["up_primary"]] += rd

    wdiff.sort()
    p5, p50, p95 = numpy.percentile(wdiff, [5, 50, 95])
    print("Per PG writes:")
    print(f"  average   {int(numpy.average(wdiff)):>10d}")
    print(f"      min   {wdiff[0]:>10d}")
    print(f"    5perc   {int(p5):>10d}")
    print(f"   50perc   {int(p50):>10d}")
    print(f"   95perc   {int(p95):>10d}")
    print(f"      max   {wdiff[-1]:>10d}")

    rdiff.sort()
    p5, p50, p95 = numpy.percentile(rdiff, [5, 50, 95])
    print("\nPer PG reads:")
    print(f"  average   {int(numpy.average(rdiff)):>10d}")
    print(f"      min   {rdiff[0]:>10d}")
    print(f"    5perc   {int(p5):>10d}")
    print(f"   50perc   {int(p50):>10d}")
    print(f"   95perc   {int(p95):>10d}")
    print(f"      max   {rdiff[-1]:>10d}")

    wdiff = list(wdiff_osd.values())
    wdiff.sort()
    p5, p50, p95 = numpy.percentile(wdiff, [5, 50, 95])
    print("\nPer OSD writes:")
    print(f"  average   {int(numpy.average(wdiff)):>10d}")
    print(f"      min   {wdiff[0]:>10d}")
    print(f"    5perc   {int(p5):>10d}")
    print(f"   50perc   {int(p50):>10d}")
    print(f"   95perc   {int(p95):>10d}")
    print(f"      max   {wdiff[-1]:>10d}")

    rdiff = list(rdiff_osd.values())
    rdiff.sort()
    p5, p50, p95 = numpy.percentile(rdiff, [5, 50, 95])
    print("\nPer OSD reads:")
    print(f"  average   {int(numpy.average(rdiff)):>10d}")
    print(f"      min   {rdiff[0]:>10d}")
    print(f"    5perc   {int(p5):>10d}")
    print(f"   50perc   {int(p50):>10d}")
    print(f"   95perc   {int(p95):>10d}")
    print(f"      max   {rdiff[-1]:>10d}")


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=ceph_ho_dumper.ALLOWED_LOG_LEVELS, help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    tohdf5_parser = subparsers.add_parser('tohdfs', help="Convert binary files to hds")
    tohdf5_parser.add_argument("target", help="HDF5 file path to store to")
    tohdf5_parser.add_argument("sources", nargs="+", help="record files to import")

    report_parser = subparsers.add_parser('report', help="Make a report on data from hdf5 file")
    report_parser.add_argument("--slow-osd", action="store_true", help="Slow osd report")
    report_parser.add_argument("--slowness-source", action="store_true", help="Slow source for slow requests")
    report_parser.add_argument("--all", action="store_true", help="Show all reports")
    report_parser.add_argument("hdf5_file", help="HDF5 file to read data from")

    pg_dump_parser = subparsers.add_parser('pg_dump_report', help="Show difference between two pg dumps")
    pg_dump_parser.add_argument("old_pg_dump_file", help="Older pg dump file")
    pg_dump_parser.add_argument("new_pg_dump_file", help="Newer pg dump file")

    # set_parser.add_argument("sources", nargs="+", help="record files to import")

    # subparsers.add_parser('set_default', help="config osd's historic ops to default 20/600")
    #
    # record_parser = subparsers.add_parser('record', help="Dump osd's requests periodically")
    # record_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    # record_parser.add_argument("--size", required=True, type=int, help="Num request to keep")
    # record_parser.add_argument("--timeout", type=int, default=30, help="Timeout to run cli cmds")
    # assert CompactPacker in ALL_PACKERS
    # record_parser.add_argument("--packer", default=CompactPacker.name,
    #                            choices=[packer.name for packer in ALL_PACKERS], help="Select command packer")
    # record_parser.add_argument("--min-duration", type=int, default=30,
    #                            help="Minimal duration in ms for op to be recorded")
    # record_parser.add_argument("--record-cluster", type=int, help="Record cluster info every SECONDS seconds",
    #                            metavar='SECONDS', default=0)
    # record_parser.add_argument("--record-pg-dump", type=int, help="Record cluster pg dump info every SECONDS seconds",
    #                            metavar='SECONDS', default=0)
    # record_parser.add_argument("output_file", help="Filename to append requests logs to it")
    #
    # parse_parser = subparsers.add_parser('parse', help="Parse records from file")
    # parse_parser.add_argument("-l", "--limit", default=None, type=int, metavar="COUNT", help="Parse only COUNT records")
    # parse_parser.add_argument("file", help="Log file")
    #
    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    ceph_ho_dumper.setup_logger(logger, opts.log_level, opts.log)

    if opts.subparser_name == 'tohdfs':
        convert_to_hdfs(opts.target, opts.sources)
        return 0

    if opts.subparser_name == 'report':
        df = load_hdf(opts.hdf5_file)
        if opts.slow_osd or opts.all:
            top_slow_osds(df)
        if opts.slowness_source or opts.all:
            primary_writes_s = df.op_type == ceph_ho_dumper.OpType.write_primary.value
            secondary_writes_s = df.op_type == ceph_ho_dumper.OpType.write_secondary.value
            stat_by_slowness_pd(df, primary_writes_s | secondary_writes_s)
        return 0

    # p10, p100, p1000, p10000, p100000 = make_histo(all_ops)
    # print(f" 10ms: {p10:>10d}\n100ms: {p100:>10d}\n   1s: {p1000:>10d}\n  10s: {p10000:>10d}\n 100s: {p100000:>10d}")

    if opts.subparser_name == 'pg_dump_report':
        analyze_pgs(opts.old_pg_dump_file, opts.new_pg_dump_file)

    assert False, "Unknonw cmd {}".format(opts.subparser_name)


    # sata2_pools_s = (df.pool == 115) | (df.pool == 117)
    slow_req_s = df.duration > 100

    # stat_by_slowness_pd(df, sata2_pools_s & writes)
    # show_histo(df)
    # top_slow_pgs(df)
    plot_stages_part_distribution(df, (primary_writes_s | secondary_writes_s))
    # plot_op_time_distribution(df, sata2_pools_s & primary_writes_s)

    # per_PG_OSD_stat(all_ops, pg_map)
    # stages_stat(all_ops)
    # stat_by_slowness(all_ops)

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
