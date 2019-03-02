import collections

import itertools
import json
import sys
from enum import Enum

import math
import logging
import argparse
from typing import Iterator, Tuple, Iterable, List, Dict, Set, Any, cast, Optional, Sequence, Union, NamedTuple, \
    Mapping, MutableMapping

import numpy
import pandas
import matplotlib
from matplotlib import pyplot
from dataclasses import dataclass, field

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


#
# def analyze_pgs(pg1_path: str, pg2_path: str):
#     pg_dump1 = json.load(open(pg1_path))
#     pg_dump2 = json.load(open(pg2_path))
#
#     stats1 = {pg["pgid"]: pg for pg in pg_dump1['pg_stats']}
#     wdiff = []
#     rdiff = []
#     wdiff_osd = Counter()
#     rdiff_osd = Counter()
#     for pg2 in pg_dump2['pg_stats']:
#         if pg2['pgid'].split(".")[0] in ("117", "115"):
#             wd = pg2["stat_sum"]["num_write"] - stats1[pg2['pgid']]["stat_sum"]["num_write"]
#             wdiff.append(wd)
#             for osd_id in pg2["up"]:
#                 wdiff_osd[osd_id] += wd
#
#             rd = pg2["stat_sum"]["num_read"] - stats1[pg2['pgid']]["stat_sum"]["num_read"]
#             rdiff.append(rd)
#             rdiff_osd[pg2["up_primary"]] += rd
#
#     wdiff.sort()
#     p5, p50, p95 = numpy.percentile(wdiff, [5, 50, 95])
#     print("Per PG writes:")
#     print(f"  average   {int(numpy.average(wdiff)):>10d}")
#     print(f"      min   {wdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {wdiff[-1]:>10d}")
#
#     rdiff.sort()
#     p5, p50, p95 = numpy.percentile(rdiff, [5, 50, 95])
#     print("\nPer PG reads:")
#     print(f"  average   {int(numpy.average(rdiff)):>10d}")
#     print(f"      min   {rdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {rdiff[-1]:>10d}")
#
#     wdiff = list(wdiff_osd.values())
#     wdiff.sort()
#     p5, p50, p95 = numpy.percentile(wdiff, [5, 50, 95])
#     print("\nPer OSD writes:")
#     print(f"  average   {int(numpy.average(wdiff)):>10d}")
#     print(f"      min   {wdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {wdiff[-1]:>10d}")
#
#     rdiff = list(rdiff_osd.values())
#     rdiff.sort()
#     p5, p50, p95 = numpy.percentile(rdiff, [5, 50, 95])
#     print("\nPer OSD reads:")
#     print(f"  average   {int(numpy.average(rdiff)):>10d}")
#     print(f"      min   {rdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {rdiff[-1]:>10d}")
#
#
#
# def show_histo(df: pandas.DataFrame):
#     for name, selector in iter_classes_selector(df):
#         for is_read in (True, False):
#             if is_read:
#                 io_selector = df['op_type'] == ceph_ho_dumper.OP_READ
#             else:
#                 io_selector = (df['op_type'] == ceph_ho_dumper.OP_WRITE_SECONDARY) | \
#                               (df['io_type'] == ceph_ho_dumper.OP_WRITE_PRIMARY)
#
#             bins = numpy.array([25, 50, 100, 200, 300, 500, 700] +
#                                [1000, 3000, 5000, 10000, 20000, 30000, 100000])
#             times = df['duration'][io_selector & selector]
#             res, _ = numpy.histogram(times, bins)
#             res[-2] += res[-1]
#             print(f"\n-----------------------\n{'read' if is_read else 'write'} for class {name}")
#             for start, stop, res in zip(bins[1:-2], bins[2:-1], res[1:-1]):
#                 if res != 0:
#                     print(f"{start:>5d}ms ~ {stop:>5d}ms  {res:>8d}")
#
#
# @dataclass
# class OpsData:
#     pg_before: Dict
#     pg_after: Dict
#     all_df: pandas.DataFrame
#     slow_df: pandas.DataFrame
#     pools2classes: Dict[str, Set[int]]


# -------  COMMON FUNCTIONS  -------------------------------------------------------------------------------------------


DEFAULT_BINS = (2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000, 30000)


def get_stages_duration_2d(df: pandas.DataFrame, bins: Iterable[float] = DEFAULT_BINS) -> Dict[str, List[float]]:

    res = {
        'total_ops': [],
        'total_dur': [],
        'local_io': [],
        'download': [],
        'wait_for_pg': [],
        'min_duration': [],
        'max_duration': []}

    max_val = df['duration'].max()

    fbins = [0] + [val for val in bins if val < max_val] + [max_val]  # type: List[float]

    for min_val, max_val in zip(fbins[:-1], fbins[1:]):
        ops = df[(df['duration'] < max_val) & (df['duration'] >= min_val)]
        res['total_ops'].append(len(ops))
        res['total_dur'].append(ops['duration'].sum())
        res['local_io'].append(ops['local_io'].sum())
        res['wait_for_pg'].append(ops['wait_for_pg'].sum())
        res['download'].append(ops['download'].sum())
        res['min_duration'].append(min_val)
        res['max_duration'].append(max_val)
    return res


def seconds_to_str(seconds: Union[int, float]) -> str:
    seconds = int(seconds)

    s = seconds % 60
    m = (seconds // 60) % 60
    h = (seconds // 3600) % 24
    d = seconds // (3600 * 24)

    if d == 0:
        if h == 0:
            if s == 0:
                return f"{s:02d}"
            return f"{m:02d}:{s:02d}"
        return f"{h}:{m:02d}:{s:02d}"
    return f"{d}d {h:02d}:{m:02d}:{s:02d}"


# --- LOADERS AND CONVERTERS  ------------------------------------------------------------------------------------------


StorageType = MutableMapping[str, Union[pandas.DataFrame, pandas.Series]]


@dataclass
class ClusterInfo:
    osd2node: Dict[int, str]
    osd2class: Dict[int, str]
    pool_id2name: Dict[int, str]

    def store(self, storage: StorageType):
        osds = sorted(set(itertools.chain(self.osd2class.keys(), self.osd2node.keys())))
        classes = [self.osd2class.get(osd_id, "") for osd_id in osds]
        nodes = [self.osd2node.get(osd_id, "") for osd_id in osds]
        frame = pandas.DataFrame({"osd_ids": osds,
                                  "osd_class": classes,
                                  "osd_node": nodes})
        storage['osd_params'] = frame

        pools_ids, pools_names = map(list, zip(*self.pool_id2name.items()))
        frame = pandas.DataFrame({"pool_name": pools_names, "pool_id": pools_ids})
        storage['pool_params'] = frame

    @classmethod
    def load(cls, storage: StorageType) -> 'ClusterInfo':
        frame = storage['osd_params']
        osd2node = {itm['osd_ids']: itm['osd_node']
                    for _, itm in frame[['osd_ids', 'osd_node']].iterrows()
                    if itm['osd_node'] != ""}
        osd2class = {itm['osd_ids']: itm['osd_class']
                     for _, itm in frame[['osd_ids', 'osd_class']].iterrows()
                     if itm['osd_class'] != ""}
        frame = storage['pool_params']
        pool_id2name = {itm['pool_id']: itm['pool_name']
                        for _, itm in frame[['pool_id', 'pool_name']].iterrows()
                        if itm['pool_name'] != ""}
        return cls(osd2node, osd2class, pool_id2name)


class LogInfo(NamedTuple):
    data: pandas.DataFrame
    info: ClusterInfo
    pg_dump: pandas.DataFrame
    pg_dump_times: List[int]

    def store(self, storage: StorageType):
        storage['data'] = self.data
        self.info.store(storage)
        storage['pg_dump'] = self.pg_dump
        storage['pg_dump_time'] = pandas.Series(self.pg_dump_times)

    @classmethod
    def load(cls, storage: StorageType) -> 'LogInfo':
        data = storage['data']
        info = ClusterInfo.load(storage)
        data['node'] = [info.osd2node.get(osd_id) for osd_id in data['osd_id']]
        return cls(data, info,
                   pg_dump=storage['pg_dump'],
                   pg_dump_times=list(storage['pg_dump_time']))


def load_hdf(fname: str) -> LogInfo:
    with pandas.HDFStore(fname) as fd:
        return LogInfo.load(fd)


def iterate_op_records(fnames: List[str]) -> Iterator[Tuple[ceph_ho_dumper.RecId, Any]]:
    for fname in fnames:
        logger.info(f"Start processing {fname}")
        with open(fname, 'rb') as fd:
            # yield from ceph_ho_dumper.parse(fd)
            for rec_id, op in ceph_ho_dumper.parse(fd):
                if rec_id == ceph_ho_dumper.RecId.params:
                    if 'hostname' not in op:
                        op['hostname'] = fname.split('-', 1)[0]
                yield rec_id, op


def convert_to_hdfs(hdf5_target: str, fnames: List[str]):
    info = parse_logs_to_pd(iterate_op_records(fnames))
    with pandas.HDFStore(hdf5_target) as fd:
        info.store(fd)


def extract_from_pgdump(data: Dict[str, Any]) -> Tuple[int, Dict[str, List[int]]]:
    ctime = ceph_ho_dumper.to_unix_ms(data['stamp']) // 1000
    res: Dict[str, List[int]] = collections.defaultdict(list)

    for pg_info in data["pg_stats"]:
        pool, pgid = pg_info['"pgid"'].split('.')
        res['pgid'].append(int(pgid, 16))
        res['pool'].append(int(pool))

        for name, val in pg_info["stat_sum"].items():
            res[name].append(val)

    return ctime, res


def parse_logs_to_pd(ops_iterator: Iterable[Tuple[ceph_ho_dumper.RecId, Any]]) -> LogInfo:

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

    pg_info: Dict[str, List[int]] = collections.defaultdict(list)
    pg_dump_ctimes: List[int] = []

    pool_id2name: Dict[int, str] = {}
    hostname: Optional[str] = None
    osd2hostname: Dict[int, str] = {}

    prev = 0

    for op_tp, params in ops_iterator:
        if len(durations) // 1000000!= prev // 1000000:
            logger.debug(f"{len(durations) // 1000000:,} millions of records processed")
        prev = len(durations)
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

                if op['pool'] in pool_id2name:
                    assert pool_id2name[op['pool']] == op['pool_name'], "Pool mapping changed, this case don't handled"
                else:
                    pool_id2name[op['pool']] = op['pool_name']

                assert hostname is not None
                osd2hostname[op['osd_id']] = hostname
                pools.append(op['pool'])
                pgs.append(op['pg'])
                osd_ids.append(op['osd_id'])
                durations.append(op['duration'])
                op_types.append(tp.value)
                local_io.append(local_io_tm)
                wait_for_pg.append(wait_for_pg_tm)
                download.append(download_tm)
                wait_for_replica.append(wait_for_replica_tm)
        elif op_tp == ceph_ho_dumper.RecId.params:
            hostname = params['hostname']
        elif op_tp == ceph_ho_dumper.RecId.params:
            if ceph_ho_dumper.PG_DUMP in params:
                ctime, new_pg_data = extract_from_pgdump(params[ceph_ho_dumper.PG_DUMP])
                for name, vals in new_pg_data.items():
                    pg_info[name].extend(vals)
                pg_dump_ctimes.append(ctime)

            hostname = params['hostname']

    logger.info(f"{len(durations):,} total record processed")

    cluster_info = ClusterInfo(osd2node=osd2hostname, osd2class={}, pool_id2name=pool_id2name)
    data = pandas.DataFrame({
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

    return LogInfo(data, cluster_info, pandas.DataFrame(pg_info), pg_dump_ctimes)


# -----------   PLOT FUNCTIONS  ----------------------------------------------------------------------------------------


def plot_stages_part_distribution(df: pandas.DataFrame,
                                  min_time: int = 100,
                                  max_time: int = 30000,
                                  bins: int = 20,
                                  xticks: Tuple[int, ...] = (100, 200, 300, 500, 700, 1000, 1500,
                                                             2000, 3000, 5000, 10000, 20000, 30000)):

    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    vals = get_stages_duration_2d(df, bins)

    local_io = numpy.array(vals['local_io'])
    wait_for_pg = numpy.array(vals['wait_for_pg'])
    download = numpy.array(vals['download'])
    total = numpy.array(vals['total_dur'])

    total[total < 1] = 1
    local_io = local_io / total
    download = download / total
    wait_for_pg = wait_for_pg / total

    pyplot.plot(range(len(local_io)), local_io, marker='o', label="disk io")
    pyplot.plot(range(len(download)), download, marker='o', label="net io")
    pyplot.plot(range(len(wait_for_pg)), wait_for_pg, marker='o', label="pg lock")

    pyplot.xlabel('Request latency, logscale')
    pyplot.ylabel('Time part, consumed by different stages')

    xticks_pos = [math.log10(vl / 100) * ((len(local_io) - 1) / math.log10(30000 / 100)) for vl in xticks]
    pyplot.xticks(xticks_pos, list(map(str, xticks)))
    pyplot.legend()
    pyplot.show()


def plot_op_time_distribution(df: pandas.DataFrame,
                              min_time: int = 100,
                              max_time: int = 30000,
                              bins: int = 40,
                              clip: int = 2000):
    times = df['duration']
    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    bins = [0] + list(bins)

    if bins[-1] < times.max():
        bins.append(times.max())

    vals, _ = numpy.histogram(times, bins)
    vals[-2] += vals[-1]
    vals = vals[1:-1]
    bins = numpy.array(bins[1:-1])
    bins_centers = (bins[1:] + bins[:-1]) / 2
    vals = numpy.clip(vals, 0, clip)
    pyplot.plot(bins_centers, vals, linestyle='--', marker='o', color='b')
    pyplot.xlabel('Request latency, ms')
    pyplot.ylabel('Request count, clipped on 2000 ')
    pyplot.show()


# ---------  Table-based reports ---------------------------------------------------------------------------------------


class Aligner(Enum):
    left = '<'
    center = '^'
    right = '>'


@dataclass
class Table:
    caption: str
    headers: List[str]
    formatters: Optional[List[str]] = None
    data: List[Iterable] = field(default_factory=list, init=False)
    align : Optional[List[Aligner]] = None

    def add_line(self, *items):
        self.data.append(items)

    def add_items(self, *itms):
        cast(List, self.data[-1]).extend(itms)


def table2txt(table: Table) -> str:

    table_formatters = table.formatters if table.formatters is not None else ["{}"] * len(table.headers)

    formatted_content = [table.headers]
    for line in table.data:
        formatted_content.append([fmt.format(val) for fmt, val in zip(table_formatters, line)])

    col_widths = [max(map(len, line)) for line in zip(table.headers, *formatted_content)]
    total_w = sum(col_widths) + 3 * len(col_widths) - 1

    aligners = [Aligner.right.value] * len(table.headers) \
        if table.align is None else \
        [al.value for al in table.align]

    line_formater = "|".join(" {:%s%s} " % (aligner, col_w) for aligner, col_w in zip(aligners, col_widths))
    res = [table.caption.center(total_w)]

    for idx, line in enumerate(formatted_content):
        if idx in (0, 1):
            res.append("-" * total_w)
        res.append(line_formater.format(*line))
    res.append("-" * total_w)

    return "\n".join(res)


def top_slow_pgs(df: pandas.DataFrame, count: int = 20) -> Table:
    pg_ids = df[df['duration'] > 100].groupby(['pool', 'pg']).value_counts()
    pg_ids = pg_ids.sort_values(ascending=False)[:count].items()
    res = Table("Top slow PG:", ["Pool.PG", "Slow request count"])
    for (pool, pg), cnt in pg_ids.items():
        res.add_line(f"{pool}.{pg:x}", cnt)
    return res


def top_slow_osds(df: pandas.DataFrame, count: int = 10, time_limit: int = 100) -> Iterator[Table]:
    last_n = lambda ddf: ddf.sort_values(ascending=False)[:count].items()

    res = Table("OSDs with largest slow requests count:", ["OSD id", "Count"])
    for osd_id, cnt in last_n(df['osd_id'][df['duration'] > time_limit].value_counts()):
        res.add_line(osd_id, cnt)
    yield res

    res = Table("OSDs with longest io:", ["OSD id", "Spend time"])
    for osd_id, total in last_n(df.groupby(['osd_id'])['local_io'].agg('sum')):
        res.add_line(osd_id, seconds_to_str(total))
    yield res

    res = Table("OSDs with longest pg wait:", ["OSD id", "Spend time"])
    for osd_id, total in last_n(df.groupby(['osd_id'])['wait_for_pg'].agg('sum')):
        res.add_line(osd_id, seconds_to_str(total))
    yield res

    res = Table("OSDs with longest net transfer time:", ["OSD id", "Spend time"])
    for osd_id, total in last_n(df.groupby(['osd_id'])['download'].agg('sum')):
        res.add_line(osd_id, seconds_to_str(total))
    yield res


def top_slow_nodes(df: pandas.DataFrame, count: int = 10, time_limit: int = 100) -> Iterator[Table]:
    last_n = lambda ddf: ddf.sort_values(ascending=False)[:count].items()

    res = Table("Nodes with most slow requests counts:", ["Node", "Count"])
    for node, cnt in last_n(df['node'][df['duration'] > time_limit].value_counts()):
        res.add_line(node, cnt)
    yield res

    res = Table("Nodes with longest io:", ["Node", "Time"])
    for node, total in last_n(df.groupby(['node'])['local_io'].agg('sum')):
        res.add_line(node, seconds_to_str(total))
    yield res

    res = Table("Nodes with longest pg wait:", ["Node", "Time"])
    for node, total in last_n(df.groupby(['node'])['wait_for_pg'].agg('sum')):
        res.add_line(node, seconds_to_str(total))
    yield res

    res = Table("Nodes with longest net transfer time:", ["Node", "Time"])
    for node, total in last_n(df.groupby(['node'])['download'].agg('sum')):
        res.add_line(node, seconds_to_str(total))
    yield res


def slowness_cumulative(df: pandas.DataFrame) -> Table:
    df_f = df[(df['download'] != -1) | (df['wait_for_pg'] != -1) | (df['local_io'] != -1)]

    net_slowest = (df_f['download'] > df_f['wait_for_pg']) & (df_f['download'] > df_f['local_io'])
    pg_slowest = (df_f['wait_for_pg'] > df_f['download']) & (df_f['wait_for_pg'] > df_f['local_io'])
    disk_slowest = (df_f['local_io'] > df_f['wait_for_pg']) & (df_f['local_io'] > df_f['download'])

    MS2S = 1000
    duration = df['duration']
    res = Table("Slowness source:",
                ["Slowness", "Count", "Time,s", "Avg,ms", "5pc,ms", "50pc,ms", "95pc,ms"])

    for key, durations in [('disk', duration[disk_slowest]),
                            ('net', duration[net_slowest]),
                            ('pg', duration[pg_slowest])]:

        if len(durations) > 0:
            p05, p50, p95 = map(int, numpy.percentile(durations, [5, 50, 95]))
            avg = int(numpy.average(durations))
        else:
            p05 = p50 = p95 = ""
            avg = ""
        res.add_line(key, len(durations), int(durations.sum() / MS2S), avg, p05, p50, p95)
    return res


def duration_distribution(df: pandas.DataFrame, bins: Iterable[float]) -> Table:
    max_dura = df['duration'].max()
    hist, edges = numpy.histogram(df['duration'], [i for i in bins if i < max_dura])
    total = len(df) / 100.

    res = Table("Requests distribution by duration", ["Max duration", "Count, %", "Cumulative total, %"])
    cumulative = 0
    for upper, val in zip(edges[:-1], hist):
        cumulative += val
        res.add_line(upper, "{:>.1f}%".format(val / total), "{:>.1f}%".format(cumulative / total))
    return res


def slowness_by_duration(df: pandas.DataFrame) -> Table:
    st2dur = get_stages_duration_2d(df)
    res = Table("Slowness source for different duration ranges",
                ["Max request time(ms)", "Total ops", "% total ops",
                 "Total time", "% total time", "Net %", "Disk %", "PG wait %"])
    total_ops_count = sum(st2dur['total_ops']) / 100
    total_time = sum(st2dur['total_dur']) / 100
    for total, total_dur, disk, net, pg, max_dur in zip(st2dur['total_ops'], st2dur['total_dur'],
                                                        st2dur['local_io'], st2dur['download'],
                                                        st2dur['wait_for_pg'], st2dur['max_duration']):
        if total == 0:
            continue

        total_dur_c = 0.01 if total_dur < 1 else total_dur / 100

        res.add_line(max_dur,
                     total,
                     "{:.1f}".format(total / total_ops_count),
                     seconds_to_str(total_dur),
                     "{:.1f}".format(total_dur / total_time),
                     int(net / total_dur_c),
                     int(disk / total_dur_c),
                     int(pg / total_dur_c))
    return res


# ----------------------------------------------------------------------------------------------------------------------


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=ceph_ho_dumper.ALLOWED_LOG_LEVELS, help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    tohdf5_parser = subparsers.add_parser('tohdfs', help="Convert binary files to hds")
    tohdf5_parser.add_argument("target", help="HDF5 file path to store to")
    tohdf5_parser.add_argument("sources", nargs="+", help="record files to import")

    report_parser = subparsers.add_parser('report', help="Make a report on data from hdf5 file")
    report_parser.add_argument("--count", default=10, type=int, help="Slowest object(PG/OSD/node) count")
    report_parser.add_argument("--slow-osd", action="store_true", help="Slow osd report")
    report_parser.add_argument("--source-by-duration", action="store_true", help="Show source by duration report")
    report_parser.add_argument("--slow-node", action="store_true", help="Slow osd report")
    report_parser.add_argument("--slowness-source", action="store_true", help="Slow source for slow requests")
    report_parser.add_argument("--duration-distribution", action="store_true", help="Show all reports")

    plot_parser = subparsers.add_parser('plot', help="Make a report on data from hdf5 file")
    plot_parser.add_argument("--slowness-source", action="store_true", help="Slow source for slow requests")
    plot_parser.add_argument("--duration-distribution", action="store_true", help="Show all reports")

    for rparser in (plot_parser, report_parser):
        rparser.add_argument("--all", action="store_true", help="Slow source for slow requests")
        rparser.add_argument("hdf5_file", help="HDF5 file to read data from")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    ceph_ho_dumper.setup_logger(logger, opts.log_level, opts.log)

    if opts.subparser_name == 'tohdfs':
        convert_to_hdfs(opts.target, opts.sources)
        return 0

    if opts.subparser_name == 'report' or opts.subparser_name == 'plot':

        info = load_hdf(opts.hdf5_file)
        df = info.data

        primary_writes = df[df['op_type'] == ceph_ho_dumper.OpType.write_primary.value]
        secondary_writes = df[df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value]
        writes = df[(df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value) |
                    (df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value)]
        reads = df[df['op_type'] == ceph_ho_dumper.OpType.read.value]

        if opts.subparser_name == 'plot':
            if opts.all or opts.slowness_source:
                plot_stages_part_distribution(df)

            if opts.all or opts.duration_distribution:
                plot_op_time_distribution(df, max_time=10000, clip=50000)
        else:
            if opts.all or opts.duration_distribution:
                print(table2txt(duration_distribution(df, DEFAULT_BINS)))
                print()

            if opts.all or opts.slowness_source:
                print(table2txt(slowness_by_duration(df)))
                print()
                print(table2txt(slowness_cumulative(writes)))
                print()

            if opts.all or opts.slow_osd:
                for table in top_slow_osds(df, opts.count):
                    print(table2txt(table))
                    print()

            if opts.slow_node or opts.all and 'node' in df:
                for table in top_slow_nodes(df, opts.count):
                    print(table2txt(table))
                    print()

        return 0

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
