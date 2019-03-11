import os
import sys

import math
import logging
import argparse
import itertools
import collections
from enum import Enum
from typing import Iterator, Tuple, Iterable, List, Dict, Any, cast, Optional, Union, NamedTuple, MutableMapping

import numpy
import pandas
import matplotlib
from matplotlib import pyplot
from dataclasses import dataclass, field

import ceph_ho_dumper


matplotlib.rcParams.update({'font.size': 30, 'lines.linewidth': 5})


logger = logging.getLogger()


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
            if m == 0:
                if s == 0:
                    return 0
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
    pool_info: pandas.DataFrame
    pool_info_times: List[int]

    def store(self, storage: StorageType):
        storage['data'] = self.data.drop(['host', 'pool_name'], axis=1)
        self.info.store(storage)
        storage['pg_dump'] = self.pg_dump
        storage['pg_dump_time'] = pandas.Series(self.pg_dump_times)
        storage['pool_info'] = self.pool_info
        storage['pool_info_times'] = pandas.Series(self.pool_info_times)

    @classmethod
    def load(cls, storage: StorageType) -> 'LogInfo':
        info = ClusterInfo.load(storage)
        data = storage['data']
        pool_names = [info.pool_id2name.get(pool_id) for pool_id in data['pool_id']]
        data['pool_name'] = pandas.Series(pool_names).astype('category')
        data['host'] = pandas.Series([info.osd2node.get(osd_id) for osd_id in data['osd_id']]).astype('category')

        return cls(data=data, info=info,
                   pg_dump=storage['pg_dump'],
                   pg_dump_times=list(storage['pg_dump_time']),
                   pool_info=storage['pool_info'],
                   pool_info_times=list(storage['pool_info_times']))


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
        pool, pgid = pg_info['pgid'].split('.')
        res['pgid'].append(int(pgid, 16))
        res['pool_id'].append(int(pool))

        for name, val in pg_info["stat_sum"].items():
            res[name].append(val)

    return ctime, res


def extract_from_pool_df(data: Dict[str, Any]) -> Dict[str, Union[List[str], List[int]]]:
    res: Dict[str, List[int]] = collections.defaultdict(list)

    for pool_info in data["pools"]:
        for name, val in pool_info.items():
            res[name].append(val)

    return res


def parse_logs_to_pd(ops_iterator: Iterable[Tuple[ceph_ho_dumper.RecId, Any]]) -> LogInfo:

    osd_ids: List[int] = []
    durations: List[int] = []
    op_types: List[int] = []
    pool_ids: List[int] = []
    pgs: List[int] = []
    pool_name: List[str] = []
    hosts: List[str] = []

    local_io: List[int] = []
    wait_for_pg: List[int] = []
    download: List[int] = []
    wait_for_replica: List[int] = []
    rec_time: List[int] = []

    supported_types = (ceph_ho_dumper.OpType.read,
                       ceph_ho_dumper.OpType.write_secondary,
                       ceph_ho_dumper.OpType.write_primary)

    pg_info: Dict[str, List[int]] = collections.defaultdict(list)
    pg_dump_times: List[int] = []
    pool_info: Dict[str, Union[List[int], List[int]]] = collections.defaultdict(list)
    pool_info_times: List[int] = []

    pool_id2name: Dict[int, str] = {}
    hostname: Optional[str] = None
    osd2hostname: Dict[int, str] = {}

    prev = 0

    for op_tp, params in ops_iterator:
        if len(durations) // 1000000 != prev // 1000000:
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
                    download_tm = op.get('download', 0)
                    wait_for_replica_tm = op.get('wait_for_replica', 0)
                    local_io_tm = op['local_io']
                    wait_for_pg_tm = op['wait_for_pg']

                if op['pool'] in pool_id2name:
                    assert pool_id2name[op['pool']] == op['pool_name'], "Pool mapping changed, this case don't handled"
                else:
                    pool_id2name[op['pool']] = op['pool_name']

                assert hostname is not None
                osd2hostname[op['osd_id']] = hostname
                pool_ids.append(op['pool'])
                pgs.append(op['pg'])
                osd_ids.append(op['osd_id'])
                durations.append(op['duration'])
                op_types.append(tp.value)
                local_io.append(local_io_tm)
                wait_for_pg.append(wait_for_pg_tm)
                download.append(download_tm)
                wait_for_replica.append(wait_for_replica_tm)
                rec_time.append(op['time'])
                pool_name.append(op['pool_name'])
                hosts.append(hostname)

        elif op_tp == ceph_ho_dumper.RecId.params:
            hostname = params['hostname']
        elif op_tp == ceph_ho_dumper.RecId.cluster_info:
            if ceph_ho_dumper.PG_DUMP in params:
                ctime, new_pg_data = extract_from_pgdump(params[ceph_ho_dumper.PG_DUMP])
                for name, vals in new_pg_data.items():
                    pg_info[name].extend(vals)
                pg_dump_times.append(ctime)

            if ceph_ho_dumper.RADOS_DF in params:
                new_pool_data = extract_from_pool_df(params[ceph_ho_dumper.RADOS_DF])
                data_len = None
                for name, vals in new_pool_data.items():
                    pool_info[name].extend(vals)
                    if data_len is None:
                        data_len = len(vals)
                    else:
                        assert data_len == len(vals)

                if data_len:
                    pool_info_times.extend([params.get('time', 0)] * data_len)

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
        'pool_id': numpy.array(pool_ids, dtype=numpy.uint8),
        'pg': numpy.array(pgs, dtype=numpy.uint32),
        'time': numpy.array(rec_time, dtype=numpy.uint32),
        'pool_name': pandas.Series(pool_name).astype('category'),
        'host': pandas.Series(hosts).astype('category'),
    })

    return LogInfo(data, cluster_info,
                   pg_dump=pandas.DataFrame(pg_info),
                   pg_dump_times=pg_dump_times,
                   pool_info=pandas.DataFrame(pool_info),
                   pool_info_times=pool_info_times)


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
    for node, cnt in last_n(df['host'][df['duration'] > time_limit].value_counts()):
        res.add_line(node, cnt)
    yield res

    res = Table("Nodes with longest io:", ["Node", "Time"])
    for node, total in last_n(df.groupby(['host'])['local_io'].agg('sum')):
        res.add_line(node, seconds_to_str(total))
    yield res

    res = Table("Nodes with longest pg wait:", ["Node", "Time"])
    for node, total in last_n(df.groupby(['host'])['wait_for_pg'].agg('sum')):
        res.add_line(node, seconds_to_str(total))
    yield res

    res = Table("Nodes with longest net transfer time:", ["Node", "Time"])
    for node, total in last_n(df.groupby(['host'])['download'].agg('sum')):
        res.add_line(node, seconds_to_str(total))
    yield res


def slowness_cumulative(df: pandas.DataFrame) -> Table:
    net_slowest = (df['download'] > df['wait_for_pg']) & (df['download'] > df['local_io'])
    pg_slowest = (df['wait_for_pg'] > df['download']) & (df['wait_for_pg'] > df['local_io'])
    disk_slowest = (df['local_io'] > df['wait_for_pg']) & (df['local_io'] > df['download'])

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


def pool_pg_wait(df: pandas.DataFrame, pool_df: pandas.DataFrame) -> Table:
    bypool = df[['pool_name', 'duration', 'wait_for_pg']].groupby(['pool_name'])
    count = bypool.count()
    count_dct = dict(zip(count.index, count['duration'].values))
    total = bypool['duration'].agg('sum').to_dict()
    wait_pg = bypool['wait_for_pg'].agg('sum').to_dict()
    wait_pg_perc = {name: int(100 * wait / max(1, total[name])) for name, wait in wait_pg.items()}

    t = Table("Slowness by pool", ["Name", "Total ops", "Total slow", "Slow %", "Total slow time",
                                   "Total pg wait", "pg wait avg", "pg wait %"])
    for pool_name, wait_perc in sorted(wait_pg_perc.items(), key=lambda x: -x[1]):
        write_ops_arr = pool_df[pool_df['name'] == pool_name]['write_ops']
        read_ops_arr = pool_df[pool_df['name'] == pool_name]['read_ops']
        write_ops = write_ops_arr.values[-1] - write_ops_arr.values[0]
        read_ops = read_ops_arr.values[-1] - read_ops_arr.values[0]
        t.add_line(pool_name,
                   write_ops + read_ops,
                   count_dct[pool_name],
                   100 * count_dct[pool_name] // max(1, write_ops + read_ops),
                   seconds_to_str(total[pool_name]),
                   seconds_to_str(int(wait_pg[pool_name])),
                   int(wait_pg[pool_name] / max(1, count_dct[pool_name])),
                   wait_perc)

    return t

# ----------------------------------------------------------------------------------------------------------------------


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=ceph_ho_dumper.ALLOWED_LOG_LEVELS, help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    dbinfo_parsr = subparsers.add_parser('dbinfo', help="Show database info")
    dbinfo_parsr.add_argument("hdf5_file", help="HDF5 file to read data from")

    tohdf5_parser = subparsers.add_parser('tohdfs', help="Convert binary files to hds")
    tohdf5_parser.add_argument("target", help="HDF5 file path to store to")
    tohdf5_parser.add_argument("sources", nargs="+", help="record files to import")

    report_parser = subparsers.add_parser('report', help="Make a report on data from hdf5 file")
    report_parser.add_argument("--count", default=10, type=int, help="Slowest object(PG/OSD/node) count")
    report_parser.add_argument("--slow-osd", action="store_true", help="Slow osd report")
    report_parser.add_argument("--slow-node", action="store_true", help="Slow osd report")
    report_parser.add_argument("--slowness-source", action="store_true", help="Slow source for slow requests")
    report_parser.add_argument("--duration-distribution", action="store_true", help="Show all reports")
    report_parser.add_argument("--per-pool", action="store_true")

    plot_parser = subparsers.add_parser('plot', help="Make a report on data from hdf5 file")
    plot_parser.add_argument("--slowness-source", action="store_true", help="Slow source for slow requests")
    plot_parser.add_argument("--duration-distribution", action="store_true", help="Show all reports")

    for rparser in (plot_parser, report_parser):
        rparser.add_argument("--op", nargs='+', choices=['read', 'write_primary', 'write_secondary', 'write'],
                             default=['read', 'write'],
                             help="Select op type")
        rparser.add_argument("--min-time", type=int, default=0, help="Select min op time to considered slow")
        rparser.add_argument("--all", action="store_true", help="Slow source for slow requests")
        rparser.add_argument("hdf5_file", help="HDF5 file to read data from")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    ceph_ho_dumper.setup_logger(logger, opts.log_level, opts.log)

    if opts.subparser_name == 'tohdfs':
        convert_to_hdfs(opts.target, opts.sources)
        return 0

    if opts.subparser_name == 'dbinfo':
        info = load_hdf(opts.hdf5_file)
        df = info.data
        print("On-disk size ", os.stat(opts.hdf5_file).st_size // 1024 // 1024, "MiB")
        print(df.info(memory_usage='deep'))

        for dtype in ['uint32', 'uint16', 'uint8', 'category']:
            selected_dtype = df.select_dtypes(include=[dtype])
            mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
            mean_usage_mb = mean_usage_b / 1024 ** 2
            print(f"Average memory usage for {dtype} columns: {mean_usage_mb:03.2f} MiB")

        return 0

    if opts.subparser_name == 'report' or opts.subparser_name == 'plot':

        info = load_hdf(opts.hdf5_file)
        df = info.data
        if opts.min_time != 0:
            df = df[df['duration'] > opts.min_time]

        all_ops = set()
        for op in set(opts.op):
            if op == 'write':
                all_ops.add('write_primary')
                all_ops.add('write_secondary')
            else:
                all_ops.add(op)

        if all_ops != {'read', 'write_primary', 'write_secondary'}:
            selectors = [df['op_type'] == getattr(ceph_ho_dumper.OpType, name).value for name in all_ops]
            selector = selectors[0]
            for more_sel in selectors[1:]:
                selector |= more_sel
            df = df[selector]

        # primary_writes = df[df['op_type'] == ceph_ho_dumper.OpType.write_primary.value]
        # secondary_writes = df[df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value]
        # writes = df[(df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value) |
        #             (df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value)]
        # reads = df[df['op_type'] == ceph_ho_dumper.OpType.read.value]

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
                print(table2txt(slowness_cumulative(df)))
                print()

            if opts.all or opts.per_pool:
                print(table2txt(pool_pg_wait(df, info.pool_info)))
                print()

            if opts.all or opts.slow_osd:
                for table in top_slow_osds(df, opts.count):
                    print(table2txt(table))
                    print()

            if (opts.slow_node or opts.all) and 'host' in df:
                for table in top_slow_nodes(df, opts.count):
                    print(table2txt(table))
                    print()

        return 0

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
