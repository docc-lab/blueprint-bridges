#!/usr/bin/env python3
"""Tomislav-RetCtx (2026-11-09, spike investigation): the anatomy of every stall episode of an instrumented run, on one
millisecond timeline: HTTP requests at the load generator (node-0 capture), MongoDB operations of post-db and
urlshorten-db (their veth captures), and the 10 ms CFS samples of those two pods (spikediag_fastcg.py): CPU used,
periods throttled, throttled time.

For each episode (requests slower than --slow-ms, gaps < 1 s) it prints 50 ms bins from 1.5 s before the onset to
0.5 s after the last release: requests sent / slow, the slowest request sent in the bin, per database the number of
operations started and the slowest of them, CPU cores used and throttled ms in the bin. Then a run-wide test: for
every 50 ms bin with a database operation slower than 100 ms, was that database throttled in that bin or the one before?
usage: spikediag_anatomy.py ROOT [--slow-ms 150] [--bin-ms 50]"""
import argparse
import bisect
import collections
import gzip
import json
import pickle
from datetime import datetime
from pathlib import Path

import spikediag_analyze as s


def fastcg(path, uids):
    """uid -> sorted [(t, usage_usec, nr_periods, nr_throttled, throttled_usec)] for the wanted pods."""
    out = collections.defaultdict(list)
    with gzip.open(path, 'rt') as stream:
        for line in s.lines_until_eof(stream):
            if line.startswith('#'):
                continue
            p = line.split()
            if len(p) == 6 and p[1] in uids:
                out[p[1]].append((float(p[0]),) + tuple(int(x) for x in p[2:]))
    return out


_TIMES = {}


def at(series, t):
    """Cumulative counters at time t (last sample at or before t)."""
    times = _TIMES.get(id(series))
    if times is None:
        times = _TIMES[id(series)] = [x[0] for x in series]
    i = bisect.bisect_right(times, t) - 1
    return series[max(i, 0)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('root', type=Path)
    ap.add_argument('--slow-ms', type=float, default=150)
    ap.add_argument('--bin-ms', type=float, default=50)
    a = ap.parse_args()
    diag = a.root / 'diag'
    cache = diag / 'anatomy-cache.pkl'
    if cache.exists():
        pairs, mongo = pickle.load(open(cache, 'rb'))
    else:
        c0 = s.analyse_pcap(next(diag.glob('*-node-0.pcap')))
        pairs = sorted((t0, t1) for v in c0['http'].values() for t0, t1 in v)
        mongo = {}
        for db in ('post-db', 'urlshorten-db'):
            p = next(diag.glob(f'*-{db}.pcap'), None)
            if p:
                mongo[db] = s.analyse_mongo(p)['ops']
        pickle.dump((pairs, mongo), open(cache, 'wb'))
    pods = json.loads((diag / 'pods-all.json').read_text())['items']
    uid = {}
    for p in pods:
        for db in ('post-db', 'urlshorten-db'):
            if p['metadata']['name'].startswith(db + '-'):
                uid[db] = p['metadata']['uid']
    cg = {}
    for db, node in (('post-db', 5), ('urlshorten-db', 11)):
        f = diag / f'fastcg-node-{node}.txt.gz'
        if f.exists() and db in uid:
            cg[db] = fastcg(f, {uid[db]})[uid[db]]
    points = []
    for f in a.root.glob('run/*/*/result.json'):
        r = json.loads(f.read_text())
        if 'started' in r and (f.parent.name.startswith(('rate-', 'probe-'))):
            points.append((datetime.fromisoformat(r['started']).timestamp(), datetime.fromisoformat(r['finished']).timestamp(),
                           f.parent.name, r))
    points.sort()
    fmt = lambda t: datetime.utcfromtimestamp(t).strftime('%H:%M:%S.%f')[:-3]
    starts = [p[0] for p in pairs]
    mstarts = {db: [o[0] for o in ops] for db, ops in mongo.items()}
    print(f'== {a.root.name}: {len(pairs):,} HTTP requests; ' + ', '.join(f'{db} {len(o):,} ops' for db, o in mongo.items())
          + '; 10 ms CFS samples: ' + ', '.join(f'{db} {len(v):,}' for db, v in cg.items()))
    eps = [ep for ep in s.episodes([(t0, t1, None) for t0, t1 in pairs], a.slow_ms / 1e3)
           if any(p[0] - 1 <= ep[0][0] <= p[1] + 1 for p in points)]
    b = a.bin_ms / 1e3
    for k, ep in enumerate(eps, 1):
        t_on, t_end = ep[0][0], max(x[1] for x in ep)
        pt = next((p for p in points if p[0] - 1 <= t_on <= p[1] + 1), None)
        print(f'\n-- episode {k}: {pt[2] if pt else "?"} onset {fmt(t_on)}, {len(ep)} slow requests, max '
              f'{max(x[1] - x[0] for x in ep) * 1e3:.0f} ms, last release {fmt(t_end)}')
        print('   bin start     sent slow maxlat |' + ''.join(f' {db[:9]:>9s}: ops  max  cores thr_ms |' for db in mongo))
        t = t_on - 1.5
        while t < t_end + 0.5:
            lo, hi = bisect.bisect_left(starts, t), bisect.bisect_left(starts, t + b)
            lat = [pairs[i][1] - pairs[i][0] for i in range(lo, hi)]
            row = f'   {fmt(t)} {len(lat):4d} {sum(x > a.slow_ms / 1e3 for x in lat):4d} {max(lat, default=0) * 1e3:6.0f} |'
            for db, ops in mongo.items():
                mlo, mhi = bisect.bisect_left(mstarts[db], t), bisect.bisect_left(mstarts[db], t + b)
                mlat = [ops[i][1] - ops[i][0] for i in range(mlo, mhi)]
                cores = thr = 0.0
                if db in cg and cg[db]:
                    x0, x1 = at(cg[db], t), at(cg[db], t + b)
                    dt = max(x1[0] - x0[0], 1e-3)
                    cores, thr = (x1[1] - x0[1]) / 1e6 / dt, (x1[4] - x0[4]) / 1e3
                row += f' {"":>9s}  {len(mlat):4d} {max(mlat, default=0) * 1e3:5.0f} {cores:5.2f} {thr:6.1f} |'
            print(row)
            t += b
    # run-wide: slow database operations vs throttling of that database
    print('\n== run-wide: 50 ms bins (inside ramp points / probes) holding a database operation slower than 100 ms')
    for db, ops in mongo.items():
        if db not in cg:
            continue
        slow_bins = sorted({int(o[0] / b) for o in ops if o[1] - o[0] > 0.1 and any(p[0] <= o[0] <= p[1] for p in points)})
        thr_bins = 0
        for sb in slow_bins:
            x0, x1 = at(cg[db], (sb - 1) * b), at(cg[db], (sb + 1) * b)
            thr_bins += (x1[4] - x0[4]) > 0
        all_bins = set()
        for p in points:
            all_bins.update(range(int(p[0] / b), int(p[1] / b)))
        thr_any = 0
        for bb in all_bins:
            x0, x1 = at(cg[db], bb * b), at(cg[db], (bb + 1) * b)
            thr_any += (x1[4] - x0[4]) > 0
        print(f'   {db}: {len(slow_bins)} bins with an operation > 100 ms; throttled in that bin or the one before: '
              f'{thr_bins}; share of all measured bins with any throttling: {thr_any / max(1, len(all_bins)):.3%}')


if __name__ == '__main__':
    main()
