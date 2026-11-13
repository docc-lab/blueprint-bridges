#!/usr/bin/env python3
"""Tomislav-RetCtx (2026-11-09, spike investigation): attribute every stall episode of an instrumented run to a
mechanism, from the evidence the run recorded.

Episode = requests slower than --slow-ms at the load generator (node-0 capture), gaps < 1 s. Window = 0.6 s before the
onset to the last release. Mechanisms, checked in this order (an episode can show several; the first wins):
  NIC-DROP     node-3's NIC dropped packets (rx_missed) and usertimeline-db had a retransmission timeout or SYN
               retransmit in the window (the per-user timeline that findAndModify returns overflows node-3's RX ring)
  RTO          any application pod had a retransmission timeout or SYN retransmit (per-pod netns counters)
  DB-THROTTLE  post-db or urlshorten-db was CFS-throttled within 0.1 s before .. 0.4 s after the onset (10 ms samples)
  NIC-ONLY     node-3 dropped packets but no timeout was recorded (fast retransmits only)
  NONE         nothing recorded
Prints one line per episode and a summary for the points below --knee req/s.
usage: spikediag_classify.py ROOT [--slow-ms 150] [--knee 2000]"""
import argparse
import bisect
import collections
import gzip
import json
import pickle
from datetime import datetime
from pathlib import Path

import spikediag_analyze as s
import spikediag_anatomy as an

APP = ('composepost', 'text-service', 'urlshorten', 'usermention', 'wrk2api', 'post-storage', 'usertimeline',
       'hometimeline', 'socialgraph', 'user-service', 'media', 'uniqueid', '-db-', '-cache-')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('root', type=Path)
    ap.add_argument('--slow-ms', type=float, default=150)
    ap.add_argument('--knee', type=int, default=2000)
    a = ap.parse_args()
    diag = a.root / 'diag'
    cache = diag / 'anatomy-cache.pkl'
    if cache.exists():
        pairs, mongo = pickle.load(open(cache, 'rb'))
    else:
        c0 = s.analyse_pcap(next(diag.glob('*-node-0.pcap')))
        pairs = sorted((t0, t1) for v in c0['http'].values() for t0, t1 in v)
        # Tomislav-RetCtx (2026-11-11): a fresh-points run has one capture per database pod (one pod per point)
        mongo = {db: sorted(op for p in sorted(diag.glob(f'*-{db}*.pcap')) for op in s.analyse_mongo(p)['ops'])
                 for db in ('post-db', 'urlshorten-db')}
        pickle.dump((pairs, mongo), open(cache, 'wb'))
    # Tomislav-RetCtx (2026-11-11): every pod the run had: pods-all.json (taken at the end) plus the pod snapshots a
    # fresh-points run takes whenever its pods change (pods-*.json.gz), so the pods of earlier points keep their names
    pods = json.loads((diag / 'pods-all.json').read_text())['items']
    for f in sorted(diag.glob('pods-2*.json.gz')):
        pods += json.loads(gzip.open(f, 'rt').read())['items']
    name = {p['metadata']['uid']: p['metadata']['name'].split('-nt-')[0] for p in pods}
    uids = {db: {p['metadata']['uid'] for p in pods if p['metadata']['name'].startswith(db + '-')}
            for db in ('post-db', 'urlshorten-db')}
    cg = {}
    for db, node in (('post-db', 5), ('urlshorten-db', 11)):
        f = diag / f'fastcg-node-{node}.txt.gz'
        if f.exists() and uids[db]:
            cg[db] = [v for v in an.fastcg(f, uids[db]).values() if v]
    samp = {f.name[8:-7]: s.deltas(s.sampler(f)) for f in diag.glob('sampler-node-*.txt.gz')}
    has_podtcp = any(d.get('podtcp') for ds in samp.values() for d in ds)
    points = []
    for f in a.root.glob('run/*/*/result.json'):
        if f.parent.name.startswith(('rate-', 'probe-')):
            r = json.loads(f.read_text())
            points.append((datetime.fromisoformat(r['started']).timestamp(), datetime.fromisoformat(r['finished']).timestamp(),
                           f.parent.name, r['offered_rps']))
    eps = s.episodes([(t0, t1, None) for t0, t1 in pairs], a.slow_ms / 1e3)
    rows = []
    for ep in eps:
        t_on, t_end = ep[0][0], max(x[1] for x in ep)
        pt = next((p for p in points if p[0] <= t_on <= p[1]), None)
        if not pt:
            continue
        w0, w1 = t_on - 0.6, t_end
        ev = collections.defaultdict(collections.Counter)
        missed3 = 0
        for node, ds in samp.items():
            for d in ds:
                # Tomislav-RetCtx: bnx2 refreshes its hardware counters about once a second (stats_ticks), so node-3's
                # rx_missed shows a drop up to ~1 s late: its window reaches 1.2 s past the last release.
                if d['t1'] < w0 or d['t0'] > w1 + (1.2 if node == 'node-3' else 0):
                    continue
                if node == 'node-3' and d['t0'] <= w1 + 1.2:
                    missed3 += d.get('nic_rx_missed', 0)
                if d['t0'] > w1:
                    continue
                for u, c in d.get('podtcp', {}).items():
                    nm = name.get(u, '')
                    if any(k in nm for k in APP):
                        ev[nm].update(c)
        rto = {nm: c['rto'] + c['syn_retrans'] for nm, c in ev.items() if c['rto'] or c['syn_retrans']}
        thr = {}
        for db, many in cg.items():
            # Tomislav-RetCtx: summed over the database's pods (one per point in a fresh-points run; at() of a pod that
            # does not exist in the window gives no change)
            d = sum(an.at(series, t_on + 0.4)[4] - an.at(series, t_on - 0.1)[4] for series in many)
            if d > 0:
                thr[db] = round(d / 1e3)
        if missed3 and 'usertimeline-db' in rto:
            kind = 'NIC-DROP'
        elif rto:
            kind = 'RTO'
        elif thr:
            kind = 'DB-THROTTLE'
        elif missed3:
            kind = 'NIC-ONLY'
        else:
            kind = 'NONE'
        rows.append({'onset': s.fmt_t(t_on), 'point': pt[2], 'offered': pt[3], 'slow': len(ep),
                     'max_ms': round(max(x[1] - x[0] for x in ep) * 1e3), 'kind': kind, 'rto': rto, 'throttled_ms': thr,
                     'node3_missed': missed3})
    print(f'== {a.root.name}: {len(rows)} episodes (> {a.slow_ms:.0f} ms); per-pod TCP counters: {"yes" if has_podtcp else "NO"}; '
          f'10 ms CFS: {", ".join(cg) or "none"}')
    for r in rows:
        print(f"{r['onset']} {r['point']:11s} slow {r['slow']:5d} max {r['max_ms']:5d} ms  {r['kind']:11s} "
              f"node-3 drops {r['node3_missed']:5d}  timeouts {r['rto'] or '-'}  db throttled {r['throttled_ms'] or '-'}")
    pre = [r for r in rows if r['offered'] < a.knee]
    by = collections.defaultdict(lambda: [0, 0, 0])
    for r in pre:
        by[r['kind']][0] += 1
        by[r['kind']][1] += r['slow']
        by[r['kind']][2] = max(by[r['kind']][2], r['max_ms'])
    print(f'-- below {a.knee} req/s: ' + '; '.join(f'{k}: {v[0]} episodes, {v[1]} slow requests, worst {v[2]} ms'
                                                   for k, v in sorted(by.items(), key=lambda kv: -kv[1][1])))
    (diag / 'episode-classes.json').write_text(json.dumps(rows, indent=1))


if __name__ == '__main__':
    main()
