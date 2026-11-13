#!/usr/bin/env python3
"""Tomislav-RetCtx (user 2026-11-08: "I would like to be able to output logs of gc activity"): the Go garbage
collections of every application service of a run, from what the runner already saved.

The services run with GODEBUG=gctrace=1 (derive --app-gctrace), so the Go runtime writes one line per collection to
stderr:

    gc 34 @997.803s 0%: 0.15+4.9+0.10 ms clock, 1.2+9.3/6.2/0+0.85 ms cpu, 901->902->6 MB, 903 MB goal, 0 MB stacks,
    0 MB globals, 8 P

(collection number, seconds since the process started, % of CPU spent in GC since then; wall-clock ms of the
stop-the-world sweep termination + concurrent mark + stop-the-world mark termination; CPU ms of the same phases, the
mark split into assist / background / idle; heap MB at the start of the cycle -> at its end -> live after it; the next
goal; GOMAXPROCS; "(forced)" for a runtime.GC call).

Every ramp point saves a before/after snapshot holding the last RETCTX_LOG_TAIL (2000) lines of each service's log
(run_dsb_sn_e2e.snapshot), so the snapshots together cover the run although each one is truncated; the lines are
merged here and de-duplicated by (pod, collection number).  A line's wall-clock time is the container's start time
(ready-pods.json) plus its "@" seconds.  Gaps (collection numbers no snapshot kept) are reported.

Writes, per case (<root>/run/<case>):
  <case>/gc-events.csv   one row per collection: time, service, pod, n, uptime, STW and mark ms, CPU ms, heap MB
  <case>/gc-points.csv   one row per ramp point and service: collections, STW ms total / max, GC CPU ms in the point
and prints a per-service coverage line and the points where any service collected.

usage: gc_logs.py ROOT [ROOT ...] [--case NAME]
"""
import argparse
import csv
import gzip
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

GC_LINE = re.compile(
    r'^gc (?P<n>\d+) @(?P<up>[\d.]+)s (?P<pct>\d+)%: (?P<c1>[\d.]+)\+(?P<c2>[\d.]+)\+(?P<c3>[\d.]+) ms clock, '
    r'(?P<u1>[\d.]+)\+(?P<ua>[\d.]+)/(?P<ub>[\d.]+)/(?P<ui>[\d.]+)\+(?P<u3>[\d.]+) ms cpu, '
    r'(?P<h0>\d+)->(?P<h1>\d+)->(?P<h2>\d+) MB, (?P<goal>\d+) MB goal, (?:(?P<stacks>\d+) MB stacks, )?'
    r'(?:(?P<globals>\d+) MB globals, )?(?P<procs>\d+) P(?P<forced> \(forced\))?')


def parse_time(text):
    return datetime.fromisoformat(text.replace('Z', '+00:00'))


def container_starts(case_dir):
    """pod name -> start time of its (first) container, from the readiness record the runner saved."""
    data = json.loads((case_dir / 'ready-pods.json').read_text())
    starts = {}
    for pod in data['items'] if isinstance(data, dict) else data:
        for status in pod.get('status', {}).get('containerStatuses', []):
            running = status.get('state', {}).get('running', {})
            if running.get('startedAt'):
                starts.setdefault(pod['metadata']['name'], parse_time(running['startedAt']))
    return starts


def service_of(pod):
    """frontend-service-hotel-v-esx...-ctr-748c6cbfb-lmvmh -> frontend-service."""
    m = re.match(r'(.+?-service)-', pod)
    return m.group(1) if m else pod


def collect(case_dir):
    starts = container_starts(case_dir)
    events = {}
    for path in case_dir.rglob('logs-*.txt.gz'):
        pod = path.name[len('logs-'):-len('.txt.gz')]
        if '-service-' not in pod or pod not in starts:
            continue
        with gzip.open(path, 'rt', errors='replace') as stream:
            for line in stream:
                m = GC_LINE.match(line)
                if not m:
                    continue
                key = (pod, int(m['n']))
                if key in events:
                    continue
                g = {k: float(v) for k, v in m.groupdict().items() if v is not None and k != 'forced'}
                events[key] = {
                    'time': starts[pod] + timedelta(seconds=g['up']),
                    'service': service_of(pod), 'pod': pod, 'n': int(g['n']), 'uptime_s': g['up'],
                    'gc_cpu_pct_since_start': int(g['pct']),
                    'stw_ms': round(g['c1'] + g['c3'], 3), 'mark_ms': g['c2'],
                    'cpu_ms': round(g['u1'] + g['ua'] + g['ub'] + g['ui'] + g['u3'], 3), 'assist_cpu_ms': g['ua'],
                    'heap_start_mb': int(g['h0']), 'heap_end_mb': int(g['h1']), 'live_mb': int(g['h2']),
                    'goal_mb': int(g['goal']), 'procs': int(g['procs']), 'forced': bool(m['forced']),
                }
    return sorted(events.values(), key=lambda e: e['time'])


def points(case_dir):
    out = []
    for path in sorted(case_dir.glob('rate-*/result.json')):
        r = json.loads(path.read_text())
        if 'started' in r and 'finished' in r:
            out.append((r['offered_rps'], parse_time(r['started']), parse_time(r['finished']), r))
    return out


def analyse(case_dir):
    events = collect(case_dir)
    if not events:
        print(f'{case_dir}: no gctrace lines (GODEBUG=gctrace=1 not set?)')
        return
    fields = ['time', 'service', 'pod', 'n', 'uptime_s', 'gc_cpu_pct_since_start', 'stw_ms', 'mark_ms', 'cpu_ms',
              'assist_cpu_ms', 'heap_start_mb', 'heap_end_mb', 'live_mb', 'goal_mb', 'procs', 'forced']
    with open(case_dir / 'gc-events.csv', 'w', newline='') as stream:
        w = csv.DictWriter(stream, fieldnames=fields)
        w.writeheader()
        for e in events:
            w.writerow({**e, 'time': e['time'].isoformat(timespec='milliseconds')})
    print(f'== {case_dir}: {len(events)} collections -> gc-events.csv')
    for pod in sorted({e['pod'] for e in events}):
        ns = sorted(e['n'] for e in events if e['pod'] == pod)
        missing = (ns[-1] - ns[0] + 1) - len(ns)
        print(f'   {service_of(pod):24s} gc {ns[0]}..{ns[-1]}, {len(ns)} kept, {missing} missing'
              + ('' if ns[0] == 1 else f' (1..{ns[0] - 1} before the first snapshot kept)'))
    rows = []
    for offered, t0, t1, r in points(case_dir):
        inside = [e for e in events if t0 <= e['time'] <= t1]
        for service in sorted({e['service'] for e in inside}):
            s = [e for e in inside if e['service'] == service]
            rows.append({'offered_rps': offered, 'service': service, 'collections': len(s),
                         'stw_ms_total': round(sum(e['stw_ms'] for e in s), 3),
                         'stw_ms_max': round(max(e['stw_ms'] for e in s), 3),
                         'mark_ms_max': round(max(e['mark_ms'] for e in s), 3),
                         'gc_cpu_ms_total': round(sum(e['cpu_ms'] for e in s), 3),
                         'p99_ms': r.get('p99_ms'), 'mean_ms': r.get('mean_ms')})
    with open(case_dir / 'gc-points.csv', 'w', newline='') as stream:
        w = csv.DictWriter(stream, fieldnames=['offered_rps', 'service', 'collections', 'stw_ms_total', 'stw_ms_max',
                                               'mark_ms_max', 'gc_cpu_ms_total', 'p99_ms', 'mean_ms'])
        w.writeheader()
        w.writerows(rows)
    print(f'   {len(rows)} (point, service) rows -> gc-points.csv')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('roots', nargs='+', type=Path)
    ap.add_argument('--case', help='only this case directory name (e.g. 01-v)')
    a = ap.parse_args()
    for root in a.roots:
        for case_dir in sorted((root / 'run').glob('*')):
            if case_dir.is_dir() and (not a.case or case_dir.name == a.case):
                analyse(case_dir)


if __name__ == '__main__':
    main()
