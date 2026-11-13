#!/usr/bin/env python3
"""Tomislav-RetCtx (2026-11-09, spike investigation): fine-grained CFS sampler, run ON a node. Every INTERVAL seconds
(default 0.01) it reads cpu.stat of every pod cgroup under /sys/fs/cgroup/kubepods.slice and appends one binary-free
text line per pod whose counters changed:  <unix time> <pod uid> <usage_usec> <nr_periods> <nr_throttled> <throttled_usec>
to /var/tmp/spikediag-fastcg.txt, until /var/tmp/spikediag.stop exists or DURATION seconds pass. New pods are picked
up every second (the deployment starts after the sampler).
usage: spikediag_fastcg.py [DURATION_S] [INTERVAL_S]"""
import glob
import os
import sys
import time

DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 7200
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 0.01
OUT, STOP = '/var/tmp/spikediag-fastcg.txt', '/var/tmp/spikediag.stop'  # not /dev/shm (logind RemoveIPC)
PATTERNS = ['/sys/fs/cgroup/kubepods.slice/kubepods-pod*.slice/cpu.stat',
            '/sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod*.slice/cpu.stat']


def pods():
    out = {}
    for pattern in PATTERNS:
        for path in glob.glob(pattern):
            uid = path.split('pod')[-1].split('.slice')[0].replace('_', '-')
            out[uid] = path
    return out


def main():
    end = time.time() + DURATION
    table, last, rescan = pods(), {}, 0.0
    with open(OUT, 'w', buffering=1 << 16) as out:
        out.write(f'# {os.uname().nodename} interval {INTERVAL} started {time.time():.6f}\n')
        while time.time() < end and not os.path.exists(STOP):
            now = time.time()
            if now >= rescan:
                table, rescan = pods(), now + 1.0
            for uid, path in table.items():
                try:
                    with open(path) as f:
                        v = {k: int(x) for k, x in (line.split() for line in f)}
                except (OSError, ValueError):
                    continue
                row = (v.get('usage_usec', 0), v.get('nr_periods', 0), v.get('nr_throttled', 0), v.get('throttled_usec', 0))
                if last.get(uid) != row:
                    out.write(f'{now:.6f} {uid} {row[0]} {row[1]} {row[2]} {row[3]}\n')
                    last[uid] = row
            time.sleep(max(0.0, INTERVAL - (time.time() - now)))
        out.write(f'# stopped {time.time():.6f}\n')


if __name__ == '__main__':
    main()
