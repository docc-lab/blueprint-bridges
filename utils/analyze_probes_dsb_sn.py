#!/usr/bin/env python3
"""Tomislav-RetCtx (user 2026-11-05): CPU per request at the fixed-rate probe points of a ramp root
(run_dsb_sn_nw.probe: <case>/probe-pre and <case>/probe-post), from the kubelet /stats/summary snapshots the runner
stores in <point>/before|after/node-node-N.txt.gz. Reports, per case: completed rate and p50/p99 of each probe, CPU
milliseconds per request summed over application services, databases and caches, tracing agents, the gateway and the
trace store, the per-service breakdown, and the pre -> post drift (the steady-state check). With --trend it also lists
every ramp point (rate-*): completed rate, application and user-timeline-database CPU per request, inbound bytes per
request on the user-timeline-service node, and the user-timeline document size from app-state.json, so a drift across
the ramp is visible point by point.

usage: analyze_probes_dsb_sn.py ROOT [ROOT ...] [--trend] [--json OUT]
"""
import argparse
import glob
import gzip
import json
import os
from datetime import datetime
from pathlib import Path


def _ts(text):
    return datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp()


def load_snapshot(directory):
    pods, nodes = {}, {}
    for path in glob.glob(os.path.join(directory, 'node-node-*.txt.gz')):
        try:
            summary = json.load(gzip.open(path))
        except Exception:  # noqa: BLE001 - a node whose snapshot failed is left out
            continue
        node = summary['node']
        interfaces = {i['name']: (i.get('rxBytes', 0), i.get('txBytes', 0))
                      for i in node.get('network', {}).get('interfaces', [])}
        nodes[node['nodeName']] = (_ts(node['cpu']['time']), node['cpu']['usageCoreNanoSeconds'], interfaces)
        for pod in summary.get('pods', []):
            cpu = pod.get('cpu', {})
            if 'usageCoreNanoSeconds' in cpu:
                pods[pod['podRef']['name']] = (_ts(cpu['time']), cpu['usageCoreNanoSeconds'], node['nodeName'])
    return pods, nodes


def base_of(pod, variant):
    marker = '-' + variant + '-ctr'
    return pod.split(marker)[0] if marker in pod else pod.rsplit('-', 2)[0]


def group_of(base):
    if base.endswith('-service'):
        return 'application'
    if base.endswith('-db') or base.endswith('-cache'):
        return 'databases+caches'
    if base == 'otelcol':
        return 'agents'
    if base == 'otelgw':
        return 'gateway'
    if base.startswith(('clickhouse', 'jaeger', 'elasticsearch', 'shim')):
        return 'trace store'
    # Tomislav-RetCtx: calico-node, kube-proxy, metallb speaker, coredns, the registry, ...: cluster plumbing whose
    # mostly fixed CPU is not a cost of the application or of tracing; kept apart so it does not pollute either.
    return 'cluster system'


def point_costs(point, variant):
    """CPU ms per completed request per service base and per group, plus node inbound bytes per request."""
    result = json.loads((point / 'result.json').read_text())
    rps = result['completed_rps']
    before_pods, before_nodes = load_snapshot(point / 'before')
    after_pods, after_nodes = load_snapshot(point / 'after')
    per_base, nodes_of = {}, {}
    for name, (t1, c1, node) in after_pods.items():
        if name not in before_pods:
            continue
        t0, c0, _ = before_pods[name]
        if t1 <= t0:
            continue
        base = base_of(name, variant)
        per_base[base] = per_base.get(base, 0.0) + (c1 - c0) / 1e9 / (t1 - t0) * 1000 / rps
        nodes_of[base] = node
    groups = {}
    for base, ms in per_base.items():
        groups[group_of(base)] = groups.get(group_of(base), 0.0) + ms
    inbound = {}
    for node, (t1, _, net1) in after_nodes.items():
        if node in before_nodes and 'eno2' in net1 and 'eno2' in before_nodes[node][2] and t1 > before_nodes[node][0]:
            t0, _, net0 = before_nodes[node]
            inbound[node] = (net1['eno2'][0] - net0['eno2'][0]) / (t1 - t0) / rps
    state = point / 'app-state.json'
    return {'offered_rps': result.get('offered_rps'), 'completed_rps': rps,
            'p50_ms': result.get('p50_ms', result.get('latency_p50_ms')),
            'p99_ms': result.get('p99_ms', result.get('latency_p99_ms')),
            'errors': result.get('non_2xx_3xx'), 'ms_per_request': per_base, 'groups': groups,
            'node_of': nodes_of, 'eno2_rx_bytes_per_request': inbound,
            'app_state': json.loads(state.read_text()) if state.exists() else None}


def drift(pre, post):
    return None if not pre else 100.0 * (post - pre) / pre


def analyze(root, trend):
    cases = {c['name']: c for c in json.loads((root / 'cases.json').read_text())}
    out = {}
    for case_dir in sorted((root / 'run').glob('*')):
        name = case_dir.name.split('-', 1)[1]
        if name not in cases or '-interrupted-' in case_dir.name:
            continue
        variant = cases[name]['variant']
        entry = {'kind': cases[name]['kind']}
        for tag in ('pre', 'post'):
            point = case_dir / f'probe-{tag}'
            if (point / 'result.json').exists():
                entry[tag] = point_costs(point, variant)
        if trend:
            entry['trend'] = [point_costs(p.parent, variant) for p in sorted(case_dir.glob('rate-*/result.json'))]
        out[case_dir.name] = entry
    return out


def show(root, report, trend):
    print(f'== {root}')
    for case, entry in report.items():
        print(f'-- {case} ({entry["kind"]})')
        pre, post = entry.get('pre'), entry.get('post')
        for tag, p in (('pre', pre), ('post', post)):
            if p:
                print(f'   probe-{tag}: {p["completed_rps"]:.0f} req/s completed, p50 {p["p50_ms"]} ms, p99 {p["p99_ms"]} ms, '
                      f'errors {p["errors"]}')
        if pre:
            print(f'   {"CPU ms per request":34s} {"pre":>8s} {"post":>8s} {"drift":>8s}')
            for group in ('application', 'databases+caches', 'agents', 'gateway', 'trace store', 'cluster system'):
                a = pre['groups'].get(group, 0.0)
                b = (post or {}).get('groups', {}).get(group)
                d = drift(a, b) if b is not None else None
                print(f'   {group:34s} {a:8.3f} {"" if b is None else f"{b:8.3f}"} {"" if d is None else f"{d:+7.1f}%"}')
            for base, a in sorted(pre['ms_per_request'].items(), key=lambda kv: -kv[1]):
                if a < 0.02:
                    continue
                b = (post or {}).get('ms_per_request', {}).get(base)
                d = drift(a, b) if b is not None else None
                print(f'     {base:32s} {a:8.3f} {"" if b is None else f"{b:8.3f}"} {"" if d is None else f"{d:+7.1f}%"}')
        if trend and entry.get('trend'):
            ut_node = (pre or entry['trend'][0])['node_of'].get('usertimeline-service')
            print(f'   trend: {"offered":>7s} {"done":>7s} {"app ms":>7s} {"ut-db ms":>8s} {"ut-svc ms":>9s} '
                  f'{"rxB/req " + str(ut_node):>16s} {"ut doc B":>8s} {"posts/doc":>9s}')
            for p in entry['trend']:
                state = p['app_state'] or {}
                ut = state.get('usertimeline') or {}
                per_doc = ut.get('posts_per_doc') or {}
                print(f'          {p["offered_rps"]:7d} {p["completed_rps"]:7.0f} {p["groups"].get("application", 0):7.3f} '
                      f'{p["ms_per_request"].get("usertimeline-db", 0):8.3f} {p["ms_per_request"].get("usertimeline-service", 0):9.3f} '
                      f'{p["eno2_rx_bytes_per_request"].get(ut_node, 0):16.0f} {ut.get("avg_obj_size", 0):8.0f} '
                      f'{per_doc.get("mean", 0):9.1f}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('roots', nargs='+', type=Path)
    parser.add_argument('--trend', action='store_true', help='also list every ramp point')
    parser.add_argument('--json', type=Path, default=None, help='write the full report as JSON')
    args = parser.parse_args()
    full = {}
    for root in args.roots:
        report = analyze(root.resolve(), args.trend)
        full[str(root)] = report
        show(root, report, args.trend)
    if args.json:
        args.json.write_text(json.dumps(full, indent=1, default=str))
