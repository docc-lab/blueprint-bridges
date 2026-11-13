#!/usr/bin/env python3
"""Tomislav-RetCtx (2026-10-30): re-place a derived REAL-WORK Social Network root for the cluster of 8-thread / 12 GB
nodes (Dell R710, one Xeon E5530: 4 cores + HT), the Social Network counterpart of fit_cluster_8core.py.

Placement follows September's node pinning (examples/dsb_sn/node-pinning-nt_es.yaml: every cache and database beside
its service), sized for 8 allocatable threads per node (~0.7 taken by kube-system, 0.5 by the collector agent):

  node-1   composepost-service 6500m                        (the hot service: 7.7/8 cores at every September knee)
  node-2   hometimeline-service 3500m, hometimeline-cache 1500m, urlshorten-service 1500m
  node-3   usertimeline-service 3000m, usertimeline-cache 1500m, usermention-service 2000m
  node-4   socialgraph-service 1500m, social-cache 1000m, social-db 1500m, text-service 2500m
  node-5   post-storage-service 4000m, post-cache 1500m, post-db 1000m
  node-6   uniqueid-service 3000m, media-service 3000m
  node-7   wrk2api-service 6500m                            (HTTP entry; the frontend of this app)
  node-8   user-service 4000m, user-cache 1500m, user-db 1000m
  node-10  usertimeline-db 6500m                            (3.5 ms CPU per post: the hottest pod of the first fit)
  node-11  urlshorten-db 1500m, userid-service 3000m
  node-9   ClickHouse 4000m / 6Gi (the gateway's 2 cores / 2Gi and the shim stay there)

Tomislav-RetCtx (user 2026-11-08): each service's GOMAXPROCS follows its CPU limit (prepare_dsb_sn_nw.app_gomaxprocs:
ceil(limit), at least 2, the Go runtime's own container rule; 1500m -> 2, 6500m -> 7) instead of prepare's 8;
build-complete.json records app_gomaxprocs = 'cpu-limit' and run_dsb_sn_nw.verify_deployment checks it.

Usage: fit_cluster_8core_sn.py <root> [<root> ...]; records cluster-fit.json and a plan note, refreshes
build-complete.json's manifest hash (run_dsb_sn_nw.deploy verifies it).  Fails if a known deployment is missing or
an unknown service/cache/db deployment appears, so a naming change cannot silently leave a pod unplaced.
"""
import hashlib
import json
import sys
from pathlib import Path

import yaml

from prepare_dsb_sn_e2e import environment
from prepare_dsb_sn_nw import GOMAXPROCS_RULE, app_gomaxprocs

PLACEMENT = {
    # Tomislav-RetCtx (2026-10-30 17:55 UTC): the first fit capped the user-timeline MongoDB at 1000m and the no-tracing
    # ramp hit a wall at ~250 req/s with that pod pinned at 0.92 cores (3.5 ms CPU per ComposePost: DSB's user timeline is
    # a per-user document that every post rewrites). It now has a node of its own; text (1.1 ms/req) gets 3000m.
    'composepost-service': ('node-1', '6500m'),
    'hometimeline-service': ('node-2', '3500m'), 'hometimeline-cache': ('node-2', '1500m'), 'urlshorten-service': ('node-2', '1500m'),
    'usertimeline-service': ('node-3', '3000m'), 'usertimeline-cache': ('node-3', '1500m'), 'usermention-service': ('node-3', '2000m'),
    # Tomislav-RetCtx (2026-10-30 18:50 UTC): with social-db at 500m the social-graph seeding (37,624 follows, 200 in
    # parallel) returned 882 HTTP 500s (the first fit's 1000m seeded cleanly); the database gets 1500m again.
    'socialgraph-service': ('node-4', '1500m'), 'social-cache': ('node-4', '1000m'), 'social-db': ('node-4', '1500m'),
    'text-service': ('node-4', '2500m'),
    'post-storage-service': ('node-5', '4000m'), 'post-cache': ('node-5', '1500m'), 'post-db': ('node-5', '1000m'),
    'uniqueid-service': ('node-6', '3000m'), 'media-service': ('node-6', '3000m'),
    'wrk2api-service': ('node-7', '6500m'),
    'user-service': ('node-8', '4000m'), 'user-cache': ('node-8', '1500m'), 'user-db': ('node-8', '1000m'),
    'usertimeline-db': ('node-10', '6500m'),
    'urlshorten-db': ('node-11', '1500m'), 'userid-service': ('node-11', '3000m'),
}
CLICKHOUSE = {'cpu': '4000m', 'memory': '6Gi'}
IGNORED = ('clickhouse', 'otelgw', 'otelcol', 'jaeger', 'elasticsearch', 'jaeger-shim')


def fit(root):
    root = Path(root)
    report = {}
    for case in json.loads((root / 'cases.json').read_text()):
        directory = Path(case['case'])
        manifest = directory / 'manifest.yaml'
        documents = list(yaml.safe_load_all(manifest.read_text()))
        variant = case['variant']
        changes, seen = {}, set()
        for doc in documents:
            if not doc or doc.get('kind') != 'Deployment':
                continue
            name = doc['metadata']['name']
            pod = doc['spec']['template']['spec']
            container = pod['containers'][0]
            base = name.removesuffix('-' + variant + '-ctr')
            if base == 'clickhouse':
                container['resources'] = {key: dict(CLICKHOUSE) for key in ('requests', 'limits')}
                changes[name] = {'node': pod.get('nodeSelector', {}).get('kubernetes.io/hostname'), **CLICKHOUSE}
                continue
            if base in PLACEMENT:
                node, cpu = PLACEMENT[base]
                pod['nodeSelector'] = {'kubernetes.io/hostname': node}
                for key in ('requests', 'limits'):
                    container.setdefault('resources', {}).setdefault(key, {})['cpu'] = cpu
                changes[name] = {'node': node, 'cpu': cpu}
                if '-service-' in name:  # Tomislav-RetCtx (user 2026-11-08): GOMAXPROCS follows the CPU limit
                    environment(container, {'GOMAXPROCS': app_gomaxprocs(cpu)})
                    changes[name]['gomaxprocs'] = app_gomaxprocs(cpu)
                seen.add(base)
            elif any(base.startswith(p) for p in IGNORED):
                continue
            else:
                raise SystemExit(f'{case["name"]}: unplaced deployment {name} (base {base})')
        # Tomislav-RetCtx (2026-11-06): the upstream-matching snbounded app has no userid service.
        optional = {'userid-service'} if case.get('app') == 'snbd' else set()
        missing = set(PLACEMENT) - seen - optional
        assert not missing, f'{case["name"]}: deployments not found: {sorted(missing)}'
        manifest.write_text(yaml.safe_dump_all(documents, sort_keys=False))
        complete_path = directory / 'build-complete.json'
        complete = json.loads(complete_path.read_text())
        complete['manifest_sha256'] = hashlib.sha256(manifest.read_bytes()).hexdigest()
        complete['cluster_fit'] = '8-core nodes (fit_cluster_8core_sn.py)'
        complete['app_gomaxprocs'] = 'cpu-limit'
        complete_path.write_text(json.dumps(complete, indent=2) + '\n')
        report[case['name']] = changes
    plan_path = root / 'plan.json'
    plan = json.loads(plan_path.read_text())
    plan['cluster_fit'] = ('8-thread / 12 GB nodes (cluster rebuilt 2026-10-09): September node pinning (each cache/db beside its '
                           'service) sized for 8 threads: composepost 6500m alone on node-1, wrk2api 6500m alone on node-7, the '
                           'user-timeline MongoDB 6500m alone on node-10, the other services 1500-4000m with their caches and databases, ClickHouse 4000m/6Gi on node-9 '
                           '(utils/fit_cluster_8core_sn.py)')
    plan['app_cpus'] = 6.5
    plan['app_gomaxprocs'] = GOMAXPROCS_RULE
    plan_path.write_text(json.dumps(plan, indent=2) + '\n')
    (root / 'cluster-fit.json').write_text(json.dumps(report, indent=2) + '\n')
    print(root, 'fitted:', {k: len(v) for k, v in report.items()})


if __name__ == '__main__':
    for root in sys.argv[1:]:
        fit(root)
