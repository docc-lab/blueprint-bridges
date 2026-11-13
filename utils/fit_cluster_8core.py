#!/usr/bin/env python3
"""Tomislav-RetCtx (2026-10-29): re-place a derived HotelReservation root for a cluster of 8-core / 12 GB nodes.

The September placement (prepare_dsb_hotel.PLACEMENT: a service of 8 cores beside its 8-core database
and 4-core cache on one node; ClickHouse 16 cores / 64 GiB on node-9) cannot schedule on the cluster
rebuilt on October 9 (15 nodes of 8 allocatable cores and 12 GB, ~0.7 core taken by kube-system,
0.5 by the collector agent on node-1..8).  This rewrites a root's manifests in place, identically for
every case, so roots compared against each other stay comparable:

  services        node-1..8 as before, 6500m each
  databases       3000m, two per node: geo+rate node-10, profile+recomd node-11, reserv+user node-12
  caches          reservation 4000m + profile 2000m on node-13, rate 4000m on node-14 (as September's 4 cores)
  ClickHouse      node-9, 4000m / 6Gi (the gateway's 2 cores / 2Gi and the shim stay there)

Tomislav-RetCtx (user 2026-11-08): each service's GOMAXPROCS follows its CPU limit (prepare_dsb_sn_nw.app_gomaxprocs:
ceil(limit), at least 2, the Go runtime's own container rule; 6500m -> 7) instead of prepare's 8; build-complete.json
records app_gomaxprocs = 'cpu-limit' and run_dsb_sn_nw.verify_deployment checks it.

Usage: fit_cluster_8core.py <root> [<root> ...]; records cluster-fit.json and a plan note, refreshes
build-complete.json's manifest hash (run_dsb_sn_nw.deploy verifies it).
"""
import hashlib
import json
import sys
from pathlib import Path

import yaml

from prepare_dsb_sn_e2e import environment
from prepare_dsb_sn_nw import GOMAXPROCS_RULE, app_gomaxprocs

SERVICE_CPU, MONGO_CPU = '6500m', '3000m'
# memcached CPU as in September (4 cores for the two hot caches; a 1-core reservation cache was the first
# attempt's bottleneck at ~2,500 req/s with the services at under 3 cores): reserv 4 + profile 2 on node-13,
# rate 4 alone on node-14.
CACHE_CPU = {'reserv-cache': '4000m', 'rate-cache': '4000m', 'profile-cache': '2000m'}
STORE_NODES = {'geo-db': 'node-10', 'rate-db': 'node-10', 'profile-db': 'node-11', 'recomd-db': 'node-11',
               'reserv-db': 'node-12', 'user-db': 'node-12',
               'rate-cache': 'node-14', 'profile-cache': 'node-13', 'reserv-cache': 'node-13'}
CLICKHOUSE = {'cpu': '4000m', 'memory': '6Gi'}


def fit(root):
    root = Path(root)
    report = {}
    for case in json.loads((root / 'cases.json').read_text()):
        directory = Path(case['case'])
        manifest = directory / 'manifest.yaml'
        documents = list(yaml.safe_load_all(manifest.read_text()))
        variant = case['variant']
        changes = {}
        for doc in documents:
            if not doc or doc.get('kind') != 'Deployment':
                continue
            name = doc['metadata']['name']
            pod = doc['spec']['template']['spec']
            container = pod['containers'][0]
            base = name.removesuffix('-' + variant + '-ctr')
            if '-service-' in name:
                cpu = SERVICE_CPU
            elif base in STORE_NODES:
                cpu = MONGO_CPU if base.endswith('-db') else CACHE_CPU[base]
                pod['nodeSelector'] = {'kubernetes.io/hostname': STORE_NODES[base]}
            elif base == 'clickhouse':
                container['resources'] = {key: dict(CLICKHOUSE) for key in ('requests', 'limits')}
                changes[name] = {'node': pod.get('nodeSelector', {}).get('kubernetes.io/hostname'), **CLICKHOUSE}
                continue
            else:
                continue
            for key in ('requests', 'limits'):
                container.setdefault('resources', {}).setdefault(key, {})['cpu'] = cpu
            changes[name] = {'node': pod.get('nodeSelector', {}).get('kubernetes.io/hostname'), 'cpu': cpu}
            if '-service-' in name:  # Tomislav-RetCtx (user 2026-11-08): GOMAXPROCS follows the CPU limit
                environment(container, {'GOMAXPROCS': app_gomaxprocs(cpu)})
                changes[name]['gomaxprocs'] = app_gomaxprocs(cpu)
        manifest.write_text(yaml.safe_dump_all(documents, sort_keys=False))
        complete_path = directory / 'build-complete.json'
        complete = json.loads(complete_path.read_text())
        complete['manifest_sha256'] = hashlib.sha256(manifest.read_bytes()).hexdigest()
        complete['cluster_fit'] = '8-core nodes (fit_cluster_8core.py)'
        complete['app_gomaxprocs'] = 'cpu-limit'
        complete_path.write_text(json.dumps(complete, indent=2) + '\n')
        report[case['name']] = changes
    plan_path = root / 'plan.json'
    plan = json.loads(plan_path.read_text())
    plan['cluster_fit'] = ('8-core / 12 GB nodes (cluster rebuilt 2026-10-09): services 6500m on node-1..8, databases 3000m '
                           'two per node on node-10..12, caches reserv 4/profile 2 cores on node-13 and rate 4 on node-14, ClickHouse 4000m/6Gi on node-9 '
                           '(utils/fit_cluster_8core.py); the September 8/8/4-core placement could not schedule')
    plan['app_cpus'] = 6.5
    plan['app_gomaxprocs'] = GOMAXPROCS_RULE
    plan_path.write_text(json.dumps(plan, indent=2) + '\n')
    (root / 'cluster-fit.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


if __name__ == '__main__':
    for root in sys.argv[1:]:
        report = fit(root)
        for case, changes in report.items():
            print(root, case, len(changes), 'deployments re-placed')
