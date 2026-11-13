#!/usr/bin/env python3
"""Tomislav-RetCtx (user 2026-11-09: "You need to figure out what is causing this"): INTERVENTION for the latency-spike
investigation. Removes the CPU LIMIT (the CFS quota) from the named deployments of a derived, fitted root and keeps
their CPU request, so placement and scheduling stay exactly as fit_cluster_8core_sn.py made them; only the quota that
throttles the container goes. Refreshes build-complete.json's manifest hash (run_dsb_sn_nw.deploy verifies it) and
records the change in plan.json, build-complete.json and unlimit.json.

usage: spikediag_unlimit.py ROOT BASE [BASE ...]      e.g. spikediag_unlimit.py <root> post-db urlshorten-db
(BASE = deployment name without the -<variant>-ctr suffix)"""
import hashlib
import json
import sys
from pathlib import Path

import yaml


def unlimit(root, bases):
    root = Path(root)
    report = {}
    for case in json.loads((root / 'cases.json').read_text()):
        directory = Path(case['case'])
        manifest = directory / 'manifest.yaml'
        documents = list(yaml.safe_load_all(manifest.read_text()))
        done = {}
        for doc in documents:
            if not doc or doc.get('kind') != 'Deployment':
                continue
            base = doc['metadata']['name'].removesuffix('-' + case['variant'] + '-ctr')
            if base in bases:
                container = doc['spec']['template']['spec']['containers'][0]
                limits = container.get('resources', {}).get('limits', {})
                assert 'cpu' in limits, (base, container.get('resources'))
                done[base] = {'removed_limit': limits.pop('cpu'), 'request_kept': container['resources']['requests']['cpu']}
                if not limits:
                    container['resources'].pop('limits')
        assert set(done) == set(bases), (case['name'], sorted(set(bases) - set(done)))
        manifest.write_text(yaml.safe_dump_all(documents, sort_keys=False))
        complete_path = directory / 'build-complete.json'
        complete = json.loads(complete_path.read_text())
        complete['manifest_sha256'] = hashlib.sha256(manifest.read_bytes()).hexdigest()
        complete['cpu_limit_removed'] = done
        complete_path.write_text(json.dumps(complete, indent=2) + '\n')
        report[case['name']] = done
    plan_path = root / 'plan.json'
    plan = json.loads(plan_path.read_text())
    plan['cpu_limit_removed'] = {'deployments': sorted(bases), 'why': 'spike investigation intervention (2026-11-09): CFS '
                                 'throttling of these MongoDB pods coincided with every large pre-knee stall',
                                 'tool': 'utils/spikediag_unlimit.py'}
    plan_path.write_text(json.dumps(plan, indent=2) + '\n')
    (root / 'unlimit.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


if __name__ == '__main__':
    print(json.dumps(unlimit(sys.argv[1], sys.argv[2:]), indent=1))
