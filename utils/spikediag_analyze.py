#!/usr/bin/env python3
"""Tomislav-RetCtx (user 2026-11-09: "You need to figure out what is causing this. I want a detailed report.
Unexplained spikes are utterly unacceptable"): analysis of an instrumented run of spikediag_chain.sh.

Inputs (<root>/diag): node-0.pcap (load generator side: wrk2 <-> wrk2api, port 2000), node-7.pcap (wrk2api side:
port 2000 in, gRPC 12345 out), sampler-node-N.txt.gz (spikediag_sampler.sh), pods-all.json; plus the runner's
rate-*/result.json.

1. HTTP request/response pairs from each capture (HTTP/1.1, one outstanding request per wrk2 connection): request =
   first new client data segment while the connection is idle, response = first new server data segment after it.
2. STALLS: requests whose latency at node-0 exceeds --slow-ms, grouped into episodes (gaps < 1 s). For each episode:
   onset (first slow request sent), release (when their responses arrive: if they all wait on one thing, they are
   released together), the node-7 view of the same requests (same 4-tuple), gRPC streams wrk2api -> composepost in the
   window (HTTP/2 HEADERS frames by stream id, first frame of each segment), TCP retransmissions / zero windows / SYNs
   in either capture, and every node's kernel counters (sampler deltas) in the window against that node's baseline.
usage: spikediag_analyze.py ROOT [--slow-ms 150] [--json OUT]
"""
import argparse
import bisect
import collections
import glob
import gzip
import json
import mmap
import socket
import struct
import sys
from datetime import datetime
from pathlib import Path

# ----------------------------------------------------------------------------------------------------------- pcap
def packets(path):
    """(ts, src, sport, dst, dport, seq, ack, flags, window, plen, head) for every IPv4/TCP packet of an EN10MB pcap."""
    with open(path, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    magic = struct.unpack_from('<I', mm, 0)[0]
    if magic in (0xa1b2c3d4, 0xa1b23c4d):
        e = '<'
    else:
        e = '>'
        magic = struct.unpack_from('>I', mm, 0)[0]
    div = 1e9 if magic == 0xa1b23c4d else 1e6
    assert struct.unpack_from(e + 'I', mm, 20)[0] == 1, 'expected Ethernet link type'
    off, n = 24, len(mm)
    rec = struct.Struct(e + 'IIII')
    while off + 16 <= n:
        s, frac, incl, _ = rec.unpack_from(mm, off)
        off += 16
        p = off
        off += incl
        if off > n:
            break
        if incl < 54 or mm[p + 12] != 0x08 or mm[p + 13] != 0x00:
            continue
        ip = p + 14
        ihl = (mm[ip] & 0x0f) * 4
        if mm[ip + 9] != 6:
            continue
        total = (mm[ip + 2] << 8) | mm[ip + 3]
        tcp = ip + ihl
        sport, dport, seq, ack, offflags, window = struct.unpack_from('>HHIIHH', mm, tcp)
        thl = (offflags >> 12) * 4
        flags = offflags & 0x1ff
        plen = total - ihl - thl
        head_start = tcp + thl
        head = bytes(mm[head_start:min(off, head_start + 30)]) if plen > 0 else b''
        yield (s + frac / div, mm[ip + 12:ip + 16], sport, mm[ip + 16:ip + 20], dport, seq, ack, flags, window, plen,
               head)


SYN, RST, FIN, ACK = 0x02, 0x04, 0x01, 0x10


class Flow:
    __slots__ = ('isn', 'high', 'retrans', 'zero_windows', 'sent')

    def __init__(self, isn):
        self.isn, self.high, self.retrans, self.zero_windows = isn, 0, [], []
        self.sent = collections.deque(maxlen=4096)   # (rel seq, first transmission time) of recent new segments


def original_time(fl, rel):
    """First transmission time of the segment covering rel (None if it left the recent window)."""
    for r, t in reversed(fl.sent):
        if r <= rel:
            return t
    return None


def ip(b):
    return socket.inet_ntoa(bytes(b))


def analyse_pcap(path, http_port=2000, grpc_port=12345):
    """HTTP pairs keyed by 4-tuple, gRPC stream pairs, retransmissions, zero windows and SYNs."""
    flows = {}
    http = collections.defaultdict(list)       # (client ip, client port, server ip) -> [(t_req, t_resp)]
    outstanding = {}
    grpc_req, grpc_resp = {}, {}               # (conn, stream id) -> first HEADERS time
    syns, rsts = [], []
    count = 0
    for ts, src, sport, dst, dport, seq, ack, flags, window, plen, head in packets(path):
        count += 1
        key = (bytes(src), sport, bytes(dst), dport)
        fl = flows.get(key)
        if fl is None:
            fl = flows[key] = Flow(seq + (1 if flags & SYN else 0))
            if flags & SYN:
                syns.append((ts, ip(src), sport, ip(dst), dport, 'SYN-ACK' if flags & ACK else 'SYN'))
        elif flags & SYN:
            syns.append((ts, ip(src), sport, ip(dst), dport, 'SYN-again'))
        if flags & RST:
            rsts.append((ts, ip(src), sport, ip(dst), dport))
        if window == 0 and not flags & (SYN | RST):
            fl.zero_windows.append(ts)
        new = False
        if plen > 0:
            rel = (seq - fl.isn) & 0xffffffff
            end = rel + plen
            if end <= fl.high:
                t_orig = original_time(fl, rel)
                fl.retrans.append((ts, rel, plen, round((ts - t_orig) * 1e3, 1) if t_orig else None))
            else:
                new = rel >= fl.high
                fl.high = end
                fl.sent.append((rel, ts))
        if not new:
            continue
        if dport == http_port:                 # client -> wrk2api
            conn = (bytes(src), sport, bytes(dst))
            if conn not in outstanding:
                outstanding[conn] = ts
        elif sport == http_port:               # wrk2api -> client
            conn = (bytes(dst), dport, bytes(src))
            t0 = outstanding.pop(conn, None)
            if t0 is not None:
                http[conn].append((t0, ts))
        elif grpc_port in (sport, dport) and len(head) >= 9 and not head.startswith(b'PRI * HTTP/2.0'):
            ftype = head[3]
            sid = int.from_bytes(head[5:9], 'big') & 0x7fffffff
            if ftype == 1 and sid:
                if dport == grpc_port:
                    grpc_req.setdefault(((bytes(src), sport), sid), ts)
                else:
                    grpc_resp.setdefault(((bytes(dst), dport), sid), ts)
    grpc = sorted((t0, grpc_resp[k]) for k, t0 in grpc_req.items() if k in grpc_resp and grpc_resp[k] >= t0)
    unanswered = sorted(t0 for k, t0 in grpc_req.items() if k not in grpc_resp)
    retrans = sorted((t, ip(k[0]), k[1], ip(k[2]), k[3], rel, plen, delay_ms)
                     for k, fl in flows.items() for t, rel, plen, delay_ms in fl.retrans)
    zero = sorted((t, ip(k[0]), k[1], ip(k[2]), k[3]) for k, fl in flows.items() for t in fl.zero_windows)
    return {'packets': count, 'http': http, 'grpc': grpc, 'grpc_unanswered': unanswered, 'retrans': retrans,
            'zero_windows': zero, 'syns': sorted(syns), 'rsts': sorted(rsts)}


# -------------------------------------------------------------------------------------------------------- sampler
def lines_until_eof(stream):
    """Lines of a gzip stream that may have been copied while still being written (truncated last block)."""
    try:
        for line in stream:
            yield line
    except (EOFError, OSError):
        return


def sampler(path):
    """[(t, {counter: value})] cumulative, plus the per-pod and per-CPU breakdowns."""
    ticks, tcp_h, ext_h, cur = [], [], [], None
    with gzip.open(path, 'rt', errors='replace') as stream:
        for line in lines_until_eof(stream):
            tag, _, rest = line.rstrip('\n').partition(' ')
            if tag == 'tcp_h':
                tcp_h = rest.split()
            elif tag == 'ext_h':
                ext_h = rest.split()
            elif tag == '@':
                cur = {'t': float(rest), 'c': {}, 'pod': {}, 'cpu': [], 'podtcp': {}}
                ticks.append(cur)
            elif cur is None:
                continue
            elif tag == 'tcp':
                cur['c'].update({'Tcp' + k: int(v) for k, v in zip(tcp_h, rest.split())})
            elif tag == 'ext':
                cur['c'].update({k: int(v) for k, v in zip(ext_h, rest.split())})
            elif tag == 'softnet':
                cpus = [tuple(map(int, c.split(','))) for c in rest.strip(';').split(';') if c]
                cur['c'].update(softnet_dropped=sum(c[1] for c in cpus), softnet_squeezed=sum(c[2] for c in cpus))
                cur['softnet'] = [c[0] for c in cpus]
            elif tag == 'nic':
                v = list(map(int, rest.split()))
                cur['c'].update(nic_rx=v[0], nic_rx_missed=v[1], nic_rx_dropped=v[2], nic_rx_fifo=v[3],
                                nic_rx_errors=v[4], nic_tx=v[5], nic_tx_dropped=v[6])
            elif tag == 'vx' and rest.strip():
                v = list(map(int, rest.split()))
                cur['c'].update(vx_rx=v[0], vx_rx_dropped=v[1], vx_tx=v[2], vx_tx_dropped=v[3])
            elif tag == 'ct' and rest.strip():
                v = list(map(int, rest.split()))
                cur['c'].update(ct_insert_failed=v[0], ct_drop=v[1], ct_early_drop=v[2])
            elif tag == 'psi':
                v = list(map(int, rest.split()))
                cur['c'].update(psi_cpu_some=v[0], psi_io_some=v[1], psi_io_full=v[2], psi_mem_some=v[3],
                                psi_mem_full=v[4])
            elif tag == 'cpu':
                cur['cpu'] = [tuple(map(int, c.split(','))) for c in rest.strip(';').split(';') if c]
            elif tag == 'pod':
                for item in rest.split():
                    uid, _, vals = item.partition('=')
                    v = vals.split(',')
                    cur['pod'][uid.replace('_', '-')] = tuple(int(x) if x else 0 for x in v)
            elif tag == 'podtcp':
                cur['podtcp'] = {}
                for item in rest.split():
                    uid, _, vals = item.partition('=')
                    v = [int(x) for x in vals.split(',') if x != '']
                    if len(v) == 7:
                        cur['podtcp'][uid.replace('_', '-')] = v
            elif tag == 'vm':
                v = list(map(int, rest.split()))
                cur['c'].update(pgmajfault=v[0], compact_stall=v[1], allocstall=v[2] + v[3], thp_fault_alloc=v[4],
                                pswpin=v[5], pswpout=v[6])
            elif tag == 'disk':
                io = 0
                for d in rest.split():
                    io += int(d.split('=')[1].split(',')[4])
                cur['c']['disk_io_ms'] = io
    return [t for t in ticks if t['c'] and t['cpu']]


KEYS = ['TcpRetransSegs', 'TCPTimeouts', 'TCPLossProbes', 'TCPSynRetrans', 'TCPSpuriousRTOs', 'ListenOverflows',
        'ListenDrops', 'TCPBacklogDrop', 'TCPRcvQDrop', 'TCPZeroWindowDrop', 'TCPToZeroWindowAdv', 'TCPMemoryPressures',
        'softnet_dropped', 'softnet_squeezed', 'nic_rx_missed', 'nic_rx_dropped', 'nic_rx_fifo', 'nic_rx_errors',
        'nic_tx_dropped', 'vx_rx_dropped', 'vx_tx_dropped', 'ct_insert_failed', 'ct_drop', 'ct_early_drop',
        'pgmajfault', 'compact_stall', 'allocstall', 'pswpin', 'pswpout']
RATES = ['psi_cpu_some', 'psi_io_some', 'psi_io_full', 'psi_mem_some', 'psi_mem_full', 'disk_io_ms']


def deltas(ticks):
    """Per tick interval: counter increments, PSI / disk busy as a fraction of the interval, the busiest CPU's softirq
    and total busy share, and each pod's CFS throttling (count, ms) and cpu.pressure fraction."""
    out = []
    for a, b in zip(ticks, ticks[1:]):
        dt = b['t'] - a['t']
        d = {'t0': a['t'], 't1': b['t']}
        for k in KEYS:
            if k in a['c'] and k in b['c']:
                d[k] = b['c'][k] - a['c'][k]
        for k in RATES:
            if k in a['c'] and k in b['c']:
                d[k] = (b['c'][k] - a['c'][k]) / (dt * (1e3 if k == 'disk_io_ms' else 1e6))
        soft, busy = [], []
        for ca, cb in zip(a['cpu'], b['cpu']):
            tot = sum(cb) - sum(ca) or 1
            soft.append((cb[6] - ca[6]) / tot)
            busy.append(1 - ((cb[3] - ca[3]) + (cb[4] - ca[4])) / tot)
        d['softirq_max'] = max(soft)
        d['softirq_cpu'] = soft.index(max(soft))
        d['busy_max'] = max(busy)
        d['busy_mean'] = sum(busy) / len(busy)
        pods = {}
        for uid, vb in b['pod'].items():
            va = a['pod'].get(uid)
            if va and len(va) >= 5 and len(vb) >= 5:
                pods[uid] = {'throttled': vb[1] - va[1], 'throttled_ms': (vb[2] - va[2]) / 1e3,
                             'cpu_cores': (vb[3] - va[3]) / 1e6 / dt, 'pressure': (vb[4] - va[4]) / 1e6 / dt}
        d['pods'] = pods
        ptcp = {}
        for uid, vb in b.get('podtcp', {}).items():
            va = a.get('podtcp', {}).get(uid)
            if va:
                dv = [x - y for x, y in zip(vb, va)]
                if any(dv[:6]):
                    ptcp[uid] = dict(zip(('retrans', 'rto', 'tlp', 'syn_retrans', 'spurious_rto', 'fast_retrans'), dv[:6]))
        d['podtcp'] = ptcp
        out.append(d)
    return out


# ----------------------------------------------------------------------------------------------------------- stalls
def episodes(pairs, slow_s, gap_s=1.0):
    """Group slow requests (t_req, t_resp, conn) into episodes separated by >= gap_s."""
    slow = sorted(p for p in pairs if p[1] - p[0] >= slow_s)
    eps, cur = [], []
    for p in slow:
        if cur and p[0] - cur[-1][0] > gap_s:
            eps.append(cur); cur = []
        cur.append(p)
    if cur:
        eps.append(cur)
    return eps


def pct(values, q):
    v = sorted(values)
    return v[min(len(v) - 1, int(q * len(v)))] if v else None


def fmt_t(t):
    return datetime.utcfromtimestamp(t).strftime('%H:%M:%S.%f')[:-3]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('root', type=Path)
    ap.add_argument('--slow-ms', type=float, default=150)
    ap.add_argument('--json', type=Path)
    ap.add_argument('--all', action='store_true', help='also episodes outside ramp points and probes (warm-up, pre-fill)')
    a = ap.parse_args()
    diag = a.root / 'diag'
    pods = {}
    if (diag / 'pods-all.json').exists():
        for p in json.loads((diag / 'pods-all.json').read_text())['items']:
            pods[p['metadata']['uid']] = (p['metadata']['name'], p['spec'].get('nodeName'))
    points = []
    for f in glob.glob(f'{a.root}/run/*/*/result.json'):
        r = json.loads(open(f).read())
        if 'started' in r and 'finished' in r:
            points.append((datetime.fromisoformat(r['started']).timestamp(), datetime.fromisoformat(r['finished']).timestamp(),
                           Path(f).parent.name, r))
    points.sort()

    def point_of(t):
        for t0, t1, name, r in points:
            if t0 - 1 <= t <= t1 + 1:
                return name, r
        return None, None

    print(f'== {a.root.name}')
    caps = {}
    for n in (0, 7):
        path = next(iter(diag.glob(f'*-node-{n}.pcap')), None)
        if path:
            caps[n] = analyse_pcap(path)
            log = next(iter(diag.glob(f'*-node-{n}.tcpdump.log')), None)
            dropped = [l.strip() for l in open(log)] if log else []
            print(f'   node-{n} capture: {caps[n]["packets"]:,} TCP packets, {sum(len(v) for v in caps[n]["http"].values()):,} '
                  f'HTTP pairs, {len(caps[n]["grpc"]):,} gRPC stream pairs, {len(caps[n]["retrans"])} retransmitted segments; '
                  + ' / '.join(l for l in dropped if 'dropped' in l))
    samp = {}
    for f in sorted(diag.glob('sampler-node-*.txt.gz')):
        node = f.name[len('sampler-'):-len('.txt.gz')]
        samp[node] = deltas(sampler(f))
    print(f'   samplers: {len(samp)} nodes, {sum(len(v) for v in samp.values()):,} intervals')

    # every HTTP pair seen at node-0, with the node-7 times of the same request (same 4-tuple, same order)
    c0, c7 = caps.get(0), caps.get(7)
    pairs = []
    for conn, lst in c0['http'].items():
        other = c7['http'].get(conn, []) if c7 else []
        starts7 = [p[0] for p in other]
        for t_req, t_resp in lst:
            j = bisect.bisect_left(starts7, t_req - 0.05)
            m = other[j] if j < len(other) and abs(other[j][0] - t_req) < 2 else None
            pairs.append((t_req, t_resp, conn, m))
    pairs.sort()
    eps = episodes(pairs, a.slow_ms / 1e3)
    if not a.all:
        eps = [ep for ep in eps if (point_of(ep[0][0])[0] or '').startswith(('rate-', 'probe-'))]
    print(f'   {len(pairs):,} requests seen at node-0; {sum(len(e) for e in eps):,} slower than {a.slow_ms:.0f} ms in '
          f'{len(eps)} episodes\n')
    report = []
    grpc = c7['grpc'] if c7 else []
    grpc_starts = [g[0] for g in grpc]
    for i, ep in enumerate(eps):
        t_on = ep[0][0]
        releases = sorted(p[1] for p in ep)
        t_rel = pct(releases, 0.5)
        t_end = max(releases)
        lat = [p[1] - p[0] for p in ep]
        name, r = point_of(t_on)
        # requests sent in the window: were they sent on schedule? (gaps in sending = generator side)
        window = [p for p in pairs if t_on - 1 <= p[0] <= t_end]
        sends = sorted(p[0] for p in window)
        max_send_gap = max((b - a_ for a_, b in zip(sends, sends[1:])), default=0)
        # node-7 view of the slow requests: time inside node-7 (arrival -> response sent) vs the node-0 latency
        inside = [(p[3][1] - p[3][0], p[1] - p[0], p[3][0] - p[0]) for p in ep if p[3]]
        # gRPC wrk2api -> composepost streams started in the window
        lo = bisect.bisect_left(grpc_starts, t_on - 0.5)
        hi = bisect.bisect_right(grpc_starts, t_end)
        g = grpc[lo:hi]
        g_lat = [b - a_ for a_, b in g]
        e = {'episode': i + 1, 'point': name, 'offered': r and r.get('offered_rps'), 'point_p99_ms': r and r.get('p99_ms'),
             'onset': fmt_t(t_on), 'onset_unix': t_on, 'release_median': fmt_t(t_rel), 'last_release': fmt_t(t_end),
             'slow_requests': len(ep), 'max_latency_ms': round(max(lat) * 1e3, 1),
             'release_spread_ms': round((pct(releases, 0.9) - pct(releases, 0.1)) * 1e3, 1),
             'max_send_gap_ms': round(max_send_gap * 1e3, 1),
             'node7_inside_ms_max': round(max((x[0] for x in inside), default=0) * 1e3, 1),
             'node7_share_of_latency': round(sum(x[0] for x in inside) / max(1e-9, sum(x[1] for x in inside)), 3) if inside else None,
             'net_0_to_7_ms_max': round(max((x[2] for x in inside), default=0) * 1e3, 2),
             'grpc_streams': len(g), 'grpc_max_ms': round(max(g_lat, default=0) * 1e3, 1),
             'grpc_p50_ms': round((pct(g_lat, .5) or 0) * 1e3, 1)}
        w0, w1 = t_on - 1.0, t_end + 0.5
        e['retrans'] = [(fmt_t(x[0]),) + x[1:] for c in (c0, c7) if c for x in c['retrans'] if w0 <= x[0] <= w1][:40]
        e['zero_windows'] = [(fmt_t(x[0]),) + x[1:] for c in (c0, c7) if c for x in c['zero_windows'] if w0 <= x[0] <= w1][:20]
        e['syns'] = [(fmt_t(x[0]),) + x[1:] for c in (c0, c7) if c for x in c['syns'] if w0 <= x[0] <= w1][:20]
        e['rsts'] = [(fmt_t(x[0]),) + x[1:] for c in (c0, c7) if c for x in c['rsts'] if w0 <= x[0] <= w1][:20]
        # kernel counters on every node in the window, against that node's typical interval
        nodes = {}
        for node, ds in samp.items():
            inwin = [d for d in ds if d['t1'] >= w0 and d['t0'] <= w1]
            if not inwin:
                continue
            hits = {}
            for k in KEYS:
                v = sum(d.get(k, 0) for d in inwin)
                if v:
                    base = sorted(d.get(k, 0) for d in ds)
                    hits[k] = (v, base[len(base) // 2], base[int(len(base) * .99)])
            for k in RATES + ['softirq_max', 'busy_max']:
                v = max(d.get(k, 0) for d in inwin)
                base = sorted(d.get(k, 0) for d in ds)
                p99 = base[int(len(base) * .99)]
                if v > max(p99, 0.05):
                    hits[k] = (round(v, 3), round(base[len(base) // 2], 3), round(p99, 3))
            thr = collections.defaultdict(float)
            pres = collections.defaultdict(float)
            for d in inwin:
                for uid, pv in d['pods'].items():
                    thr[uid] += pv['throttled_ms']
                    pres[uid] = max(pres[uid], pv['pressure'])
            for uid in thr:
                if thr[uid] > 0 or pres[uid] > 0.2:
                    nm = pods.get(uid, (uid[:8], None))[0]
                    hits[f'pod:{nm}'] = {'throttled_ms': round(thr[uid], 1), 'cpu_pressure_max': round(pres[uid], 3)}
            ptcp = collections.defaultdict(lambda: collections.Counter())
            for d in inwin:
                for uid, pv in d.get('podtcp', {}).items():
                    ptcp[uid].update(pv)
            for uid, c in ptcp.items():
                if c['rto'] or c['syn_retrans'] or c['retrans']:
                    nm = pods.get(uid, (uid[:8], None))[0]
                    hits[f'tcp:{nm}'] = {k: v for k, v in c.items() if v}
            if hits:
                nodes[node] = hits
        e['nodes'] = nodes
        report.append(e)
        print(f"-- episode {e['episode']}: {e['point']} (offered {e['offered']}, point p99 {e['point_p99_ms']} ms)")
        print(f"   onset {e['onset']}  release median {e['release_median']}  last {e['last_release']}  "
              f"{e['slow_requests']} slow requests, max {e['max_latency_ms']} ms, 10-90 % release spread {e['release_spread_ms']} ms")
        print(f"   generator: largest gap between requests sent {e['max_send_gap_ms']} ms | node-0 -> node-7 transit max "
              f"{e['net_0_to_7_ms_max']} ms | time inside node-7 max {e['node7_inside_ms_max']} ms, share of latency "
              f"{e['node7_share_of_latency']}")
        print(f"   gRPC wrk2api -> composepost in window: {e['grpc_streams']} streams, p50 {e['grpc_p50_ms']} ms, max {e['grpc_max_ms']} ms")
        for k in ('retrans', 'zero_windows', 'syns', 'rsts'):
            if e[k]:
                print(f'   {k}: {len(e[k])}: ' + '; '.join(' '.join(map(str, x)) for x in e[k][:8]))
        for node, hits in sorted(e['nodes'].items(), key=lambda kv: int(kv[0].split('-')[1])):
            print(f'   {node}: ' + ', '.join(f'{k}={v}' for k, v in hits.items()))
        print()
    # baseline: how often do retransmissions etc. happen outside episodes?
    if a.json:
        a.json.write_text(json.dumps({'root': str(a.root), 'slow_ms': a.slow_ms, 'episodes': report}, indent=1, default=str))


if __name__ == '__main__':
    main()


# ---------------------------------------------------------------------------------------------- MongoDB wire protocol
MONGO_OPS = {1: 'reply', 2004: 'query', 2012: 'compressed', 2013: 'msg'}


def analyse_mongo(path, port=27017):
    """Every MongoDB operation in a capture of one server's traffic: [(t_request, t_reply, client port, opcode)].
    Follows message boundaries per TCP direction (header = msgLen, requestID, responseTo, opCode, little-endian), so a
    reply is paired with its request by responseTo == requestID even when messages span several segments."""
    nxt, pending, ops, retrans = {}, {}, [], []
    high = {}
    for ts, src, sport, dst, dport, seq, ack, flags, window, plen, head in packets(path):
        if plen <= 0:
            continue
        key = (bytes(src), sport, bytes(dst), dport)
        end = (seq + plen) & 0xffffffff
        h = high.get(key)
        if h is not None and ((end - h) & 0xffffffff) > 0x7fffffff or (h is not None and end == h):
            retrans.append((ts, ip(src), sport, ip(dst), dport, plen))
            continue
        high[key] = end
        start = nxt.get(key)
        if start is None or start == seq:
            if len(head) < 16:
                nxt.pop(key, None)
                continue
            length, rid, rto, opcode = struct.unpack_from('<iiii', head, 0)
            if opcode not in MONGO_OPS or not 16 <= length < 48 << 20:
                nxt.pop(key, None)
                continue
            nxt[key] = (seq + length) & 0xffffffff
            client = sport if dport == port else dport
            if dport == port:
                pending[(client, rid)] = (ts, opcode)
            else:
                req = pending.pop((client, rto), None)
                if req:
                    ops.append((req[0], ts, client, req[1]))
        elif ((seq - start) & 0xffffffff) < 0x7fffffff:
            nxt.pop(key, None)          # skipped past a boundary (capture gap): resynchronise on the next header
    ops.sort()
    return {'ops': ops, 'unanswered': len(pending), 'retrans': retrans}
