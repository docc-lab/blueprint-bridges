#!/usr/bin/env python3
"""Tomislav-RetCtx (user 2026-11-06: "plot the averaged results please (as offered throughput on the x-axis and response
time on the Y) - make separate graphs for mean and p99"): response time against OFFERED throughput for the
upstream-matching Social Network campaign (snbounded, Redis snapshots off), one figure for the mean and one for the p99.
None, Vanilla, PB, CGPB, SB; each load level is the average over that kind's runs of the run's wrk2 mean (or p99), with
error bars = bootstrap 5-95 % of that average over the runs (with two runs: the range of the two). Style as
plot_resp_vs_achieved.py: one column wide (3.33 in), 8 pt text, LINEAR y capped so the pre-knee region is readable
(points above leave the top of the panel), every point at full weight, legend in one row above the panel.
usage: plot_snbd_offered.py --nt ROOT... --v ROOT... --pb ROOT... --cgpb ROOT... --sb ROOT... --out DIR
                            [--ymax-mean MS] [--ymax-p99 MS] [--height IN]"""
import argparse, glob, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator, MultipleLocator
from plot_ramp_n1 import COLORS, LABELS, ORDER

W, FS = 3.33, 8
BOTTOM_IN, TOP_IN, LEFT_IN = 0.34, 0.20, 0.42  # margins in inches (tick labels + x label; one legend row; y label)


def runs_of(root):
    """Every ramp point of one run: {offered req/s: result.json}.  Tomislav-RetCtx: root is a run root
    with one case, or one case directory (<root>/run/01-<kind>) of a root holding several kinds."""
    files = glob.glob(f'{root}/rate-*/result.json') or glob.glob(f'{root}/run/*/rate-*/result.json')
    return {r['offered_rps']: r for r in (json.load(open(f)) for f in files)}


def averaged(runs, key, n_boot=2000, seed=7):
    """[(offered, average over runs, bootstrap 5 %, bootstrap 95 %)] for the load levels every run reached."""
    rng = np.random.default_rng(seed)
    out = []
    for offered in sorted(set.intersection(*(set(r) for r in runs))):
        vals = np.array([r[offered][key] for r in runs], float)
        boots = vals[rng.integers(0, len(vals), size=(n_boot, len(vals)))].mean(axis=1)
        out.append((offered, vals.mean(), np.percentile(boots, 5), np.percentile(boots, 95)))
    return out


def figure(data, key, ymax, ylabel, height, out):
    kinds = [k for k in ORDER if k in data]  # Tomislav-RetCtx: the kinds given (user 2026-11-06: a subset while sb runs)
    fig, ax = plt.subplots(figsize=(W, height))
    fig.subplots_adjust(left=LEFT_IN / W, right=.985, top=1 - TOP_IN / height, bottom=BOTTOM_IN / height)
    xmax = 0
    for k in kinds:
        pts = averaged(data[k], key)
        xs = [p[0] / 1000 for p in pts]; ys = [p[1] for p in pts]; xmax = max(xmax, max(xs))
        ls = ':' if k == 'nt' else '-'
        ax.plot(xs, ys, color=COLORS[k], lw=1.1, ls=ls, marker='o', ms=1.6, mew=0, zorder=3)
        if len(data[k]) > 1:  # Tomislav-RetCtx: one run has no spread to show
            ax.errorbar(xs, ys, yerr=[[max(0, p[1] - p[2]) for p in pts], [max(0, p[3] - p[1]) for p in pts]],
                        fmt='none', ecolor=COLORS[k], elinewidth=.6, capsize=1.3, capthick=.6, zorder=2)
    ax.set_ylim(0, ymax); ax.set_xlim(0, xmax * 1.03)
    ax.set_ylabel(ylabel, labelpad=1.5); ax.set_xlabel('offered (k req/s)', labelpad=1.5)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=4, steps=[1, 2, 2.5, 5, 10], integer=True))
    ax.xaxis.set_major_locator(MultipleLocator(0.5))
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    ax.grid(True, lw=.3, alpha=.35); ax.set_axisbelow(True); ax.tick_params(length=2, pad=1.5)
    handles = [Line2D([], [], color=COLORS[k], lw=1.2, ls=':' if k == 'nt' else '-', label=LABELS[k]) for k in kinds]
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(.56, 1.0), ncol=5, frameon=False, handlelength=1.2,
               handletextpad=.3, columnspacing=.8, borderaxespad=0, borderpad=.1)
    for ext in ('pdf', 'png', 'svg'):
        fig.savefig(f'{out}.{ext}', dpi=300)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    for k in ORDER:
        ap.add_argument(f'--{k}', nargs='+', default=None, help=f'{LABELS[k]} run roots (kinds left out are not drawn)')
    ap.add_argument('--out', required=True, help='output directory')
    ap.add_argument('--ymax-mean', type=float, default=60); ap.add_argument('--ymax-p99', type=float, default=200)
    ap.add_argument('--ymax-p50', type=float, default=60)  # Tomislav-RetCtx (user 2026-11-06: "also produce a p50 plot")
    ap.add_argument('--height', type=float, default=1.7, help='figure height (in)')
    ap.add_argument('--name', default='snbd-offered', help='file name prefix, e.g. snbd-mixed-offered')
    a = ap.parse_args()
    plt.rcParams.update({'font.size': FS, 'axes.labelsize': FS, 'xtick.labelsize': FS, 'ytick.labelsize': FS,
                         'legend.fontsize': FS, 'font.family': 'sans-serif', 'axes.linewidth': .6,
                         'xtick.major.width': .6, 'ytick.major.width': .6, 'pdf.fonttype': 42, 'svg.fonttype': 'none'})
    data = {k: [runs_of(r) for r in getattr(a, k)] for k in ORDER if getattr(a, k)}
    os.makedirs(a.out, exist_ok=True)
    figure(data, 'mean_ms', a.ymax_mean, 'mean resp. time (ms)', a.height, f'{a.out}/{a.name}-mean')
    figure(data, 'p99_ms', a.ymax_p99, 'p99 resp. time (ms)', a.height, f'{a.out}/{a.name}-p99')
    figure(data, 'p50_ms', a.ymax_p50, 'p50 resp. time (ms)', a.height, f'{a.out}/{a.name}-p50')
    json.dump({k: getattr(a, k) for k in ORDER if getattr(a, k)} | {'ymax_mean': a.ymax_mean, 'ymax_p99': a.ymax_p99, 'ymax_p50': a.ymax_p50},
              open(f'{a.out}/{a.name}-inputs.json', 'w'), indent=1)
    print('wrote', a.out)


if __name__ == '__main__':
    main()
