#!/usr/bin/env python3
"""
Generate benchmark and schematic figures for the Project Azure report.

Reads artifacts from data/bench/ (produced by `cargo run -p gateway --bin azure_bench`).
Writes PNGs to docs/figures/.

  pip install -r docs/requirements-figures.txt
  python3 docs/make_figures.py

Optional:
  python3 docs/make_figures.py --bench-dir path/to/bench --out-dir path/to/figures

All charts share one teal–blue palette (class ``P`` + ``apply_theme()``); adjust hexes there to re-theme.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import LogNorm
import numpy as np


# ---------------------------------------------------------------------------
# Unified figure theme (single cool family: teal–slate blues, print-friendly)
# ---------------------------------------------------------------------------
class P:
    ink = "#1b2838"
    grid = "#d8e0e8"
    primary = "#156082"
    secondary = "#3d8eb9"
    tertiary = "#7eb0d0"
    muted = "#9db3c8"
    static_bar = "#8aa3b8"
    adaptive_bar = "#156082"
    fill_soft = "#e8f1f6"
    arrow = "#3d5a73"
    heat_seq = "Blues"


def _mode_colors(n: int) -> list:
    """n distinct shades in the same hue family (light → dark)."""
    base = plt.get_cmap("Blues")(np.linspace(0.35, 0.92, max(n, 2)))
    return [tuple(c) for c in base[:n]]


def apply_theme() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": P.ink,
            "axes.labelcolor": P.ink,
            "axes.titlecolor": P.ink,
            "xtick.color": P.ink,
            "ytick.color": P.ink,
            "text.color": P.ink,
            "grid.color": P.grid,
            "grid.alpha": 0.75,
            "grid.linestyle": "-",
            "font.size": 10,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "legend.framealpha": 0.92,
            "legend.edgecolor": P.grid,
        }
    )


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def unwrap_row(obj: dict) -> dict:
    if "row" in obj:
        return obj["row"]
    return obj


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def fig_latency_tradeoff(rows: list[dict], out: Path) -> None:
    raws = [unwrap_row(x) for x in rows]
    sel = [
        r
        for r in raws
        if r.get("capacity") == 256 and r.get("consumer_work_us") == 0
    ]
    if not sel:
        return
    sel.sort(key=lambda r: r["reader_sleep_us"])
    xs = [r["reader_sleep_us"] for r in sel]
    ys = [max(r["p95_lag_us"], 1) for r in sel]
    thr = [r["throughput_rps_total"] for r in sel]

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax1.plot(
        xs, ys, "o-", color=P.primary, lw=2, markersize=8, label="P95 ingest lag (µs)"
    )
    ax1.set_xlabel("Reader sleep (µs), static micro-bench")
    ax1.set_ylabel("P95 lag (µs)")
    ax1.set_yscale("log")
    ax1.grid(True)

    ax2 = ax1.twinx()
    ax2.plot(
        xs, thr, "s--", color=P.secondary, lw=1.5, markersize=6, label="Throughput (rd/s)"
    )
    ax2.set_ylabel("Total throughput (readings/s)")

    lines = ax1.get_lines() + ax2.get_lines()
    ax1.legend(lines, [ln.get_label() for ln in lines], loc="upper left", framealpha=0.9)
    fig.suptitle("Stable regime: reader sleep vs latency & throughput (cap=256, no consumer work)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_capacity_backpressure(rows: list[dict], out: Path) -> None:
    raws = [unwrap_row(x) for x in rows]
    sel = [
        r
        for r in raws
        if r.get("reader_sleep_us") == 0 and r.get("consumer_work_us") == 200
    ]
    if not sel:
        return
    sel.sort(key=lambda r: r["capacity"])
    caps = [r["capacity"] for r in sel]
    p95 = [max(r["p95_lag_us"], 1) for r in sel]
    ov = [r["sensor_overwrites"] for r in sel]

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax1.bar(
        np.arange(len(caps)),
        p95,
        color=P.primary,
        alpha=0.88,
        label="P95 lag (µs)",
    )
    ax1.set_xticks(np.arange(len(caps)))
    ax1.set_xticklabels([str(c) for c in caps], rotation=15)
    ax1.set_xlabel("Buffer capacity (slots)")
    ax1.set_ylabel("P95 lag (µs)")
    ax1.set_yscale("log")

    ax2 = ax1.twinx()
    ax2.plot(
        np.arange(len(caps)),
        ov,
        "o-",
        color=P.secondary,
        lw=2,
        markersize=8,
        label="Sensor overwrites",
    )
    ax2.set_ylabel("Upstream overwrites (simulated ring)")

    fig.suptitle("Overload (200 µs consumer work): capacity reshapes lag vs loss")
    fig.tight_layout()
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper right")
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_load_scaling(load_rows: list[dict], out: Path) -> None:
    if not load_rows:
        return
    by_tag: dict[str, list[dict]] = defaultdict(list)
    for x in load_rows:
        tag = x.get("tag", "default")
        by_tag[tag].append(unwrap_row(x))

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    for ax, (tag, series) in zip(axes, sorted(by_tag.items())):
        series.sort(key=lambda r: r["offered_rps_total"])
        off = [r["offered_rps_total"] for r in series]
        thr = [r["throughput_rps_total"] for r in series]
        ov = [r["sensor_overwrites"] for r in series]
        ax.plot(off, thr, "o-", label="Achieved throughput", color=P.primary)
        ax.set_xlabel("Offered load (readings/s, both sensors)")
        ax.set_ylabel("Throughput (readings/s)")
        ax.grid(True)
        ax.set_title(tag.replace("_", " "))
        ax2 = ax.twinx()
        ax2.plot(
            off,
            ov,
            "s--",
            color=P.secondary,
            alpha=0.9,
            label="Overwrites",
            markersize=5,
        )
        ax2.set_ylabel("Overwrites")
    fig.suptitle("Load scaling (buffer cap=256, reader sleep=0)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_governor_ab(gov_rows: list[dict], out: Path) -> None:
    if not gov_rows:
        return
    keys = []
    for x in gov_rows:
        r = unwrap_row(x)
        keys.append((r["capacity"], r["consumer_work_us"]))
    keys = sorted(set(keys))

    static_p95, adapt_p95 = [], []
    static_ov, adapt_ov = [], []
    for cap, cw in keys:
        for x in gov_rows:
            r = unwrap_row(x)
            if r["capacity"] != cap or r["consumer_work_us"] != cw:
                continue
            pol = x.get("policy", "static")
            if pol == "static":
                static_p95.append(max(r["p95_lag_us"], 1))
                static_ov.append(r["sensor_overwrites"])
            else:
                adapt_p95.append(max(r["p95_lag_us"], 1))
                adapt_ov.append(r["sensor_overwrites"])

    if not static_p95:
        return

    xn = np.arange(len(keys))
    w = 0.35
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 7), sharex=True)
    ax1.bar(xn - w / 2, static_p95, w, label="Static 100 µs", color=P.static_bar)
    ax1.bar(xn + w / 2, adapt_p95, w, label="Adaptive governor", color=P.adaptive_bar)
    ax1.set_ylabel("P95 lag (µs)")
    ax1.set_yscale("log")
    ax1.legend()
    ax1.grid(True, axis="y")
    ax1.set_title("Governor A/B: latency")

    ax2.bar(xn - w / 2, static_ov, w, label="Static", color=P.static_bar)
    ax2.bar(xn + w / 2, adapt_ov, w, label="Adaptive", color=P.adaptive_bar)
    ax2.set_ylabel("Sensor overwrites")
    ax2.set_xticks(xn)
    ax2.set_xticklabels([f"cap={c}\nwork={w}µs" for c, w in keys], fontsize=8)
    ax2.set_title("Governor A/B: upstream loss proxy")
    ax2.grid(True, axis="y")

    fig.suptitle("Static nominal sleep vs adaptive polling (same offered load)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_governor_cpu_efficiency(gov_rows: list[dict], out: Path) -> None:
    """Scatter throughput vs process CPU seconds (from azure_bench getrusage)."""
    if not gov_rows:
        return
    st_thr, st_cpu = [], []
    ad_thr, ad_cpu = [], []
    for x in gov_rows:
        r = unwrap_row(x)
        cpu = float(r.get("process_cpu_secs") or 0)
        if cpu <= 1e-12:
            continue
        thr = float(r.get("throughput_rps_total") or 0)
        pol = x.get("policy", "")
        if pol == "static":
            st_thr.append(thr)
            st_cpu.append(cpu)
        else:
            ad_thr.append(thr)
            ad_cpu.append(cpu)
    if not st_thr and not ad_thr:
        return

    fig, ax = plt.subplots(figsize=(7, 5))
    if st_thr:
        ax.scatter(
            st_thr,
            st_cpu,
            s=72,
            marker="o",
            edgecolors=P.ink,
            linewidths=0.55,
            c=P.static_bar,
            alpha=0.9,
            label="Static 100 µs",
        )
    if ad_thr:
        ax.scatter(
            ad_thr,
            ad_cpu,
            s=72,
            marker="s",
            edgecolors=P.ink,
            linewidths=0.55,
            c=P.adaptive_bar,
            alpha=0.88,
            label="Adaptive",
        )
    ax.set_xlabel("Throughput (readings/s, total)")
    ax.set_ylabel("Process CPU time (s, user + sys)")
    ax.grid(True)
    ax.legend()
    fig.suptitle("Governor A/B: throughput vs process CPU (Unix getrusage)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_rho_scatter(rows: list[dict], out: Path) -> None:
    raws = [unwrap_row(x) for x in rows]
    if len(raws) < 2:
        return
    rho = [max(r["rho_hat"], 1e-6) for r in raws]
    p95 = [max(r["p95_lag_us"], 1) for r in raws]
    caps = [r["capacity"] for r in raws]

    fig, ax = plt.subplots(figsize=(7, 5))
    sc = ax.scatter(
        rho, p95, c=caps, cmap=P.heat_seq, alpha=0.82, s=55, norm=LogNorm(), edgecolors=P.ink, linewidths=0.35
    )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("ρ̂ (offered × consumer work × 10⁻⁶)")
    ax.set_ylabel("P95 lag (µs)")
    ax.grid(True)
    fig.colorbar(sc, ax=ax, label="Buffer capacity")
    fig.suptitle("Pareto sweep: stability ratio vs tail lag")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_watermark_heatmap(rows: list[dict], out: Path, work_us: int = 200) -> None:
    raws = [unwrap_row(x) for x in rows]
    sel = [r for r in raws if r.get("consumer_work_us") == work_us]
    if not sel:
        return
    caps = sorted({r["capacity"] for r in sel})
    sleeps = sorted({r["reader_sleep_us"] for r in sel})
    mat = np.full((len(sleeps), len(caps)), np.nan)
    for r in sel:
        i = sleeps.index(r["reader_sleep_us"])
        j = caps.index(r["capacity"])
        mat[i, j] = r["buffer_high_watermark"]

    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(mat, aspect="auto", cmap=P.heat_seq, origin="lower")
    ax.set_xticks(np.arange(len(caps)))
    ax.set_xticklabels([str(c) for c in caps])
    ax.set_yticks(np.arange(len(sleeps)))
    ax.set_yticklabels([str(s) for s in sleeps])
    ax.set_xlabel("Buffer capacity")
    ax.set_ylabel("Reader sleep (µs)")
    fig.colorbar(im, ax=ax, label="High watermark (items)")
    fig.suptitle(f"Queue high watermark heatmap (consumer work = {work_us} µs)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_push_waits(rows: list[dict], out: Path) -> None:
    raws = [unwrap_row(x) for x in rows]
    raws.sort(key=lambda r: -r.get("push_wait_count", 0))
    top = raws[: min(12, len(raws))]
    if not top:
        return
    labels = [
        f"c={r['capacity']}\ns={r['reader_sleep_us']}\nw={r['consumer_work_us']}" for r in top
    ]
    vals = [r["push_wait_count"] for r in top]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    bar_colors = plt.get_cmap(P.heat_seq)(np.linspace(0.45, 0.88, len(top)))
    ax.barh(range(len(top)), vals, color=bar_colors)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xlabel("Producer push-wait count (backpressure)")
    ax.invert_yaxis()
    fig.suptitle("Top configurations by blocked producers (Pareto sweep)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_governor_ticks(gov_rows: list[dict], out: Path) -> None:
    if not gov_rows:
        return
    static_eco = static_fr = adapt_eco = adapt_fr = 0.0
    n_static = n_adapt = 0
    for x in gov_rows:
        r = unwrap_row(x)
        pol = x.get("policy", "")
        if pol == "static":
            n_static += 1
        else:
            n_adapt += 1
        eco = float(r.get("governor_economy_ticks", 0))
        fr = float(r.get("governor_fast_recovery_ticks", 0))
        if pol == "static":
            static_eco += eco
            static_fr += fr
        else:
            adapt_eco += eco
            adapt_fr += fr

    if n_static == 0:
        return
    static_eco /= n_static
    static_fr /= n_static
    adapt_eco /= max(n_adapt, 1)
    adapt_fr /= max(n_adapt, 1)

    fig, ax = plt.subplots(figsize=(6, 4.5))
    x = np.arange(2)
    w = 0.35
    ax.bar(
        x - w / 2,
        [static_eco, adapt_eco],
        w,
        label="Economy ticks (avg)",
        color=P.tertiary,
    )
    ax.bar(
        x + w / 2,
        [static_fr, adapt_fr],
        w,
        label="FastRecovery ticks (avg)",
        color=P.primary,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(["Static", "Adaptive"])
    ax.set_ylabel("Tick count (averaged over sweep grid)")
    ax.legend()
    ax.grid(True, axis="y")
    fig.suptitle("Governor mode activity (micro-bench instrumentation)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_governor_trace(trace_path: Path, out: Path) -> None:
    if not trace_path.exists():
        return
    with trace_path.open(encoding="utf-8") as f:
        pts = json.load(f)
    if not pts:
        return
    step = max(1, len(pts) // 1200)
    pts = pts[::step]
    t = [p["t_ms"] for p in pts]
    slp = [p["sleep_us"] for p in pts]
    modes = [p["mode"] for p in pts]
    uniq = sorted(set(modes))
    cols = _mode_colors(len(uniq))
    mode_to_c = {m: cols[i] for i, m in enumerate(uniq)}

    fig, ax = plt.subplots(figsize=(10, 4))
    for m in uniq:
        tt = [ti for ti, mo in zip(t, modes) if mo == m]
        ss = [si for si, mo in zip(slp, modes) if mo == m]
        ax.scatter(tt, ss, s=4, alpha=0.5, c=[mode_to_c[m]], label=m)

    ax.set_xlabel("Time (ms)")
    ax.set_ylabel("Governor sleep (µs)")
    ax.set_yscale("log")
    ax.grid(True)
    ax.legend(markerscale=3, fontsize=7, ncol=2)
    fig.suptitle("Adaptive sleep trajectory (subsampled governor_trace.json)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_zero_loss(zpath: Path, out: Path) -> None:
    if not zpath.exists():
        return
    with zpath.open(encoding="utf-8") as f:
        z = json.load(f)
    keys = [
        ("sensor_overwrites", "Sensor overwrites"),
        ("buffer_high_watermark", "Buffer high watermark"),
        ("push_wait_count", "Push waits"),
        ("pop_wait_count", "Pop waits"),
    ]
    labels = [k[1] for k in keys]
    vals = [float(z.get(k[0], 0)) for k in keys]
    fig, ax = plt.subplots(figsize=(7, 4))
    zc = plt.get_cmap(P.heat_seq)(np.linspace(0.42, 0.88, len(labels)))
    ax.barh(labels, vals, color=zc)
    ax.set_xlabel("Count")
    fig.suptitle(
        f"Low-pressure run snapshot ({z.get('sensors', '?')} sensors, cap={z.get('buffer_capacity', '?')})"
    )
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def fig_p95_heatmap(rows: list[dict], out: Path, work_us: int = 0) -> None:
    raws = [unwrap_row(x) for x in rows]
    sel = [r for r in raws if r.get("consumer_work_us") == work_us]
    if not sel:
        return
    caps = sorted({r["capacity"] for r in sel})
    sleeps = sorted({r["reader_sleep_us"] for r in sel})
    mat = np.full((len(sleeps), len(caps)), np.nan)
    for r in sel:
        i = sleeps.index(r["reader_sleep_us"])
        j = caps.index(r["capacity"])
        mat[i, j] = max(r["p95_lag_us"], 1)

    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(
        mat, aspect="auto", cmap=P.heat_seq, norm=LogNorm(), origin="lower"
    )
    ax.set_xticks(np.arange(len(caps)))
    ax.set_xticklabels([str(c) for c in caps])
    ax.set_yticks(np.arange(len(sleeps)))
    ax.set_yticklabels([str(s) for s in sleeps])
    ax.set_xlabel("Buffer capacity")
    ax.set_ylabel("Reader sleep (µs)")
    fig.colorbar(im, ax=ax, label="P95 lag (µs)")
    fig.suptitle(f"P95 lag heatmap (consumer work = {work_us} µs)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def schematic_pipeline(out: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 2.2))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 3)
    ax.axis("off")

    def box(x, y, w, h, txt):
        r = mpatches.FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle="round,pad=0.03",
            ec=P.primary,
            fc=P.fill_soft,
            lw=1.5,
        )
        ax.add_patch(r)
        ax.text(
            x + w / 2,
            y + h / 2,
            txt,
            ha="center",
            va="center",
            fontsize=9,
            weight="bold",
            color=P.ink,
        )

    box(0.2, 0.9, 1.4, 1.2, "Sensors\n(ring)")
    box(2.1, 0.9, 1.6, 1.2, "Readers +\nGovernor")
    box(4.0, 0.9, 1.5, 1.2, "Student\nbuffer")
    box(5.8, 0.9, 1.4, 1.2, "Aggregation\nworkers")
    box(7.5, 0.9, 1.5, 1.2, "Storage\n(write→rename)")
    for x0, x1 in [(1.6, 2.1), (3.7, 4.0), (5.5, 5.8), (7.2, 7.5)]:
        ax.annotate(
            "",
            xy=(x1, 1.5),
            xytext=(x0, 1.5),
            arrowprops=dict(arrowstyle="->", lw=1.5, color=P.arrow),
        )
    fig.suptitle("Project Azure pipeline (conceptual)", y=0.98)
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    apply_theme()
    ap = argparse.ArgumentParser(description="Generate benchmark figures for docs/figures/")
    ap.add_argument("--bench-dir", type=Path, default=None)
    ap.add_argument("--out-dir", type=Path, default=None)
    args = ap.parse_args()
    root = project_root()
    bench = args.bench_dir or root / "data" / "bench"
    outd = args.out_dir or root / "docs" / "figures"
    outd.mkdir(parents=True, exist_ok=True)

    pareto = load_jsonl(bench / "pareto_sweep.jsonl")
    load_r = load_jsonl(bench / "load_sweep.jsonl")
    gov = load_jsonl(bench / "governor_pareto_sweep.jsonl")

    written = []
    if pareto:
        p = outd / "latency_tradeoff.png"
        fig_latency_tradeoff(pareto, p)
        written.append(p)
        p = outd / "capacity_backpressure.png"
        fig_capacity_backpressure(pareto, p)
        written.append(p)
        p = outd / "bench_rho_scatter.png"
        fig_rho_scatter(pareto, p)
        written.append(p)
        p = outd / "bench_watermark_heatmap.png"
        fig_watermark_heatmap(pareto, p, work_us=200)
        written.append(p)
        p = outd / "bench_push_waits_top.png"
        fig_push_waits(pareto, p)
        written.append(p)
        p = outd / "bench_p95_heatmap_work0.png"
        fig_p95_heatmap(pareto, p, work_us=0)
        written.append(p)
        p = outd / "bench_p95_heatmap_work200.png"
        fig_p95_heatmap(pareto, p, work_us=200)
        written.append(p)

    if load_r:
        p = outd / "load_scaling.png"
        fig_load_scaling(load_r, p)
        written.append(p)

    if gov:
        p_ab = outd / "governor_ab.png"
        fig_governor_ab(gov, p_ab)
        written.append(p_ab)
        p_cpu = outd / "governor_cpu_efficiency.png"
        fig_governor_cpu_efficiency(gov, p_cpu)
        if p_cpu.exists():
            written.append(p_cpu)
        p_ticks = outd / "bench_governor_mode_ticks.png"
        fig_governor_ticks(gov, p_ticks)
        written.append(p_ticks)

    trace = bench / "governor_trace.json"
    if trace.exists():
        p = outd / "governor_trace_sleep.png"
        fig_governor_trace(trace, p)
        written.append(p)

    z = bench / "zero_loss_report.json"
    if z.exists():
        p = outd / "zero_loss_summary.png"
        fig_zero_loss(z, p)
        written.append(p)

    p = outd / "pipeline.png"
    schematic_pipeline(p)
    written.append(p)

    print("Wrote", len(written), "figure(s):")
    for w in written:
        print(" ", w.relative_to(root))


if __name__ == "__main__":
    main()
