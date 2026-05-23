#!/usr/bin/env python3
"""Summarise SchedulerBenchmarkRunner output: per-workflow tables + plots.

Reads sim_res/benchmark_results.csv (9 columns, one row per run).
Writes:
  sim_res/benchmark_summary.csv      — mean ± std per (workflow, scheduler)
  sim_res/benchmark_makespan.png     — bar chart with error bars
  sim_res/benchmark_energy.png       — bar chart with error bars
  sim_res/benchmark_speedup.png      — GnnScheduler vs HEFT speedup scatter
  sim_res/benchmark_slr.png/.pdf     — normalised SLR bar chart (HEFT = 1.0)
"""
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt
    HAS_PLT = True
except ImportError:                                  # pragma: no cover
    print("matplotlib not installed; skipping plots (pip install matplotlib)")
    HAS_PLT = False

REPO_ROOT = Path(__file__).resolve().parent.parent   # simulator/
CSV_IN    = REPO_ROOT / "sim_res" / "benchmark_results.csv"
OUT_DIR   = REPO_ROOT / "sim_res"

SCHEDULERS = ["heft", "heftds", "maxmin", "adaptive", "gnn"]
COLOURS    = {"heft": "#4c72b0", "heftds": "#dd8452",
              "maxmin": "#c44e52", "adaptive": "#8172b3",
              "gnn": "#55a868"}


def main() -> None:
    if not CSV_IN.exists():
        raise SystemExit(f"missing {CSV_IN} — run SchedulerBenchmarkRunner first")

    df = pd.read_csv(CSV_IN)
    print(f"loaded {len(df)} rows, {df.workflow.nunique()} workflows, "
          f"{df.scheduler.nunique()} schedulers, {df.seed.nunique()} seeds")

    # ---------- per (workflow, scheduler) summary table ----------
    agg = df.groupby(["workflow", "scheduler"]).agg(
        makespan_mean=("makespan_sec", "mean"),
        makespan_std=("makespan_sec", "std"),
        energy_mean=("energy_kwh", "mean"),
        energy_std=("energy_kwh", "std"),
        gnn_pct=("gnn_decisions", lambda s: 100 * s.sum()
                 / (s.sum() + df.loc[s.index, "fallback_decisions"].sum() + 1e-9)),
        avg_inf_ms=("avg_inference_ms", "mean"),
    ).reset_index()
    agg.to_csv(OUT_DIR / "benchmark_summary.csv", index=False)
    print(f"\nwrote {OUT_DIR / 'benchmark_summary.csv'}")

    # ---------- aggregate over all workflows ----------
    print("\n=== aggregate (mean over all workflows × seeds) ===")
    overall = df.groupby("scheduler").agg(
        makespan_mean=("makespan_sec", "mean"),
        makespan_median=("makespan_sec", "median"),
        energy_mean=("energy_kwh", "mean"),
        gnn_pct=("gnn_decisions", lambda s: 100 * s.sum()
                 / (s.sum() + df.loc[s.index, "fallback_decisions"].sum() + 1e-9)),
        avg_inf_ms=("avg_inference_ms", "mean"),
    )
    print(overall.to_string(float_format=lambda x: f"{x:.3f}"))

    # ---------- head-to-head: GNN speedup over HEFT ----------
    print("\n=== head-to-head: GnnScheduler vs HEFT (per workflow) ===")
    pivot_ms = df.pivot_table(index="workflow", columns="scheduler",
                              values="makespan_sec", aggfunc="mean")
    pivot_e = df.pivot_table(index="workflow", columns="scheduler",
                             values="energy_kwh", aggfunc="mean")
    if "gnn" in pivot_ms and "heft" in pivot_ms:
        cmp = pd.DataFrame({
            "heft_ms": pivot_ms["heft"],
            "gnn_ms": pivot_ms["gnn"],
            "ms_speedup_pct": 100 * (pivot_ms["heft"] - pivot_ms["gnn"]) / pivot_ms["heft"],
            "heft_kwh": pivot_e["heft"],
            "gnn_kwh": pivot_e["gnn"],
            "kwh_savings_pct": 100 * (pivot_e["heft"] - pivot_e["gnn"]) / pivot_e["heft"],
        })
        print(cmp.to_string(float_format=lambda x: f"{x:.2f}"))
        print(f"\nGNN wins on makespan in {(cmp.ms_speedup_pct > 0).sum()}/{len(cmp)} workflows")
        print(f"GNN wins on energy   in {(cmp.kwh_savings_pct > 0).sum()}/{len(cmp)} workflows")

    if not HAS_PLT:
        return

    workflows = sorted(df.workflow.unique())
    x = np.arange(len(workflows))
    width = 0.14

    # ---------- bar chart: makespan ----------
    fig, ax = plt.subplots(figsize=(max(8, 0.8 * len(workflows)), 5))
    for i, sched in enumerate(SCHEDULERS):
        means, stds = [], []
        for wf in workflows:
            sub = df[(df.workflow == wf) & (df.scheduler == sched)]
            means.append(sub.makespan_sec.mean() if len(sub) else 0)
            stds.append(sub.makespan_sec.std() if len(sub) > 1 else 0)
        ax.bar(x + (i - 2.5) * width, means, width, yerr=stds, capsize=3,
               label=sched.upper(), color=COLOURS[sched])
    ax.set_xticks(x)
    ax.set_xticklabels([_short(w) for w in workflows], rotation=45, ha="right")
    ax.set_ylabel("makespan (s)")
    ax.set_title("Makespan per workflow (mean ± std over 3 seeds)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "benchmark_makespan.png", dpi=120)
    print(f"\nwrote {OUT_DIR / 'benchmark_makespan.png'}")

    # ---------- bar chart: energy ----------
    fig, ax = plt.subplots(figsize=(max(8, 0.8 * len(workflows)), 5))
    for i, sched in enumerate(SCHEDULERS):
        means, stds = [], []
        for wf in workflows:
            sub = df[(df.workflow == wf) & (df.scheduler == sched)]
            means.append(sub.energy_kwh.mean() if len(sub) else 0)
            stds.append(sub.energy_kwh.std() if len(sub) > 1 else 0)
        ax.bar(x + (i - 2.5) * width, means, width, yerr=stds, capsize=3,
               label=sched.upper(), color=COLOURS[sched])
    ax.set_xticks(x)
    ax.set_xticklabels([_short(w) for w in workflows], rotation=45, ha="right")
    ax.set_ylabel("energy (kWh)")
    ax.set_title("Energy per workflow (mean ± std over 3 seeds)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "benchmark_energy.png", dpi=120)
    print(f"wrote {OUT_DIR / 'benchmark_energy.png'}")

    # ---------- normalised SLR bar chart (Sandokji-style) ----------
    #  Each workflow's HEFT makespan = 1.0 baseline; other schedulers'
    #  bars are their makespan / HEFT_makespan ratio. Values < 1.0 = faster
    #  than HEFT, > 1.0 = slower. Reads on the same axis regardless of
    #  workflow size, unlike the raw makespan plot.
    fig, ax = plt.subplots(figsize=(max(9, 0.85 * len(workflows)), 5))
    heft_baseline = {}
    for wf in workflows:
        sub = df[(df.workflow == wf) & (df.scheduler == "heft")]
        heft_baseline[wf] = sub.makespan_sec.mean() if len(sub) else 1.0

    n_sched = len(SCHEDULERS)
    for i, sched in enumerate(SCHEDULERS):
        ratios = []
        for wf in workflows:
            sub = df[(df.workflow == wf) & (df.scheduler == sched)]
            ms = sub.makespan_sec.mean() if len(sub) else 0
            base = heft_baseline[wf] if heft_baseline[wf] > 0 else 1.0
            ratios.append(ms / base)
        ax.bar(x + (i - (n_sched - 1) / 2) * width, ratios, width,
               label=sched.upper(), color=COLOURS[sched],
               edgecolor="black", linewidth=0.4)

    ax.axhline(1.0, color="black", linestyle="--", linewidth=0.8, alpha=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels([_short(w) for w in workflows], rotation=45, ha="right")
    ax.set_ylabel("Normalizált makespan (HEFT = 1.0)")
    ax.set_title("Normalizált makespan ütemezőnként\n(érték < 1.0: gyorsabb mint HEFT)")
    ax.legend(loc="upper right", ncol=2, fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "benchmark_slr.png", dpi=150)
    fig.savefig(OUT_DIR / "benchmark_slr.pdf")
    print(f"wrote {OUT_DIR / 'benchmark_slr.png'} + .pdf")

    # ---------- makespan ↔ energy correlation scatter ----------
    #  Every run is a single (makespan, energy) point; we colour by
    #  scheduler so the per-scheduler clouds are visible, fit a single
    #  global linear regression line across all points, and annotate
    #  the R² to validate the "energy is dominated by makespan" claim.
    fig, ax = plt.subplots(figsize=(7, 5))
    for sched in SCHEDULERS:
        sub = df[df.scheduler == sched]
        ax.scatter(sub.makespan_sec, sub.energy_kwh,
                   color=COLOURS[sched], s=40, alpha=0.7,
                   edgecolor="black", linewidth=0.3,
                   label=sched.upper())

    # Linear fit on all points
    coeffs = np.polyfit(df.makespan_sec, df.energy_kwh, 1)
    xs = np.array([df.makespan_sec.min(), df.makespan_sec.max()])
    ax.plot(xs, np.polyval(coeffs, xs), "k--", alpha=0.5,
            label="lineáris illesztés")

    # R² of the global correlation
    r2 = np.corrcoef(df.makespan_sec, df.energy_kwh)[0, 1] ** 2
    ax.text(0.05, 0.95, f"$R^2 = {r2:.3f}$", transform=ax.transAxes,
            fontsize=12, verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.85))

    ax.set_xlabel("Makespan [s]")
    ax.set_ylabel("Energia [kWh]")
    ax.set_title("Makespan és energiafogyasztás korrelációja\n"
                 "(minden pont egy szimulációs futás)")
    ax.legend(loc="lower right", fontsize=9, ncol=2)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "benchmark_correlation.png", dpi=150)
    fig.savefig(OUT_DIR / "benchmark_correlation.pdf")
    print(f"wrote {OUT_DIR / 'benchmark_correlation.png'} + .pdf  (R²={r2:.3f})")

    # ---------- scatter: GNN speedup vs HEFT ----------
    if "gnn" in pivot_ms and "heft" in pivot_ms:
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.scatter(pivot_ms["heft"], pivot_ms["gnn"], color=COLOURS["gnn"], s=60)
        lim = max(pivot_ms["heft"].max(), pivot_ms["gnn"].max()) * 1.05
        ax.plot([0, lim], [0, lim], "k--", alpha=0.5, label="parity")
        for wf in pivot_ms.index:
            ax.annotate(_short(wf),
                        (pivot_ms.loc[wf, "heft"], pivot_ms.loc[wf, "gnn"]),
                        fontsize=8, alpha=0.7)
        ax.set_xlabel("HEFT makespan (s)")
        ax.set_ylabel("GnnScheduler makespan (s)")
        ax.set_title("GnnScheduler vs HEFT — below the line = faster")
        ax.legend()
        fig.tight_layout()
        fig.savefig(OUT_DIR / "benchmark_speedup.png", dpi=120)
        print(f"wrote {OUT_DIR / 'benchmark_speedup.png'}")


def _short(name: str) -> str:
    """Trim verbose suffixes so x-axis labels stay readable."""
    return (name.replace("_workflow_converted", "")
                .replace(".xml", "")
                .replace("tasks", "t"))


if __name__ == "__main__":
    main()
