#!/usr/bin/env python3
"""Summarise SchedulerBenchmarkRunner output: per-workflow tables + plots.

Reads sim_res/benchmark_results.csv (9 columns, one row per run).
Writes:
  sim_res/benchmark_summary.csv      — mean ± std per (workflow, scheduler)
  sim_res/benchmark_makespan.png     — bar chart with error bars
  sim_res/benchmark_energy.png       — bar chart with error bars
  sim_res/benchmark_speedup.png      — NeuralScheduler vs HEFT speedup scatter
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

SCHEDULERS = ["heft", "heftds", "maxmin", "adaptive", "neural"]
COLOURS    = {"heft": "#4c72b0", "heftds": "#dd8452",
              "maxmin": "#c44e52", "adaptive": "#8172b3", "neural": "#55a868"}


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
        neural_pct=("neural_decisions", lambda s: 100 * s.sum()
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
        neural_pct=("neural_decisions", lambda s: 100 * s.sum()
                    / (s.sum() + df.loc[s.index, "fallback_decisions"].sum() + 1e-9)),
        avg_inf_ms=("avg_inference_ms", "mean"),
    )
    print(overall.to_string(float_format=lambda x: f"{x:.3f}"))

    # ---------- head-to-head: neural speedup over HEFT ----------
    print("\n=== head-to-head: NeuralScheduler vs HEFT (per workflow) ===")
    pivot_ms = df.pivot_table(index="workflow", columns="scheduler",
                              values="makespan_sec", aggfunc="mean")
    pivot_e = df.pivot_table(index="workflow", columns="scheduler",
                             values="energy_kwh", aggfunc="mean")
    if "neural" in pivot_ms and "heft" in pivot_ms:
        cmp = pd.DataFrame({
            "heft_ms": pivot_ms["heft"],
            "neural_ms": pivot_ms["neural"],
            "ms_speedup_pct": 100 * (pivot_ms["heft"] - pivot_ms["neural"]) / pivot_ms["heft"],
            "heft_kwh": pivot_e["heft"],
            "neural_kwh": pivot_e["neural"],
            "kwh_savings_pct": 100 * (pivot_e["heft"] - pivot_e["neural"]) / pivot_e["heft"],
        })
        print(cmp.to_string(float_format=lambda x: f"{x:.2f}"))
        print(f"\nneural wins on makespan in {(cmp.ms_speedup_pct > 0).sum()}/{len(cmp)} workflows")
        print(f"neural wins on energy   in {(cmp.kwh_savings_pct > 0).sum()}/{len(cmp)} workflows")

    if not HAS_PLT:
        return

    workflows = sorted(df.workflow.unique())
    x = np.arange(len(workflows))
    width = 0.17

    # ---------- bar chart: makespan ----------
    fig, ax = plt.subplots(figsize=(max(8, 0.8 * len(workflows)), 5))
    for i, sched in enumerate(SCHEDULERS):
        means, stds = [], []
        for wf in workflows:
            sub = df[(df.workflow == wf) & (df.scheduler == sched)]
            means.append(sub.makespan_sec.mean() if len(sub) else 0)
            stds.append(sub.makespan_sec.std() if len(sub) > 1 else 0)
        ax.bar(x + (i - 2) * width, means, width, yerr=stds, capsize=3,
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
        ax.bar(x + (i - 2) * width, means, width, yerr=stds, capsize=3,
               label=sched.upper(), color=COLOURS[sched])
    ax.set_xticks(x)
    ax.set_xticklabels([_short(w) for w in workflows], rotation=45, ha="right")
    ax.set_ylabel("energy (kWh)")
    ax.set_title("Energy per workflow (mean ± std over 3 seeds)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "benchmark_energy.png", dpi=120)
    print(f"wrote {OUT_DIR / 'benchmark_energy.png'}")

    # ---------- scatter: neural speedup vs HEFT ----------
    if "neural" in pivot_ms and "heft" in pivot_ms:
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.scatter(pivot_ms["heft"], pivot_ms["neural"], color=COLOURS["neural"], s=60)
        lim = max(pivot_ms["heft"].max(), pivot_ms["neural"].max()) * 1.05
        ax.plot([0, lim], [0, lim], "k--", alpha=0.5, label="parity")
        for wf in pivot_ms.index:
            ax.annotate(_short(wf),
                        (pivot_ms.loc[wf, "heft"], pivot_ms.loc[wf, "neural"]),
                        fontsize=8, alpha=0.7)
        ax.set_xlabel("HEFT makespan (s)")
        ax.set_ylabel("NeuralScheduler makespan (s)")
        ax.set_title("NeuralScheduler vs HEFT — below the line = faster")
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
