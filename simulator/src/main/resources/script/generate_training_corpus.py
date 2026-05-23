#!/usr/bin/env python3
"""
200-workflow training corpus generator for GNN scheduler training.

Generates:
  - 9 WfCommons recipes × 15 sizes = 135 (best effort; some recipes have
    base-graph minimum sizes that override smaller requests)
  - 65 chaotic random DAGs across 5 size × 5 chaos × 3 seed sweeps

Idempotent: skips any workflow XML that already exists in WORKFLOW_examples/.

Usage:
    python3 generate_training_corpus.py            # generates missing
    python3 generate_training_corpus.py --dry-run  # lists what would generate
    python3 generate_training_corpus.py --status   # report current corpus state
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR.parent / "demo" / "WORKFLOW_examples"

# Recipe → list of target task counts. The recipe's actual minimum may bump
# any small request upward (e.g. cycles' base graph is 69 tasks); we keep the
# requested label in the filename even when the recipe overruns.
WFCOMMONS_RECIPES = {
    "blast":       [50, 80, 120, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
    "bwa":         [50, 80, 120, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
    "cycles":      [70, 100, 130, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
    "epigenomics": [50, 80, 120, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
    "genome":      [60, 100, 130, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
    "montage":     [65, 100, 130, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
    "seismology":  [110, 130, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800, 900],
    "soykb":       [50, 80, 120, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
    "srasearch":   [50, 80, 120, 160, 200, 240, 280, 320, 360, 400, 450, 500, 600, 700, 800],
}

# Chaotic sweep — 5 sizes × 5 chaos × 3 seeds = 75, we'll truncate to 65.
CHAOTIC_SIZES   = [50, 100, 150, 200, 300]
CHAOTIC_CHAOS   = [0.10, 0.20, 0.30, 0.40, 0.50]
CHAOTIC_SEEDS   = [1, 2, 3]
CHAOTIC_TARGET  = 65


def wfcommons_target_filename(recipe: str, requested_tasks: int) -> str:
    """Mirrors generate_workflows.py / wfformat_to_dissect_converter.py."""
    return f"{recipe}_{requested_tasks}tasks_workflow_converted.xml"


def chaotic_target_filename(size: int, chaos: float, seed: int) -> str:
    chaos_pct = int(round(chaos * 100))
    return f"chaotic_n{size}_c{chaos_pct}_s{seed}.xml"


def generate_wfcommons_workflow(recipe: str, tasks: int, dry_run: bool) -> bool:
    target = OUT_DIR / wfcommons_target_filename(recipe, tasks)
    if target.exists():
        return False
    if dry_run:
        print(f"  [DRY] {target.name}")
        return True
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "generate_workflows.py"),
        "--type", recipe,
        "--tasks", str(tasks),
        "--convert",
    ]
    try:
        # The generate_workflows.py script chooses the filename itself based on
        # the recipe + requested size; it lands in WORKFLOW_examples/.
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            stderr_tail = (result.stderr or "")[-300:]
            print(f"  ⚠ {recipe}_{tasks}: failed ({result.returncode}) — {stderr_tail.strip()}")
            return False
    except subprocess.TimeoutExpired:
        print(f"  ⚠ {recipe}_{tasks}: timeout after 5 min")
        return False
    if target.exists():
        size_kb = target.stat().st_size // 1024
        print(f"  ✓ {target.name}  ({size_kb} KB)")
        return True
    print(f"  ⚠ {recipe}_{tasks}: generate_workflows.py finished but no output file")
    return False


def generate_chaotic_workflow(size: int, chaos: float, seed: int, dry_run: bool) -> bool:
    target = OUT_DIR / chaotic_target_filename(size, chaos, seed)
    if target.exists():
        return False
    if dry_run:
        print(f"  [DRY] {target.name}")
        return True
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "random_workflow_generator.py"),
        "--nodes", str(size),
        "--chaos", str(chaos),
        "--seed", str(seed),
        "--output", str(target),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            stderr_tail = (result.stderr or "")[-300:]
            print(f"  ⚠ chaotic n={size} c={chaos} s={seed}: failed — {stderr_tail.strip()}")
            return False
    except subprocess.TimeoutExpired:
        print(f"  ⚠ chaotic n={size} c={chaos} s={seed}: timeout")
        return False
    if target.exists():
        size_kb = target.stat().st_size // 1024
        print(f"  ✓ {target.name}  ({size_kb} KB)")
        return True
    return False


def report_status() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    xmls = sorted(OUT_DIR.glob("*.xml"))
    print(f"\n=== Corpus status: {len(xmls)} XML in {OUT_DIR} ===")
    by_kind = {"wfcommons": 0, "chaotic": 0, "iot": 0, "other": 0}
    for x in xmls:
        n = x.name
        if "_workflow_converted" in n:
            by_kind["wfcommons"] += 1
        elif n.startswith("chaotic_"):
            by_kind["chaotic"] += 1
        elif n.startswith("IoT_") or n.startswith("CyberShake"):
            by_kind["iot"] += 1
        else:
            by_kind["other"] += 1
    for k, v in by_kind.items():
        print(f"  {k:12s} {v}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="list what would be generated; don't run anything")
    parser.add_argument("--status", action="store_true",
                        help="just report current corpus state and exit")
    parser.add_argument("--skip-wfcommons", action="store_true",
                        help="skip WfCommons recipe generation")
    parser.add_argument("--skip-chaotic", action="store_true",
                        help="skip chaotic random-DAG generation")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.status:
        report_status()
        return

    print(f"Output directory: {OUT_DIR}")
    print(f"Mode: {'dry-run' if args.dry_run else 'generate'}")

    # ----- WfCommons recipes (135 target) -----
    if not args.skip_wfcommons:
        print("\n=== WfCommons recipes ===")
        wf_generated = 0
        wf_skipped = 0
        for recipe, sizes in WFCOMMONS_RECIPES.items():
            for n in sizes:
                if (OUT_DIR / wfcommons_target_filename(recipe, n)).exists():
                    wf_skipped += 1
                    continue
                if generate_wfcommons_workflow(recipe, n, args.dry_run):
                    wf_generated += 1
        print(f"  WfCommons: {wf_generated} generated, {wf_skipped} skipped (already exist)")

    # ----- Chaotic random DAGs (65 target) -----
    if not args.skip_chaotic:
        print("\n=== Chaotic random DAGs ===")
        chaotic_pairs = [(n, c, s) for n in CHAOTIC_SIZES
                                   for c in CHAOTIC_CHAOS
                                   for s in CHAOTIC_SEEDS][:CHAOTIC_TARGET]
        ch_generated = 0
        ch_skipped = 0
        for n, c, s in chaotic_pairs:
            if (OUT_DIR / chaotic_target_filename(n, c, s)).exists():
                ch_skipped += 1
                continue
            if generate_chaotic_workflow(n, c, s, args.dry_run):
                ch_generated += 1
        print(f"  Chaotic: {ch_generated} generated, {ch_skipped} skipped (already exist)")

    report_status()


if __name__ == "__main__":
    main()
