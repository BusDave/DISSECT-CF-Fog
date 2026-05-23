"""Visualize the adjacency matrix the GNN sees for a given workflow.

Loads a workflow XML, builds the same dense adjacency that
GnnGraphBuilder.buildWorkflowGraph() constructs at inference time,
and prints + plots it. Useful for the thesis: a visual demonstration
of the "list-based map → dense matrix" conversion the GNN needs.

Usage:
    python3 notebooks/visualize_adjacency.py <workflow.xml>

    # Or with default (CyberShake demo):
    python3 notebooks/visualize_adjacency.py
"""
from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def load_workflow_adjacency(xml_path: Path):
    """Parse a DISSECT-CF workflow XML and build its adjacency matrix.

    Returns:
        (task_ids, A, A_norm) where
          task_ids:  list[str] of task ids in declaration order
          A:         raw N×N adjacency (1 if parent→child edge, 0 else)
          A_norm:    symmetrically normalised D^{-1/2} (A+I) D^{-1/2}
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Task ids in declaration order
    task_ids = [job.attrib["id"] for job in root.findall("job")]
    idx = {tid: i for i, tid in enumerate(task_ids)}
    n = len(task_ids)

    A = np.zeros((n, n), dtype=np.float32)
    for job in root.findall("job"):
        src = job.attrib["id"]
        for uses in job.findall("uses"):
            if uses.attrib.get("link") == "output" and "id" in uses.attrib:
                dst = uses.attrib["id"]
                if dst in idx:                           # ignore non-task targets
                    A[idx[src], idx[dst]] = 1.0

    # Symmetric normalisation: A_norm = D^{-1/2} (A+I) D^{-1/2}
    A_with_self = A + np.eye(n, dtype=np.float32)
    deg = A_with_self.sum(axis=1)
    d_inv_sqrt = np.where(deg > 0, 1.0 / np.sqrt(deg), 0.0)
    D_inv_sqrt = np.diag(d_inv_sqrt)
    A_norm = D_inv_sqrt @ A_with_self @ D_inv_sqrt
    return task_ids, A, A_norm


def print_compact_matrix(name: str, M: np.ndarray, max_show: int = 12) -> None:
    """Print a small text snapshot of the matrix (top-left corner)."""
    n = M.shape[0]
    k = min(n, max_show)
    print(f"\n=== {name}  ({n}×{n}, showing top-left {k}×{k}) ===")
    for i in range(k):
        row = "  ".join(f"{v:5.2f}" for v in M[i, :k])
        print(f"  [{i:2d}] {row}")
    if n > k:
        print(f"  ... ({n - k} more rows × cols)")


def main() -> None:
    repo = Path(__file__).resolve().parent.parent
    default_xml = repo / "src/main/resources/demo/WORKFLOW_examples/IoT_CyberShake_100.xml"
    xml_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_xml

    if not xml_path.exists():
        raise SystemExit(f"missing: {xml_path}")

    task_ids, A, A_norm = load_workflow_adjacency(xml_path)
    n = len(task_ids)
    edges = int(A.sum())
    density = edges / (n * n) if n > 0 else 0
    print(f"loaded {xml_path.name}: {n} tasks, {edges} edges, "
          f"density = {density:.4f}")

    # ---- text print ----
    print_compact_matrix("raw A (1 = edge)", A, max_show=12)
    print_compact_matrix("A_norm = D^-1/2 (A+I) D^-1/2", A_norm, max_show=12)

    # ---- visualisation ----
    fig, axes = plt.subplots(1, 2, figsize=(11, 5))

    axes[0].imshow(A, cmap="Greys", aspect="auto")
    axes[0].set_title(f"Nyers adjacency A\n({n} feladat, {edges} él)")
    axes[0].set_xlabel("Utód-feladat indexe")
    axes[0].set_ylabel("Szülő-feladat indexe")

    im = axes[1].imshow(A_norm, cmap="viridis", aspect="auto")
    axes[1].set_title(r"Normalizált $\hat{A} = D^{-1/2}(A+I)D^{-1/2}$")
    axes[1].set_xlabel("Utód-feladat indexe")
    axes[1].set_ylabel("Szülő-feladat indexe")
    plt.colorbar(im, ax=axes[1], fraction=0.046, pad=0.04)

    fig.suptitle(f"GNN adjacency mátrix — {xml_path.stem}")
    fig.tight_layout()

    out_png = repo / "sim_res" / f"adjacency_{xml_path.stem}.png"
    out_pdf = repo / "sim_res" / f"adjacency_{xml_path.stem}.pdf"
    fig.savefig(out_png, dpi=150)
    fig.savefig(out_pdf)
    print(f"\nwrote {out_png}\nwrote {out_pdf}")


if __name__ == "__main__":
    main()
