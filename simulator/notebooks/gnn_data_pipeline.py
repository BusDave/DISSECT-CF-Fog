"""GNN data pipeline: reconstruct workflow + cluster graphs from training CSVs.

Loaded by train_gnn.ipynb. Each scheduling decision in a CSV becomes a graph
sample with:
  - workflow node features (per-task: runtime, rank_up, in/out-deg, input_bytes,
    is_current indicator)
  - workflow edges (parent → child from XML's <uses> elements)
  - cluster node features (per-node: cpu, ram, bandwidth, geo, queue, running,
    is_cloud indicator)
  - cluster edges (fully-connected, edge weight = inverse haversine distance)
  - current_task_idx (which workflow node is being scheduled right now)
  - label (chosen cluster node, 0..19)
  - sample_weight (= 1/makespan_seconds)

The same XML may appear in many CSV rows; we cache parsed DAGs to keep
training data-loading fast.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import torch


# =============================================================================
#  Cluster topology (mirrors TrainingSetup.java)
# =============================================================================

# (name, kind, lat, lon)  — must match TrainingSetup.buildNodes() order.
CLUSTER_NODES: List[Tuple[str, str, float, float]] = [
    ("node0",  "cloud", 48.8566,  2.3522),    # Paris
    ("node1",  "fog",   51.5074, -0.1278),    # London
    ("node2",  "edge",  52.5200, 13.4050),    # Berlin (4-core)
    ("node3",  "fog",   41.9028, 12.4964),    # Rome
    ("node4",  "edge",  41.0082, 28.9784),    # Istanbul
    ("node5",  "fog",   43.7102,  7.2620),    # Nice
    ("node6",  "fog",   55.6761, 12.5683),    # Copenhagen
    ("node7",  "edge",  59.3293, 18.0686),    # Stockholm
    ("node8",  "fog",   48.2082, 16.3738),    # Vienna
    ("node9",  "fog",   50.8503,  4.3517),    # Brussels
    ("node10", "fog",   46.7762, 23.6213),    # Cluj-Napoca
    ("node11", "cloud", 48.1351, 11.5820),    # Munich
    ("node12", "fog",   53.9076, 27.5754),    # Minsk
    ("node13", "fog",   60.1695, 24.9354),    # Helsinki
    ("node14", "fog",   39.9334, 32.8597),    # Ankara
    ("node15", "edge",  40.4168, -3.7038),    # Madrid
    ("node16", "fog",   37.9838, 23.7275),    # Athens
    ("node17", "fog",   52.3702,  4.8952),    # Amsterdam
    ("node18", "edge",  55.9533, -3.1883),    # Edinburgh
    ("node19", "cloud", 51.1657, 10.4515),    # Germany
]

# Hardware spec by kind (matches the XML configs ELKH_original.xml / LPDS_16.xml).
CPU_BY_KIND       = {"cloud": 52, "fog": 16, "edge":  4}
RAM_GB_BY_KIND    = {"cloud": 64, "fog": 32, "edge":  4}
BW_MBPS_BY_KIND   = {"cloud": 1000, "fog": 500, "edge": 100}
NUM_CLUSTER_NODES = len(CLUSTER_NODES)
assert NUM_CLUSTER_NODES == 20


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def build_static_cluster_features() -> np.ndarray:
    """[N_nodes, 6] static cluster features (cpu, ram, bandwidth, lat, lon, is_cloud).
    The DYNAMIC queue/running columns come from the CSV per-row."""
    feats = np.zeros((NUM_CLUSTER_NODES, 6), dtype=np.float32)
    for i, (_, kind, lat, lon) in enumerate(CLUSTER_NODES):
        feats[i, 0] = math.log1p(CPU_BY_KIND[kind])
        feats[i, 1] = math.log1p(RAM_GB_BY_KIND[kind])
        feats[i, 2] = math.log1p(BW_MBPS_BY_KIND[kind])
        feats[i, 3] = lat / 90.0
        feats[i, 4] = lon / 180.0
        feats[i, 5] = 1.0 if kind == "cloud" else 0.0
    return feats


def build_cluster_adjacency() -> np.ndarray:
    """Fully-connected weighted adjacency, weights = 1 / (1 + haversine_km / 1000).
    Returns the **symmetrically normalized** matrix D^{-1/2} (A + I) D^{-1/2}
    ready to be multiplied with node features for one GNN message-passing step."""
    n = NUM_CLUSTER_NODES
    a = np.zeros((n, n), dtype=np.float32)
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            d_km = _haversine_km(CLUSTER_NODES[i][2], CLUSTER_NODES[i][3],
                                 CLUSTER_NODES[j][2], CLUSTER_NODES[j][3])
            a[i, j] = 1.0 / (1.0 + d_km / 1000.0)
    return normalize_adjacency(a)


def normalize_adjacency(a: np.ndarray) -> np.ndarray:
    """Add self-loops, then symmetric normalization."""
    n = a.shape[0]
    a_hat = a + np.eye(n, dtype=a.dtype)
    deg = a_hat.sum(axis=1)
    deg_inv_sqrt = 1.0 / np.sqrt(np.maximum(deg, 1e-9))
    d_mat = np.diag(deg_inv_sqrt)
    return (d_mat @ a_hat @ d_mat).astype(np.float32)


# =============================================================================
#  Workflow DAG parsing (one parse per unique XML, cached)
# =============================================================================

@dataclass
class WorkflowDag:
    """Cached parsed-XML representation."""
    task_ids:        List[str]
    task_id_to_idx:  Dict[str, int]
    runtime:         np.ndarray              # [N] seconds
    input_bytes:     np.ndarray              # [N] sum of data-input sizes
    output_bytes:    np.ndarray              # [N] sum of data-output sizes
    edge_index:      np.ndarray              # [2, E] parent_idx → child_idx
    edge_bytes:      np.ndarray              # [E] data size per edge
    rank_up:         np.ndarray              # [N] HEFT upward-rank
    in_degree:       np.ndarray              # [N]
    out_degree:      np.ndarray              # [N]
    adj_norm:        np.ndarray              # [N, N] symmetrically normalized


_DAG_CACHE: Dict[str, WorkflowDag] = {}


def parse_workflow_xml(xml_path: str) -> WorkflowDag:
    """Parse the DISSECT-CF XML schema and compute HEFT rank_up. Cached per-path."""
    if xml_path in _DAG_CACHE:
        return _DAG_CACHE[xml_path]

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Two flavors: <adag><job>...  and <workflow><adag><job>...
    jobs = list(root.iter("job"))
    if not jobs:
        raise ValueError(f"No <job> elements in {xml_path}")

    task_ids = [j.get("id") for j in jobs]
    task_id_to_idx = {tid: i for i, tid in enumerate(task_ids)}
    n = len(task_ids)

    runtime     = np.zeros(n, dtype=np.float32)
    input_bytes = np.zeros(n, dtype=np.float32)
    output_bytes = np.zeros(n, dtype=np.float32)
    edges: List[Tuple[int, int]] = []
    edge_sz: List[float] = []

    for idx, job in enumerate(jobs):
        try:
            runtime[idx] = float(job.get("runtime", "0"))
        except (TypeError, ValueError):
            runtime[idx] = 0.0

        for uses in job.findall("uses"):
            link = uses.get("link", "")
            utype = uses.get("type", "")
            size = uses.get("size")
            ref_id = uses.get("id")
            try:
                size_f = float(size) if size is not None else 0.0
            except ValueError:
                size_f = 0.0
            if utype == "data":
                if link == "input":
                    input_bytes[idx] += size_f
                elif link == "output":
                    output_bytes[idx] += size_f
                    if ref_id and ref_id in task_id_to_idx:
                        child = task_id_to_idx[ref_id]
                        edges.append((idx, child))
                        edge_sz.append(size_f)

    edge_index = (np.asarray(edges, dtype=np.int64).T
                  if edges else np.zeros((2, 0), dtype=np.int64))
    edge_bytes = np.asarray(edge_sz, dtype=np.float32)

    in_degree  = np.zeros(n, dtype=np.float32)
    out_degree = np.zeros(n, dtype=np.float32)
    if edges:
        for p, c in edges:
            out_degree[p] += 1
            in_degree[c]  += 1

    rank_up = _compute_heft_rank_up(n, runtime, edges, edge_sz)
    adj_norm = _build_directed_adjacency(n, edges)

    dag = WorkflowDag(
        task_ids=task_ids,
        task_id_to_idx=task_id_to_idx,
        runtime=runtime,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        edge_index=edge_index,
        edge_bytes=edge_bytes,
        rank_up=rank_up,
        in_degree=in_degree,
        out_degree=out_degree,
        adj_norm=adj_norm,
    )
    _DAG_CACHE[xml_path] = dag
    return dag


def _compute_heft_rank_up(n: int, runtime: np.ndarray,
                          edges: List[Tuple[int, int]],
                          edge_sz: List[float]) -> np.ndarray:
    """rank_up(t) = w(t) + max{ c(t,succ) + rank_up(succ) }, recursive.
    We use a single "average computation cost" (= runtime) since the recipe
    files quote one reference time; the actual heterogeneity is encoded in
    the cluster features. Comm cost = data_bytes / 62.5 MB/s.
    """
    children: List[List[Tuple[int, float]]] = [[] for _ in range(n)]
    for (p, c), sz in zip(edges, edge_sz):
        children[p].append((c, sz / 62_500_000.0))  # seconds

    rank = np.zeros(n, dtype=np.float32)
    visited = np.zeros(n, dtype=bool)

    def recurse(i: int) -> float:
        if visited[i]:
            return rank[i]
        m = 0.0
        for c, ccost in children[i]:
            v = ccost + recurse(c)
            if v > m:
                m = v
        rank[i] = runtime[i] + m
        visited[i] = True
        return rank[i]

    for i in range(n):
        recurse(i)
    return rank


def _build_directed_adjacency(n: int, edges: List[Tuple[int, int]]) -> np.ndarray:
    """Workflow adjacency: edge p→c gets weight 1.0. Symmetric normalize for GNN
    (so message passing aggregates from both parents and children — that's
    the standard GCN trick for directed graphs)."""
    a = np.zeros((n, n), dtype=np.float32)
    for p, c in edges:
        a[p, c] = 1.0
        a[c, p] = 1.0      # treat as undirected for GNN aggregation
    return normalize_adjacency(a)


# =============================================================================
#  CSV + sidecar → training sample
# =============================================================================

NODE_QUEUE_COLS   = [f"node{i}_queue"   for i in range(NUM_CLUSTER_NODES)]
NODE_RUNNING_COLS = [f"node{i}_running" for i in range(NUM_CLUSTER_NODES)]


def _resolve_task_index(raw_task_id: str, id_to_idx: Dict[str, int]) -> int:
    """Match a CSV task_id (e.g. '0_ID00002_0') against the XML's job ids
    ('ID00002'). The simulator prefixes the cluster index and appends an
    iteration suffix, so we try the direct match first, then progressively
    strip leading '<digits>_' and trailing '_<digits>'."""
    if raw_task_id in id_to_idx:
        return id_to_idx[raw_task_id]
    stripped = re.sub(r"^\d+_", "", raw_task_id)
    if stripped in id_to_idx:
        return id_to_idx[stripped]
    stripped2 = re.sub(r"_\d+$", "", stripped)
    if stripped2 in id_to_idx:
        return id_to_idx[stripped2]
    # Last resort: search for a substring match (cheap because the XML's
    # original id is usually a unique alphanumeric token like 'ID00002').
    for xml_id, idx in id_to_idx.items():
        if xml_id and xml_id in raw_task_id:
            return idx
    return -1


@dataclass
class GraphSample:
    """One scheduling decision, ready for the GNN."""
    wf_node_features: np.ndarray       # [N_t, 6]  float32
    wf_adj_norm:      np.ndarray       # [N_t, N_t] float32
    cl_node_features: np.ndarray       # [N_n, 8] float32
    cl_adj_norm:      np.ndarray       # [N_n, N_n] float32
    current_task_idx: int
    label:            int
    sample_weight:    float
    source_run:       str = ""         # CSV stem, used for grouped train/val split
    workflow_name:    str = ""         # derived from XML path or appName


def _workflow_node_features(dag: WorkflowDag, current_idx: int) -> np.ndarray:
    """Build the [N_t, 6] feature matrix:
       0: runtime (log1p sec)
       1: rank_up (log1p)
       2: in_degree (normalized)
       3: out_degree (normalized)
       4: total_input_bytes (log1p)
       5: is_current (1.0 for current_idx, 0 else)
    """
    n = len(dag.task_ids)
    f = np.zeros((n, 6), dtype=np.float32)
    f[:, 0] = np.log1p(dag.runtime)
    f[:, 1] = np.log1p(np.maximum(dag.rank_up, 0))
    max_in  = max(dag.in_degree.max(),  1.0)
    max_out = max(dag.out_degree.max(), 1.0)
    f[:, 2] = dag.in_degree  / max_in
    f[:, 3] = dag.out_degree / max_out
    f[:, 4] = np.log1p(dag.input_bytes)
    if 0 <= current_idx < n:
        f[current_idx, 5] = 1.0
    return f


def _cluster_node_features(static_feats: np.ndarray,
                           queue_sizes: np.ndarray,
                           running:     np.ndarray) -> np.ndarray:
    """[N_n, 8] = [static 6 cols] + [queue_size, running] both log1p."""
    n = static_feats.shape[0]
    f = np.zeros((n, 8), dtype=np.float32)
    f[:, :6] = static_feats
    f[:, 6]  = np.log1p(np.maximum(queue_sizes, 0))
    f[:, 7]  = np.log1p(np.maximum(running, 0))
    return f


def load_training_samples(training_dir: Path,
                           keep_schedulers: Tuple[str, ...] = ("heft", "heftds"),
                           verbose: bool = True) -> List[GraphSample]:
    """Scan training_dir for *.meta.json + *.csv pairs, parse the XMLs, and
    build one GraphSample per CSV row.
    Returns a list ready for a DataLoader."""
    cluster_static  = build_static_cluster_features()
    cluster_adj_norm = build_cluster_adjacency()

    samples: List[GraphSample] = []
    skipped_no_xml = 0
    skipped_other = 0

    meta_files = sorted(training_dir.glob("*.meta.json"))
    if verbose:
        print(f"scanning {len(meta_files)} sidecar JSONs in {training_dir}")

    for meta_path in meta_files:
        try:
            meta = json.loads(meta_path.read_text())
        except json.JSONDecodeError:
            skipped_other += 1
            continue
        scheduler = meta.get("scheduler", "")
        if scheduler not in keep_schedulers:
            continue

        ms = float(meta.get("makespan_seconds", 0.0))
        if ms <= 0:
            skipped_other += 1
            continue
        sample_weight = 1.0 / ms

        xml_path = meta.get("workflow_xml_path")
        if not xml_path or not Path(xml_path).exists():
            skipped_no_xml += 1
            continue

        csv_file = meta.get("csv_file") or (meta_path.stem.replace(".meta", "") + ".csv")
        csv_path = training_dir / csv_file
        if not csv_path.exists():
            skipped_other += 1
            continue

        try:
            dag = parse_workflow_xml(xml_path)
        except Exception as e:
            if verbose:
                print(f"  ⚠ failed to parse {Path(xml_path).name}: {e}")
            skipped_other += 1
            continue

        try:
            df = pd.read_csv(csv_path)
        except Exception:
            skipped_other += 1
            continue

        source_run = csv_path.stem
        wf_name = Path(xml_path).stem
        for _, row in df.iterrows():
            raw_task_id = str(row["task_id"])
            current_idx = _resolve_task_index(raw_task_id, dag.task_id_to_idx)
            if current_idx < 0:
                continue

            label = int(row["chosen_node_idx"])
            if label < 0 or label >= NUM_CLUSTER_NODES:
                continue

            queue_sizes = np.asarray([row[col] for col in NODE_QUEUE_COLS],   dtype=np.float32)
            running     = np.asarray([row[col] for col in NODE_RUNNING_COLS], dtype=np.float32)

            samples.append(GraphSample(
                wf_node_features=_workflow_node_features(dag, current_idx),
                wf_adj_norm=dag.adj_norm,
                cl_node_features=_cluster_node_features(cluster_static, queue_sizes, running),
                cl_adj_norm=cluster_adj_norm,
                current_task_idx=current_idx,
                label=label,
                sample_weight=sample_weight,
                source_run=source_run,
                workflow_name=wf_name,
            ))

    if verbose:
        print(f"loaded {len(samples)} graph samples")
        if skipped_no_xml:
            print(f"  ⚠ {skipped_no_xml} sidecars had no workflow_xml_path or missing XML")
        if skipped_other:
            print(f"  ⚠ {skipped_other} sidecars/csv pairs skipped (other errors)")
    return samples


# =============================================================================
#  Custom collate: variable-sized graphs → batch via block-diagonal adj
# =============================================================================

def collate_graph_batch(batch: List[GraphSample]) -> Dict[str, torch.Tensor]:
    """Per-sample variable graph sizes → padded batch.
    Pads all workflow DAGs in the batch to the max size and zero-fills.
    Cluster size is fixed (20), no padding needed.
    """
    bs = len(batch)
    max_t = max(s.wf_node_features.shape[0] for s in batch)
    n_n = batch[0].cl_node_features.shape[0]

    wf_feat = np.zeros((bs, max_t, 6),       dtype=np.float32)
    wf_adj  = np.zeros((bs, max_t, max_t),   dtype=np.float32)
    cl_feat = np.zeros((bs, n_n, 8),         dtype=np.float32)
    cl_adj  = np.zeros((bs, n_n, n_n),       dtype=np.float32)
    cur_idx = np.zeros(bs,                   dtype=np.int64)
    labels  = np.zeros(bs,                   dtype=np.int64)
    weights = np.zeros(bs,                   dtype=np.float32)
    wf_mask = np.zeros((bs, max_t),          dtype=np.float32)

    for i, s in enumerate(batch):
        n_t = s.wf_node_features.shape[0]
        wf_feat[i, :n_t]            = s.wf_node_features
        wf_adj[i,  :n_t, :n_t]      = s.wf_adj_norm
        cl_feat[i]                  = s.cl_node_features
        cl_adj[i]                   = s.cl_adj_norm
        cur_idx[i]                  = s.current_task_idx
        labels[i]                   = s.label
        weights[i]                  = s.sample_weight
        wf_mask[i, :n_t]            = 1.0

    return {
        "wf_node_features": torch.from_numpy(wf_feat),
        "wf_adj_norm":      torch.from_numpy(wf_adj),
        "wf_mask":          torch.from_numpy(wf_mask),
        "cl_node_features": torch.from_numpy(cl_feat),
        "cl_adj_norm":      torch.from_numpy(cl_adj),
        "current_task_idx": torch.from_numpy(cur_idx),
        "labels":           torch.from_numpy(labels),
        "sample_weights":   torch.from_numpy(weights),
    }
