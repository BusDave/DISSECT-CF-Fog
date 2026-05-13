#!/usr/bin/env python3
"""
Random Workflow Generator for DISSECT-CF-Fog
=============================================

Generates large, complex workflow DAGs with:
- Nested fan-outs (fan-out inside fan-out)
- Parallel independent branches
- Filter-map chains, diamond patterns
- Cross-edges between branches (realistic inter-branch dependencies)
- Skip-edges (shortcut connections across levels)

The result is a chaotic but valid DAG resembling real scientific workflows
(Montage, CyberShake, Epigenomics, etc.).

Usage:
    python3 random_workflow_generator.py --nodes 150 --output out.xml
    python3 random_workflow_generator.py --nodes 200 --chaos 0.3 --seed 42 --output out.xml
    python3 random_workflow_generator.py --help
"""

import argparse
import os
import random
import sys
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple

from workflow_grammar import _DEFAULT_OUTPUT_DIR
from workflow_grammar import (
    BroadcastPattern,
    ChainedFanOutPattern,
    FanOutPattern,
    ForkJoinPattern,
    GatherPattern,
    Node,
    NodeType,
    ParallelPattern,
    Pattern,
    PipelinePattern,
    StencilPattern,
    WorkflowDAG,
    WorkflowGrammar,
)


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

_COMPUTE_TYPES = [NodeType.COMPUTE, NodeType.FILTER]
_COMPUTE_WEIGHTS = [0.7, 0.3]


def _random_steps(count: int) -> List[NodeType]:
    return random.choices(_COMPUTE_TYPES, weights=_COMPUTE_WEIGHTS, k=max(1, count))


def _short_pipeline(min_len: int = 1, max_len: int = 3) -> PipelinePattern:
    return PipelinePattern(_random_steps(random.randint(min_len, max_len)))


# ---------------------------------------------------------------------------
# Segment generators
# ---------------------------------------------------------------------------

def _make_simple_fanout(budget: int, depth: int) -> Pattern:
    fan = random.randint(2, min(5, max(2, budget // 3)))
    branch_budget = max(2, (budget - 2) // fan)
    branch = _random_branch(depth - 1, branch_budget)
    return FanOutPattern(fan_out=fan, branch=branch)


def _make_nested_fanout(budget: int, depth: int) -> Pattern:
    outer_fan = random.randint(2, min(4, max(2, budget // 8)))
    inner_fan = random.randint(2, min(4, max(2, budget // (outer_fan * 3))))
    leaf_budget = max(2, (budget - 4) // (outer_fan * inner_fan))
    leaf = _random_branch(max(0, depth - 2), leaf_budget)
    inner = FanOutPattern(fan_out=inner_fan, branch=leaf)
    return FanOutPattern(fan_out=outer_fan, branch=inner)


def _make_parallel(budget: int, depth: int) -> Pattern:
    n = random.randint(2, min(4, max(2, budget // 4)))
    per = max(2, budget // n)
    branches = [_random_branch(depth - 1, per) for _ in range(n)]
    return ParallelPattern(branches)


def _make_fanout_mixed(budget: int, depth: int) -> Pattern:
    fan = random.randint(2, min(4, max(2, budget // 4)))
    per_branch = max(2, (budget - 2) // fan)
    branches = [_random_branch(depth - 1, per_branch) for _ in range(fan)]
    complex_branch = random.choice(branches)
    return FanOutPattern(fan_out=fan, branch=complex_branch)


def _make_filter_compute_chain(budget: int) -> Pattern:
    pairs = max(1, budget // 2)
    steps = []
    for _ in range(pairs):
        steps.append(NodeType.COMPUTE)
        steps.append(NodeType.FILTER)
    return PipelinePattern(steps[:budget])


def _make_diamond(budget: int, depth: int) -> Pattern:
    if budget < 12:
        return _make_simple_fanout(budget, depth)
    half = budget // 2
    return ParallelPattern([
        _make_simple_fanout(half, depth),
        _make_simple_fanout(budget - half, depth),
    ])


def _make_broadcast(budget: int, depth: int) -> Pattern:
    """Broadcast: full data copy to N branches, then aggregate."""
    fan = random.randint(2, min(4, max(2, budget // 3)))
    branch_budget = max(1, (budget - 1) // fan)
    branch = _random_branch(depth - 1, branch_budget)
    return BroadcastPattern(fan_out=fan, branch=branch)


def _make_stencil(budget: int, depth: int) -> Pattern:
    """Stencil/mesh grid with neighbour dependencies."""
    width = random.randint(3, min(6, max(3, int(budget ** 0.5) + 1)))
    stencil_depth = max(2, budget // width)
    return StencilPattern(width=width, depth=stencil_depth)


def _make_chained_fanout(budget: int, depth: int) -> Pattern:
    """Repeated fan-out -> merge stages."""
    stages = random.randint(2, min(3, max(2, budget // 10)))
    fan = random.randint(2, min(3, max(2, budget // (stages * 3))))
    per_stage = max(1, (budget - stages * 2) // (stages * fan))
    branch = _random_branch(max(0, depth - 1), per_stage)
    return ChainedFanOutPattern(stages=stages, fan_out=fan, branch=branch)


def _random_branch(depth: int, budget: int) -> Pattern:
    if depth <= 0 or budget <= 4:
        return PipelinePattern(_random_steps(max(1, budget)))

    roll = random.random()
    if roll < 0.07:
        return PipelinePattern(_random_steps(random.randint(2, max(2, budget))))
    elif roll < 0.15:
        return _make_filter_compute_chain(budget)
    elif roll < 0.30:
        return _make_simple_fanout(budget, depth)
    elif roll < 0.42:
        return _make_nested_fanout(budget, depth)
    elif roll < 0.52:
        return _make_parallel(budget, depth)
    elif roll < 0.62:
        return _make_diamond(budget, depth)
    elif roll < 0.74:
        return _make_broadcast(budget, depth)
    elif roll < 0.86:
        return _make_stencil(budget, depth)
    else:
        return _make_chained_fanout(budget, depth)


# ---------------------------------------------------------------------------
# Post-processing: cross-edges & skip-edges to make the DAG chaotic
# ---------------------------------------------------------------------------

def _compute_topo_levels(dag: WorkflowDAG) -> Dict[Node, int]:
    """Assign each node a topological level (longest path from any root)."""
    children_map: Dict[Node, List[Node]] = defaultdict(list)
    parents_map: Dict[Node, List[Node]] = defaultdict(list)
    for p, c in dag.edges:
        children_map[p].append(c)
        parents_map[c].append(p)

    # BFS from roots
    in_degree = {n: 0 for n in dag.nodes}
    for _, c in dag.edges:
        in_degree[c] += 1

    queue = deque()
    level = {}
    for n in dag.nodes:
        if in_degree[n] == 0:
            queue.append(n)
            level[n] = 0

    while queue:
        node = queue.popleft()
        for child in children_map[node]:
            new_level = level[node] + 1
            if child not in level or new_level > level[child]:
                level[child] = new_level
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    return level


def _would_create_cycle(dag: WorkflowDAG, src: Node, dst: Node) -> bool:
    """Quick check: is there already a path from dst → src? If so, adding src→dst creates a cycle."""
    children_map: Dict[Node, List[Node]] = defaultdict(list)
    for p, c in dag.edges:
        children_map[p].append(c)

    visited: Set[Node] = set()
    queue = deque([dst])
    while queue:
        node = queue.popleft()
        if node is src:
            return True
        if node in visited:
            continue
        visited.add(node)
        for child in children_map[node]:
            queue.append(child)
    return False


def add_cross_edges(dag: WorkflowDAG, chaos: float = 0.2):
    """Add random cross-branch and skip-level edges to make the DAG messy.

    chaos: fraction of existing edge count to add as new cross-edges (0.0–1.0).
           0.1 = mild cross-connections, 0.3 = heavy, 0.5 = very chaotic.
    """
    if chaos <= 0 or len(dag.nodes) < 6:
        return

    levels = _compute_topo_levels(dag)
    max_level = max(levels.values()) if levels else 0
    if max_level < 2:
        return

    # Group nodes by level
    by_level: Dict[int, List[Node]] = defaultdict(list)
    for node, lvl in levels.items():
        by_level[lvl].append(node)

    # Existing edges as a set for dedup
    existing: Set[Tuple[Node, Node]] = set(dag.edges)

    # Target number of cross-edges
    target_cross = max(1, int(len(dag.edges) * chaos))
    added = 0
    attempts = 0
    max_attempts = target_cross * 10

    sorted_levels = sorted(by_level.keys())

    while added < target_cross and attempts < max_attempts:
        attempts += 1

        # Pick a source level (not the last)
        src_level = random.choice(sorted_levels[:-1])

        # Pick a destination level at least 1 higher (skip-edges go further)
        min_dst_level = src_level + 1
        # Allow skipping up to 3 levels for skip-edges
        max_dst_level = min(max_level, src_level + random.randint(1, 3))
        dst_level = random.randint(min_dst_level, max_dst_level)

        if dst_level not in by_level:
            continue

        src_node = random.choice(by_level[src_level])
        dst_node = random.choice(by_level[dst_level])

        # Skip structural nodes as targets (don't mess up ROUTE/AGG semantics)
        if dst_node.node_type in (NodeType.SOURCE, NodeType.SINK):
            continue

        # Don't add duplicate edges
        if (src_node, dst_node) in existing:
            continue

        # Don't create cycles
        if _would_create_cycle(dag, src_node, dst_node):
            continue

        dag.add_edge(src_node, dst_node)
        existing.add((src_node, dst_node))
        added += 1


# ---------------------------------------------------------------------------
# Top-level workflow assembly
# ---------------------------------------------------------------------------

_SEGMENT_BUILDERS = [
    _make_simple_fanout,
    _make_nested_fanout,
    _make_parallel,
    _make_fanout_mixed,
    _make_diamond,
    _make_broadcast,
    _make_stencil,
    _make_chained_fanout,
]


def build_random_workflow(name: str, target_nodes: int,
                          max_nesting_depth: int,
                          chaos: float = 0.2) -> WorkflowGrammar:
    """Build a complex random workflow with cross-branch connections.

    SOURCE → [connector] → [block] → [connector] → [block] → ... → SINK
    Then add random cross-edges between branches for realistic chaos.
    """
    grammar = WorkflowGrammar(name)

    middle_budget = max(10, target_nodes - 2)
    n_segments = random.randint(2, min(5, max(2, middle_budget // 15)))

    # Pick diverse segment types
    available = list(_SEGMENT_BUILDERS)
    random.shuffle(available)
    chosen = [available[i % len(available)] for i in range(n_segments)]
    random.shuffle(chosen)

    connector_budget = max(n_segments, middle_budget // 10)
    per_segment = max(6, (middle_budget - connector_budget) // n_segments)

    parts: List[Pattern] = []
    for builder in chosen:
        parts.append(_short_pipeline(1, 2))
        parts.append(builder(per_segment, max_nesting_depth))

    if random.random() < 0.5:
        parts.append(_short_pipeline(1, 2))

    grammar.chain(
        PipelinePattern([NodeType.SOURCE]),
        *parts,
        PipelinePattern([NodeType.SINK]),
    )

    # Build the DAG, then add cross-edges
    dag = grammar.build()
    add_cross_edges(dag, chaos=chaos)

    # Store the modified DAG back
    grammar.dag = dag

    return grammar


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="random_workflow_generator.py",
        description="Generate large random DISSECT-CF-Fog workflows with nested "
                    "branching and cross-branch connections.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--nodes", "-n", type=int, default=100, metavar="N",
        help="Target node count (approximate, default: 100).",
    )
    p.add_argument(
        "--max-depth", type=int, default=4, metavar="D",
        help="Max nesting depth for recursive patterns (default: 4).",
    )
    p.add_argument(
        "--chaos", type=float, default=0.2, metavar="F",
        help="Cross-edge ratio: 0.0=clean, 0.2=moderate, 0.5=very chaotic (default: 0.2).",
    )
    p.add_argument(
        "--output", "-o", default=None, metavar="FILE",
        help="Output XML filename (placed in WORKFLOW_examples by default).",
    )
    p.add_argument(
        "--name", default=None, metavar="NAME",
        help="Workflow name in the XML (default: auto-generated).",
    )
    p.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducible output.",
    )
    p.add_argument(
        "--count", "-c", type=int, default=1, metavar="C",
        help="Generate C workflows into separate files (_1.xml, _2.xml, …).",
    )
    return p


def main():
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    target = args.nodes
    os.makedirs(_DEFAULT_OUTPUT_DIR, exist_ok=True)

    # Resolve base filename
    base_name = (os.path.basename(args.output) if args.output
                 else f"random-wf-n{target}.xml")

    for i in range(args.count):
        if args.count == 1:
            filename = base_name
            wf_name = args.name or f"random-wf-n{target}"
        else:
            stem = base_name.rsplit(".", 1)
            filename = (f"{stem[0]}_{i+1}.{stem[1]}" if len(stem) == 2
                        else f"{base_name}_{i+1}")
            wf_name = args.name or f"random-wf-{i+1}-n{target}"

        out = os.path.join(_DEFAULT_OUTPUT_DIR, filename)
        grammar = build_random_workflow(wf_name, target, args.max_depth, args.chaos)
        # Use the post-processed DAG (with cross-edges)
        xml_str = grammar.dag.to_xml(wf_name)
        with open(out, "w", encoding="utf-8") as f:
            f.write(xml_str)
        n = len(grammar.dag.nodes)
        e = len(grammar.dag.edges)
        print(f"✓ Workflow '{wf_name}' written to: {out}")
        print(f"  Nodes: {n}  Edges: {e}  (cross-edge ratio: {args.chaos})")


if __name__ == "__main__":
    main()
