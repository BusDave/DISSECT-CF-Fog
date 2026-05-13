#!/usr/bin/env python3
"""
Workflow Grammar Generator for DISSECT-CF-Fog
==============================================

Generates workflow DAGs using formal graph grammars with explicit node types
(SOURCE, COMPUTE, FILTER, ROUTE, AGGREGATOR, SINK) and outputs DISSECT-CF-Fog
XML directly — no WfCommons or JSON intermediate step required.

Usage:
    python3 workflow_grammar.py --pattern fan-out --fan-out 4 --depth 2 --output out.xml
    python3 workflow_grammar.py --pattern pipeline --depth 5 --output out.xml
    python3 workflow_grammar.py --pattern filter-map --fan-out 3 --output out.xml
    python3 workflow_grammar.py --help
"""

import argparse
import math
import os
import random
import sys
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import List, Optional, Tuple
from xml.dom import minidom

# Default output directory: ../demo/WORKFLOW_examples relative to this script
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_OUTPUT_DIR = os.path.join(_SCRIPT_DIR, "..", "demo", "WORKFLOW_examples")


# Node types
class NodeType(Enum):
    SOURCE     = auto()
    COMPUTE    = auto()
    FILTER     = auto()
    ROUTE      = auto()
    AGGREGATOR = auto()
    SINK       = auto()


# Global counter (reset per grammar build)
_counter = 0

def _next_id(node_type: NodeType) -> str:
    global _counter
    _counter += 1
    return f"{node_type.name.lower()}_{_counter:08d}"

def _reset_counter():
    global _counter
    _counter = 0


# Runtime / output-size rules per NodeType

def _gen_runtime(node_type: NodeType) -> float:
    if node_type == NodeType.SOURCE:
        return random.uniform(1.0, 5.0)
    if node_type == NodeType.COMPUTE:
        return random.uniform(10.0, 60.0)
    if node_type == NodeType.FILTER:
        return random.uniform(1.0, 10.0)
    if node_type == NodeType.ROUTE:
        return random.uniform(0.5, 2.0)
    if node_type == NodeType.AGGREGATOR:
        return random.uniform(5.0, 30.0)
    if node_type == NodeType.SINK:
        return random.uniform(0.5, 2.0)
    return 0.1


def _gen_output_size(node_type: NodeType, input_size: int, fan_out: int = 1) -> int:
    """Calculate output size based on node type.

    fan_out is only relevant for ROUTE nodes.
    """
    if node_type == NodeType.SOURCE:
        # Log-uniform 1 KB .. 100 MB — heavy-tailed like real scientific
        # workflows (CyberShake median is ~20 KB with a few huge files).
        return int(math.exp(random.uniform(math.log(1024),
                                            math.log(100 * 1024 * 1024))))
    if node_type == NodeType.COMPUTE:
        # Slight average decrease (re-encode / transform): mean ~0.85
        return max(1024, int(input_size * random.uniform(0.5, 1.2)))
    if node_type == NodeType.FILTER:
        # Filter strongly reduces: mean ~0.45
        return max(1024, int(input_size * random.uniform(0.2, 0.7)))
    if node_type == NodeType.ROUTE:
        return max(1, input_size // fan_out)
    if node_type == NodeType.AGGREGATOR:
        # input_size = max of incoming edges; aggregator typically reduces
        # via stat / merge: mean ~0.6
        return max(1024, int(input_size * random.uniform(0.3, 0.9)))
    if node_type == NodeType.SINK:
        return 0
    return 0


# Node and DAG

_MAX_EDGE_SIZE = 1 * 1024 * 1024 * 1024  # 1 GiB hard cap — Java reads size as long

class Node:
    """A vertex in the workflow DAG."""

    def __init__(self, node_type: NodeType, input_size: int = 0, fan_out: int = 1):
        self.node_id   = _next_id(node_type)
        self.node_type = node_type
        self.runtime   = _gen_runtime(node_type)
        # Cap at 1 GiB: COMPUTE/AGGREGATOR forward+sum input sizes, which can
        # explode through deep nested fan-ins and overflow Java's signed long.
        self.output_size = min(_MAX_EDGE_SIZE,
                               _gen_output_size(node_type, input_size, fan_out))

    def __repr__(self):
        return (f"Node({self.node_id}, runtime={self.runtime:.2f}, "
                f"output_size={self.output_size})")


class WorkflowDAG:
    """Directed Acyclic Graph representing a workflow."""

    def __init__(self):
        self.nodes: List[Node] = []
        self.edges: List[Tuple[Node, Node]] = []  # (parent, child)

    def add_node(self, node: Node) -> Node:
        self.nodes.append(node)
        return node

    def add_edge(self, parent: Node, child: Node):
        self.edges.append((parent, child))

    # --- derived helpers ---------------------------------------------------

    def _parents_of(self, node: Node) -> List[Node]:
        return [p for p, c in self.edges if c is node]

    def _children_of(self, node: Node) -> List[Node]:
        return [c for p, c in self.edges if p is node]

    def _is_root(self, node: Node) -> bool:
        return len(self._parents_of(node)) == 0

    # --- XML generation ----------------------------------------------------

    def to_xml(self, name: str) -> str:
        """Generate DISSECT-CF-Fog XML string from the DAG."""
        root = ET.Element("adag")
        root.set("name", name)
        root.set("repeat", "1")
        root.append(ET.Comment(" Generated by workflow_grammar.py "))

        for node in self.nodes:
            job = ET.SubElement(root, "job")
            job.set("id", node.node_id)
            job.set("runtime", f"{node.runtime:.2f}")

            # input <uses>
            # amount = number of parents (in-degree); the scheduler decrements
            # it per parent completion and only runs the job when it hits 0.
            inp = ET.SubElement(job, "uses")
            inp.set("link", "input")
            inp.set("type", "compute")
            inp.set("amount", str(len(self._parents_of(node))))

            # output <uses> — one per child edge
            children = self._children_of(node)
            for child in children:
                out = ET.SubElement(job, "uses")
                out.set("link", "output")
                out.set("id", child.node_id)
                out.set("type", "data")
                out.set("size", str(node.output_size))

        xml_str = minidom.parseString(
            ET.tostring(root, encoding="unicode")
        ).toprettyxml(indent="  ")

        # Remove extra blank lines introduced by minidom
        xml_str = "\n".join(line for line in xml_str.split("\n") if line.strip())
        return xml_str


# Pattern base class

class Pattern(ABC):
    """Abstract base for workflow structure patterns.

    build(dag, entry_nodes) → exit_nodes
    - Adds nodes/edges to dag.
    - entry_nodes: nodes whose output feeds into this pattern's first layer.
      If None or empty, the pattern creates its own entry nodes (root nodes).
    - Returns the list of exit (leaf) nodes of this pattern.
    """

    @abstractmethod
    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        ...


# PipelinePattern

class PipelinePattern(Pattern):
    """Linear chain: A → B → C → …

    Each step in `steps` is a NodeType. All entry_nodes connect to the first
    node; the last node is returned as a single-element exit list.

    If multiple entry_nodes are given each one connects to a freshly created
    first node (i.e. the pipeline is replicated once per entry).
    """

    def __init__(self, steps: List[NodeType]):
        if not steps:
            raise ValueError("PipelinePattern requires at least one step.")
        self.steps = steps

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        exits: List[Node] = []

        # Determine how many parallel pipeline instances to create.
        # If there are no entry nodes, build a single pipeline (starts at root).
        fancount = max(1, len(entry_nodes)) if entry_nodes else 1

        for i in range(fancount):
            entry = entry_nodes[i] if entry_nodes else None
            input_size = entry.output_size if entry else 0
            prev = entry

            for node_type in self.steps:
                node = dag.add_node(Node(node_type, input_size=input_size))
                if prev is not None:
                    dag.add_edge(prev, node)
                input_size = node.output_size
                prev = node

            exits.append(prev)  # last node of this pipeline instance

        return exits


# FanOutPattern

class FanOutPattern(Pattern):
    """ROUTE → [branch] × fan_out → AGGREGATOR

    One ROUTE node fans out to `fan_out` parallel branch instances,
    then all branches converge into one AGGREGATOR.
    """

    def __init__(self, fan_out: int, branch: Pattern):
        if fan_out < 1:
            raise ValueError("fan_out must be >= 1.")
        self.fan_out = fan_out
        self.branch  = branch

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        # Support multiple entry nodes by creating one ROUTE per entry
        entries = entry_nodes if entry_nodes else [None]
        all_exits: List[Node] = []

        for entry in entries:
            input_size = entry.output_size if entry else 0

            # ROUTE node — splits data across branches
            route = dag.add_node(Node(NodeType.ROUTE,
                                      input_size=input_size,
                                      fan_out=self.fan_out))
            if entry is not None:
                dag.add_edge(entry, route)

            # Build fan_out parallel branch instances
            branch_exits = self.branch.build(dag, [route] * self.fan_out)

            # AGGREGATOR — sum of all branch outputs
            agg_input_size = max((n.output_size for n in branch_exits), default=0)
            agg = dag.add_node(Node(NodeType.AGGREGATOR,
                                    input_size=agg_input_size))
            for branch_exit in branch_exits:
                dag.add_edge(branch_exit, agg)

            all_exits.append(agg)

        return all_exits


# ParallelPattern

class ParallelPattern(Pattern):
    """Multiple independent branches side by side (no shared ROUTE/AGGREGATOR).

    Each branch in `branches` receives ALL entry_nodes as its own entries
    (or starts independently if there are none). Returns all branch exits.
    """

    def __init__(self, branches: List[Pattern]):
        if not branches:
            raise ValueError("ParallelPattern requires at least one branch.")
        self.branches = branches

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        exits: List[Node] = []
        for branch in self.branches:
            branch_exits = branch.build(dag, entry_nodes)
            exits.extend(branch_exits)
        return exits


# GatherPattern

class GatherPattern(Pattern):
    """All entry nodes converge into a single AGGREGATOR (pure fan-in).

    entry_1 →+
    entry_2 →+→ AGGREGATOR
    entry_3 →+

    If only one entry node is provided, it is passed through unchanged.
    """

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        if not entry_nodes:
            raise ValueError("GatherPattern requires at least one entry node.")
        if len(entry_nodes) == 1:
            return entry_nodes

        agg_input_size = max((n.output_size for n in entry_nodes), default=0)
        agg = dag.add_node(Node(NodeType.AGGREGATOR,
                                input_size=agg_input_size))
        for entry in entry_nodes:
            dag.add_edge(entry, agg)

        return [agg]


# MultiSourcePattern

class MultiSourcePattern(Pattern):
    """Multiple independent SOURCE nodes, each feeding into a branch.

    SOURCE_1 → branch_instance_1
    SOURCE_2 → branch_instance_2
    …
    SOURCE_N → branch_instance_N

    Returns all branch exit nodes.  Ignores any entry_nodes passed in.
    """

    def __init__(self, count: int, branch: Pattern):
        if count < 1:
            raise ValueError("MultiSourcePattern requires count >= 1.")
        self.count  = count
        self.branch = branch

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        sources: List[Node] = []
        for _ in range(self.count):
            src = dag.add_node(Node(NodeType.SOURCE))
            sources.append(src)

        return self.branch.build(dag, sources)


# ForkJoinPattern

class ForkJoinPattern(Pattern):
    """Multiple sources → parallel branches → single aggregator (map-reduce).

    SOURCE_1 → branch →+
    SOURCE_2 → branch →+→ AGGREGATOR
    SOURCE_3 → branch →+

    Combines MultiSource + branch processing + Gather.
    Ignores any entry_nodes; always creates its own sources.
    """

    def __init__(self, source_count: int, branch: Pattern):
        if source_count < 1:
            raise ValueError("ForkJoinPattern requires source_count >= 1.")
        self.source_count = source_count
        self.branch = branch

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        sources: List[Node] = []
        for _ in range(self.source_count):
            src = dag.add_node(Node(NodeType.SOURCE))
            sources.append(src)

        branch_exits = self.branch.build(dag, sources)

        agg_input_size = max((n.output_size for n in branch_exits), default=0)
        agg = dag.add_node(Node(NodeType.AGGREGATOR,
                                input_size=agg_input_size))
        for exit_node in branch_exits:
            dag.add_edge(exit_node, agg)

        return [agg]


# BroadcastPattern

class BroadcastPattern(Pattern):
    """Broadcasts full data to N parallel branches, then aggregates.

    Unlike FanOut (which uses a ROUTE node to divide data equally),
    Broadcast sends the complete output to every branch — each child
    receives the full data set.

    entry →→→ branch_1 (full data) →+
          →→→ branch_2 (full data) →+→ AGGREGATOR
          →→→ branch_3 (full data) →+
    """

    def __init__(self, fan_out: int, branch: Pattern):
        if fan_out < 1:
            raise ValueError("fan_out must be >= 1.")
        self.fan_out = fan_out
        self.branch  = branch

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        entries = entry_nodes if entry_nodes else [None]
        all_exits: List[Node] = []

        for entry in entries:
            # Each branch instance connects to the same entry (full data)
            branch_entries = ([entry] * self.fan_out
                              if entry is not None else None)
            branch_exits = self.branch.build(dag, branch_entries)

            agg_input_size = max((n.output_size for n in branch_exits), default=0)
            agg = dag.add_node(Node(NodeType.AGGREGATOR,
                                    input_size=agg_input_size))
            for exit_node in branch_exits:
                dag.add_edge(exit_node, agg)

            all_exits.append(agg)

        return all_exits


# StencilPattern

class StencilPattern(Pattern):
    """Grid/mesh where each node depends on neighbours from the previous level.

    Level 0:  A1   A2   A3   A4
              |\\  |\\  |\\  |
              | \\ | \\ | \\ |
    Level 1:  B1   B2   B3   B4
              |\\  |\\  |\\  |
              | \\ | \\ | \\ |
    Level 2:  C1   C2   C3   C4

    Node (i, j) depends on nodes (i-1, j-1), (i-1, j), (i-1, j+1)
    (where they exist).
    """

    def __init__(self, width: int, depth: int,
                 node_type: NodeType = NodeType.COMPUTE):
        if width < 2:
            raise ValueError("StencilPattern requires width >= 2.")
        if depth < 1:
            raise ValueError("StencilPattern requires depth >= 1.")
        self.width     = width
        self.depth     = depth
        self.node_type = node_type

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        # Build first level
        if entry_nodes and len(entry_nodes) == self.width:
            current_level = list(entry_nodes)
        elif entry_nodes:
            # Fan out from entry nodes to width columns
            current_level = []
            for _ in range(self.width):
                input_size = max((n.output_size for n in entry_nodes), default=0)
                node = dag.add_node(Node(self.node_type,
                                         input_size=input_size))
                for entry in entry_nodes:
                    dag.add_edge(entry, node)
                current_level.append(node)
        else:
            current_level = []
            for _ in range(self.width):
                node = dag.add_node(Node(self.node_type, input_size=0))
                current_level.append(node)

        # Build subsequent levels with neighbour dependencies
        for _ in range(1, self.depth):
            next_level: List[Node] = []
            for j in range(self.width):
                parents = []
                for dj in [-1, 0, 1]:
                    pj = j + dj
                    if 0 <= pj < self.width:
                        parents.append(current_level[pj])

                input_size = max((p.output_size for p in parents), default=0)
                node = dag.add_node(Node(self.node_type,
                                         input_size=input_size))
                for parent in parents:
                    dag.add_edge(parent, node)
                next_level.append(node)

            current_level = next_level

        return current_level


# ChainedFanOutPattern

class ChainedFanOutPattern(Pattern):
    """Repeated fan-out → merge stages in sequence (iterative map-reduce).

    ROUTE → branches → AGG → ROUTE → branches → AGG → …

    Each stage is a FanOutPattern.  The output of one stage feeds the next.
    """

    def __init__(self, stages: int, fan_out: int, branch: Pattern):
        if stages < 1:
            raise ValueError("ChainedFanOutPattern requires stages >= 1.")
        self.stages  = stages
        self.fan_out = fan_out
        self.branch  = branch

    def build(self, dag: WorkflowDAG,
              entry_nodes: Optional[List[Node]]) -> List[Node]:
        current = entry_nodes
        for _ in range(self.stages):
            fanout = FanOutPattern(fan_out=self.fan_out, branch=self.branch)
            current = fanout.build(dag, current)
        return current


# WorkflowGrammar — orchestrator

class WorkflowGrammar:
    """Chains patterns sequentially to build a complete workflow DAG."""

    def __init__(self, name: str):
        self.name     = name
        self.dag      = WorkflowDAG()
        self._patterns: List[Pattern] = []

    def chain(self, *patterns: Pattern) -> "WorkflowGrammar":
        """Register patterns in order; returns self for fluent chaining."""
        self._patterns.extend(patterns)
        return self

    def build(self) -> WorkflowDAG:
        """Execute all registered patterns and return the finished DAG."""
        _reset_counter()
        self.dag = WorkflowDAG()

        current_exits: Optional[List[Node]] = None
        for pattern in self._patterns:
            current_exits = pattern.build(self.dag, current_exits)

        return self.dag

    def build_xml(self, output_path: str):
        """Build the DAG and write XML to output_path."""
        dag = self.build()
        xml_str = dag.to_xml(self.name)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_str)
        node_count = len(dag.nodes)
        edge_count = len(dag.edges)
        print(f"✓ Workflow '{self.name}' written to: {output_path}")
        print(f"  Nodes: {node_count}  Edges: {edge_count}")


# Pre-defined named patterns

def build_pipeline_grammar(name: str, depth: int) -> WorkflowGrammar:
    """SOURCE → COMPUTE × depth → SINK"""
    steps = [NodeType.SOURCE] + [NodeType.COMPUTE] * depth + [NodeType.SINK]
    g = WorkflowGrammar(name)
    g.chain(PipelinePattern(steps))
    return g


def build_fan_out_grammar(name: str, fan_out: int, depth: int) -> WorkflowGrammar:
    """SOURCE → ROUTE → (COMPUTE × depth) × fan_out → AGGREGATOR → SINK"""
    compute_steps = [NodeType.COMPUTE] * depth
    branch = PipelinePattern(compute_steps)
    g = WorkflowGrammar(name)
    g.chain(
        PipelinePattern([NodeType.SOURCE]),
        FanOutPattern(fan_out=fan_out, branch=branch),
        PipelinePattern([NodeType.SINK]),
    )
    return g


def build_filter_map_grammar(name: str, fan_out: int, depth: int) -> WorkflowGrammar:
    """SOURCE → FILTER → ROUTE → (COMPUTE → FILTER) × fan_out → AGGREGATOR → SINK"""
    compute_filter_steps = ([NodeType.COMPUTE] + [NodeType.FILTER]) * depth
    branch = PipelinePattern(compute_filter_steps)
    g = WorkflowGrammar(name)
    g.chain(
        PipelinePattern([NodeType.SOURCE, NodeType.FILTER]),
        FanOutPattern(fan_out=fan_out, branch=branch),
        PipelinePattern([NodeType.SINK]),
    )
    return g


# Pre-defined named patterns — new

def build_fork_join_grammar(name: str, source_count: int,
                            depth: int) -> WorkflowGrammar:
    """SOURCE × N → (COMPUTE × depth) × N → AGGREGATOR → SINK"""
    branch = PipelinePattern([NodeType.COMPUTE] * depth)
    g = WorkflowGrammar(name)
    g.chain(
        ForkJoinPattern(source_count=source_count, branch=branch),
        PipelinePattern([NodeType.SINK]),
    )
    return g


def build_stencil_grammar(name: str, width: int,
                          depth: int) -> WorkflowGrammar:
    """SOURCE → Stencil(width × depth) → AGGREGATOR → SINK"""
    g = WorkflowGrammar(name)
    g.chain(
        PipelinePattern([NodeType.SOURCE]),
        StencilPattern(width=width, depth=depth),
        GatherPattern(),
        PipelinePattern([NodeType.SINK]),
    )
    return g


def build_chained_fanout_grammar(name: str, stages: int, fan_out: int,
                                  depth: int) -> WorkflowGrammar:
    """SOURCE → (ROUTE → COMPUTE×depth × fan_out → AGG) × stages → SINK"""
    branch = PipelinePattern([NodeType.COMPUTE] * depth)
    g = WorkflowGrammar(name)
    g.chain(
        PipelinePattern([NodeType.SOURCE]),
        ChainedFanOutPattern(stages=stages, fan_out=fan_out, branch=branch),
        PipelinePattern([NodeType.SINK]),
    )
    return g


def build_broadcast_grammar(name: str, fan_out: int,
                            depth: int) -> WorkflowGrammar:
    """SOURCE → Broadcast → (COMPUTE × depth) × fan_out → AGGREGATOR → SINK"""
    branch = PipelinePattern([NodeType.COMPUTE] * depth)
    g = WorkflowGrammar(name)
    g.chain(
        PipelinePattern([NodeType.SOURCE]),
        BroadcastPattern(fan_out=fan_out, branch=branch),
        PipelinePattern([NodeType.SINK]),
    )
    return g


# CLI


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="workflow_grammar.py",
        description=(
            "Generate DISSECT-CF-Fog workflow XML using formal graph grammars.\n\n"
            "Pre-defined patterns:\n"
            "  pipeline         SOURCE → COMPUTE×N → SINK\n"
            "  fan-out          SOURCE → ROUTE → (COMPUTE×depth)×fan_out → AGG → SINK\n"
            "  filter-map       SOURCE → FILTER → ROUTE → (COMPUTE→FILTER)×fan_out"
            " → AGG → SINK\n"
            "  fork-join        SOURCE×N → (COMPUTE×depth)×N → AGG → SINK\n"
            "  stencil          SOURCE → mesh(width×depth) → AGG → SINK\n"
            "  chained-fan-out  SOURCE → (ROUTE→branches→AGG)×stages → SINK\n"
            "  broadcast        SOURCE → full-data-copy×fan_out → AGG → SINK\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--pattern", "-p",
        choices=["pipeline", "fan-out", "filter-map",
                 "fork-join", "stencil", "chained-fan-out", "broadcast"],
        required=True,
        help="Workflow structure pattern to generate.",
    )
    p.add_argument(
        "--fan-out", "-f",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel branches (default: 4).",
    )
    p.add_argument(
        "--depth", "-d",
        type=int,
        default=2,
        metavar="N",
        help="Number of COMPUTE steps per branch / stencil levels (default: 2).",
    )
    p.add_argument(
        "--sources",
        type=int,
        default=4,
        metavar="N",
        help="Number of independent source nodes for fork-join (default: 4).",
    )
    p.add_argument(
        "--width",
        type=int,
        default=4,
        metavar="N",
        help="Grid width for stencil pattern (default: 4).",
    )
    p.add_argument(
        "--stages",
        type=int,
        default=2,
        metavar="N",
        help="Number of fan-out stages for chained-fan-out (default: 2).",
    )
    p.add_argument(
        "--output", "-o",
        default=None,
        metavar="FILE",
        help="Output XML filename (placed in WORKFLOW_examples by default).",
    )
    p.add_argument(
        "--name", "-n",
        default=None,
        metavar="NAME",
        help="Workflow name embedded in the XML (default: derived from --pattern).",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible output.",
    )
    return p


def _resolve_output(user_output: Optional[str], default_name: str) -> str:
    """Resolve output path into WORKFLOW_examples directory.

    If user_output is given, its basename is placed in WORKFLOW_examples.
    Otherwise default_name is used.
    """
    filename = os.path.basename(user_output) if user_output else default_name
    os.makedirs(_DEFAULT_OUTPUT_DIR, exist_ok=True)
    return os.path.join(_DEFAULT_OUTPUT_DIR, filename)


def main():
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    pattern  = args.pattern
    fan_out  = args.fan_out
    depth    = args.depth
    name     = args.name or f"{pattern}-fo{fan_out}-d{depth}"

    if pattern == "pipeline":
        grammar = build_pipeline_grammar(name, depth)
    elif pattern == "fan-out":
        grammar = build_fan_out_grammar(name, fan_out, depth)
    elif pattern == "filter-map":
        grammar = build_filter_map_grammar(name, fan_out, depth)
    elif pattern == "fork-join":
        name = args.name or f"fork-join-s{args.sources}-d{depth}"
        grammar = build_fork_join_grammar(name, args.sources, depth)
    elif pattern == "stencil":
        name = args.name or f"stencil-w{args.width}-d{depth}"
        grammar = build_stencil_grammar(name, args.width, depth)
    elif pattern == "chained-fan-out":
        name = args.name or f"chained-fo{fan_out}-s{args.stages}-d{depth}"
        grammar = build_chained_fanout_grammar(name, args.stages, fan_out, depth)
    elif pattern == "broadcast":
        name = args.name or f"broadcast-fo{fan_out}-d{depth}"
        grammar = build_broadcast_grammar(name, fan_out, depth)
    else:
        print(f"Unknown pattern: {pattern}", file=sys.stderr)
        sys.exit(1)

    output = _resolve_output(args.output, f"{name}.xml")
    grammar.build_xml(output)


if __name__ == "__main__":
    main()
