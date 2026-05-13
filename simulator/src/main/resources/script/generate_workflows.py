#!/usr/bin/env python3
"""
WfCommons Workflow Generator for DISSECT-CF-Fog
================================================

Easy-to-use script for generating synthetic workflows using WfCommons.

Usage:
    python3 generate_workflows.py --help
    python3 generate_workflows.py --list
    python3 generate_workflows.py --type blast --tasks 100
    python3 generate_workflows.py --type blast --tasks 100 --convert
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path

# Default output directory for generated WfCommons JSON workflows
# (sibling of WORKFLOW_examples; XML outputs land in WORKFLOW_examples via
# the converter).
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_JSON_DIR = os.path.join(_SCRIPT_DIR, "..", "demo", "WfCommons_JSON")

# Import all available recipes
from wfcommons import (
    WorkflowGenerator,
    BlastRecipe,
    BwaRecipe,
    CyclesRecipe,
    EpigenomicsRecipe,
    GenomeRecipe,
    MontageRecipe,
    SeismologyRecipe,
    SoykbRecipe,
    SrasearchRecipe
)

# Available workflow types with metadata
WORKFLOW_RECIPES = {
    "blast": {
        "class": BlastRecipe,
        "name": "BLAST Sequence Alignment",
        "min_tasks": 45,
        "structure": "Fan-out/Fan-in",
        "description": "Protein/DNA sequence comparison workflow with parallel BLAST searches"
    },
    "bwa": {
        "class": BwaRecipe,
        "name": "BWA Genomics Pipeline",
        "min_tasks": 30,
        "structure": "MapReduce-like",
        "description": "Burrows-Wheeler Aligner for DNA sequence mapping"
    },
    "cycles": {
        "class": CyclesRecipe,
        "name": "Crop Modeling (Cycles)",
        "min_tasks": 20,
        "structure": "Mixed patterns",
        "description": "Agricultural crop growth simulation workflow"
    },
    "epigenomics": {
        "class": EpigenomicsRecipe,
        "name": "Epigenomics Sequencing",
        "min_tasks": 20,
        "structure": "Pipeline",
        "description": "DNA methylation and epigenetic analysis pipeline"
    },
    "genome": {
        "class": GenomeRecipe,
        "name": "1000 Genomes Project",
        "min_tasks": 50,
        "structure": "Complex DAG",
        "description": "Large-scale genomic variation analysis workflow"
    },
    "montage": {
        "class": MontageRecipe,
        "name": "Montage Astronomy",
        "min_tasks": 30,
        "structure": "Pipeline",
        "description": "Astronomical image mosaic generation workflow"
    },
    "seismology": {
        "class": SeismologyRecipe,
        "name": "Seismology (CyberShake)",
        "min_tasks": 30,
        "structure": "Fan-out/Fan-in",
        "description": "Earthquake hazard characterization workflow"
    },
    "soykb": {
        "class": SoykbRecipe,
        "name": "Soybean Knowledge Base",
        "min_tasks": 25,
        "structure": "Pipeline",
        "description": "Soybean genomics data processing workflow"
    },
    "srasearch": {
        "class": SrasearchRecipe,
        "name": "SRA Sequence Search",
        "min_tasks": 20,
        "structure": "Pipeline",
        "description": "NCBI Sequence Read Archive search workflow"
    }
}


def list_workflows():
    """Display all available workflow types."""
    print("\n" + "=" * 80)
    print("Available WfCommons Workflow Recipes")
    print("=" * 80)
    print()

    for i, (key, info) in enumerate(WORKFLOW_RECIPES.items(), 1):
        print(f"{i:2d}. {key:15s} - {info['name']}")
        print(f"     Structure:  {info['structure']}")
        print(f"     Min tasks:  {info['min_tasks']}")
        print(f"     Info:       {info['description']}")
        print()

    print("=" * 80)
    print(f"Total: {len(WORKFLOW_RECIPES)} workflow types")
    print()


def generate_workflow(workflow_type: str, num_tasks: int,
                      output_json: str = None,
                      runtime_factor: float = 1.0,
                      input_size_factor: float = 1.0,
                      output_size_factor: float = 1.0,
                      count: int = 1,
                      workflow_name: str = None):
    """Generate one or more workflows using WfCommons.

    Returns a list of JSON paths (length == count).
    """

    if workflow_type not in WORKFLOW_RECIPES:
        print(f"Error: Unknown workflow type '{workflow_type}'")
        print(f"Use --list to see available types")
        sys.exit(1)

    recipe_info = WORKFLOW_RECIPES[workflow_type]

    # Check minimum task count
    if num_tasks < recipe_info['min_tasks']:
        print(f"Warning: {workflow_type} requires at least {recipe_info['min_tasks']} tasks")
        print(f"         Adjusting to minimum: {recipe_info['min_tasks']}")
        num_tasks = recipe_info['min_tasks']

    # Resolve base filename into the dedicated JSON directory; only the
    # basename of a user-provided --output is honored, the path is forced.
    base = (os.path.basename(output_json) if output_json
            else f"{workflow_type}_{num_tasks}tasks_workflow.json")
    os.makedirs(_DEFAULT_JSON_DIR, exist_ok=True)
    stem, ext = os.path.splitext(base)
    if not ext:
        ext = ".json"

    print(f"\nGenerating {recipe_info['name']} workflow...")
    print(f"  Tasks:           {num_tasks}")
    print(f"  Structure:       {recipe_info['structure']}")
    print(f"  Count:           {count}")
    print(f"  Runtime factor:  {runtime_factor}")
    print(f"  Input  factor:   {input_size_factor}")
    print(f"  Output factor:   {output_size_factor}")
    print()

    try:
        recipe_class = recipe_info['class']
        recipe = recipe_class.from_num_tasks(
            num_tasks=num_tasks,
            runtime_factor=runtime_factor,
            input_file_size_factor=input_size_factor,
            output_file_size_factor=output_size_factor,
        )
        generator = WorkflowGenerator(recipe)

        # build_workflows(n) yields independently-seeded instances; for n==1
        # we use the simpler build_workflow() so the file is unsuffixed.
        if count == 1:
            workflows = [generator.build_workflow(workflow_name=workflow_name)]
        else:
            workflows = generator.build_workflows(count)

        json_paths = []
        for i, workflow in enumerate(workflows, start=1):
            filename = base if count == 1 else f"{stem}_{i}{ext}"
            path = os.path.join(_DEFAULT_JSON_DIR, filename)
            workflow.write_json(path)
            json_paths.append(path)
            tag = "" if count == 1 else f" [{i}/{count}]"
            print(f"✓ Generated{tag}: {path}  ({len(workflow.tasks)} tasks)")

        return json_paths

    except Exception as e:
        print(f"✗ Error generating workflow: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def convert_to_dissect_xml(json_file: str, xml_file: str = None):
    """Convert WfFormat JSON to DISSECT-CF-Fog XML."""

    if xml_file is None:
        xml_file = json_file.replace('.json', '_converted.xml')

    converter_script = Path(__file__).parent / "wfformat_to_dissect_converter.py"

    if not converter_script.exists():
        print(f"Error: Converter script not found: {converter_script}")
        sys.exit(1)

    print(f"\nConverting to DISSECT-CF-Fog XML format...")
    print(f"  Input:  {json_file}")
    print(f"  Output: {xml_file}")
    print()

    try:
        result = subprocess.run(
            ["python3", str(converter_script), json_file, xml_file],
            capture_output=True,
            text=True,
            check=True
        )

        print(result.stdout)
        return xml_file

    except subprocess.CalledProcessError as e:
        print(f"✗ Conversion failed:")
        print(e.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Generate WfCommons workflows for DISSECT-CF-Fog",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available workflow types
  python3 generate_workflows.py --list

  # Single Blast workflow, 100 tasks
  python3 generate_workflows.py --type blast --tasks 100 --convert

  # 50 independent Blast instances (HEFT-imitation training set)
  python3 generate_workflows.py --type blast --tasks 100 --count 50 --convert

  # Stress-test with larger files & slower runtimes
  python3 generate_workflows.py --type genome --tasks 200 \\
      --runtime-factor 1.5 --input-size-factor 2.0 --convert
        """
    )

    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List all available workflow types"
    )

    parser.add_argument(
        "--type", "-t",
        type=str,
        choices=list(WORKFLOW_RECIPES.keys()),
        help="Workflow type to generate"
    )

    parser.add_argument(
        "--tasks", "-n",
        type=int,
        help="Number of tasks in the workflow"
    )

    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Output JSON filename (default: <type>_<tasks>tasks_workflow.json)"
    )

    parser.add_argument(
        "--convert", "-c",
        action="store_true",
        help="Also convert to DISSECT-CF-Fog XML format"
    )

    parser.add_argument(
        "--xml-output",
        type=str,
        help="Output XML filename (only with --convert)"
    )

    parser.add_argument(
        "--runtime-factor",
        type=float,
        default=1.0,
        help="Scale all task runtimes (e.g. 1.5 = 50%% slower; default 1.0)"
    )

    parser.add_argument(
        "--input-size-factor",
        type=float,
        default=1.0,
        help="Scale all input file sizes (default 1.0)"
    )

    parser.add_argument(
        "--output-size-factor",
        type=float,
        default=1.0,
        help="Scale all output file sizes (default 1.0)"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Generate N independently-seeded instances of this workflow type "
             "(suffixed _1, _2, ...; default 1)"
    )

    parser.add_argument(
        "--workflow-name",
        type=str,
        help="Override the workflow name embedded in the JSON "
             "(default: recipe's '<Type>-synthetic-instance'). "
             "Ignored when --count > 1."
    )

    args = parser.parse_args()

    # Handle --list
    if args.list:
        list_workflows()
        return

    # Validate arguments
    if not args.type or not args.tasks:
        parser.print_help()
        print("\nError: --type and --tasks are required (or use --list)")
        sys.exit(1)

    if args.count < 1:
        print("Error: --count must be >= 1")
        sys.exit(1)

    # Generate workflow(s)
    json_files = generate_workflow(
        args.type, args.tasks, args.output,
        runtime_factor=args.runtime_factor,
        input_size_factor=args.input_size_factor,
        output_size_factor=args.output_size_factor,
        count=args.count,
        workflow_name=args.workflow_name,
    )

    # Convert to XML if requested. For --count > 1 the user-supplied
    # --xml-output basename is only used to derive a stem; each instance
    # gets a numeric suffix.
    if args.convert:
        for i, json_file in enumerate(json_files, start=1):
            if args.xml_output and len(json_files) > 1:
                stem, ext = os.path.splitext(os.path.basename(args.xml_output))
                xml_arg = f"{stem}_{i}{ext or '.xml'}"
            else:
                xml_arg = args.xml_output
            convert_to_dissect_xml(json_file, xml_arg)
        print(f"\n✓ Done! {len(json_files)} workflow(s) generated and converted.")
    else:
        print(f"\n✓ Done! {len(json_files)} workflow(s) generated. "
              f"Use --convert to also generate DISSECT-CF-Fog XML.")


if __name__ == '__main__':
    main()
