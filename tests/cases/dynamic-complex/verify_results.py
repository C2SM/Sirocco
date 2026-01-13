#!/usr/bin/env python3
"""Verify mathematical results from complex workflow execution.

Usage:
    python verify_results.py <workflow_pk>
"""

import sys
from pathlib import Path

from aiida import orm, load_profile
from aiida_workgraph.engine.workgraph import WorkGraphEngine

load_profile()

# Expected values per cycle
EXPECTED = {
    'setup': 1,
    'fast_1': 2,
    'medium_1': 2,
    'slow_1': 3,
    'fast_2': 3,
    'medium_2': 10,
    'slow_2': 39,
    'fast_3': 4,
    'medium_3': 20,
    'slow_3': 117,
    'finalize': 142,
}

# prepare_next depends on cycle
EXPECTED_PREPARE_NEXT = {
    '2026_01_01': 143,  # First cycle: 142 + 1
    '2026_02_01': 285,  # Second cycle: 142 + 142 + 1
}


def extract_task_and_date(job_name):
    """Extract task name and date from job name.

    Example: "dynamic_deps_complex_2026_01_07_13_35_fast_1_date_2026_01_01_00_00_00"
    Returns: ("fast_1", "2026_01_01")
    """
    parts = job_name.split('_')

    # Find date suffix
    if 'date_' in job_name:
        date_idx = job_name.index('_date_')
        task_part = job_name[:date_idx]
        date_part = job_name[date_idx+6:]  # Skip "_date_"

        # Extract task name (after workflow prefix and timestamp)
        for i in range(len(parts) - 4):
            if all(p.isdigit() for p in parts[i:i+5]):
                task_name = '_'.join(parts[i+5:]).split('_date_')[0]
                break
        else:
            task_name = task_part.split('_')[-1]

        # Extract date (YYYY_MM_DD)
        date_parts = date_part.split('_')[:3]
        date = '_'.join(date_parts)

        return task_name, date
    else:
        # No date (e.g., setup)
        for i in range(len(parts) - 4):
            if all(p.isdigit() for p in parts[i:i+5]):
                task_name = '_'.join(parts[i+5:])
                break
        else:
            task_name = parts[-1]

        return task_name, None


def verify_workflow(workflow_pk):
    """Verify mathematical results for workflow."""

    # Load workflow
    try:
        node = orm.load_node(workflow_pk)
    except Exception as e:
        print(f"Error loading node {workflow_pk}: {e}")
        return False

    # Find all CalcJob nodes
    builder = orm.QueryBuilder()
    builder.append(
        WorkGraphEngine,
        filters={'id': node.pk},
        tag='parent'
    )
    builder.append(
        WorkGraphEngine,
        with_incoming='parent',
        tag='child'
    )
    builder.append(
        orm.CalcJobNode,
        with_incoming='child',
        project=['*'],
    )

    calcjobs = [row[0] for row in builder.all()]

    if not calcjobs:
        print(f"No CalcJob nodes found under workflow {workflow_pk}")
        return False

    print(f"Verifying results for workflow PK={workflow_pk}")
    print(f"Found {len(calcjobs)} CalcJob nodes")
    print("=" * 60)
    print()

    # Group by cycle
    results = {}
    for calcjob in calcjobs:
        # Get job name from caller
        if not calcjob.caller:
            continue

        caller_label = calcjob.caller.label
        if caller_label.startswith('launch_'):
            job_name = caller_label.replace('launch_', '')
        else:
            job_name = caller_label

        task_name, date = extract_task_and_date(job_name)

        # Get remote work directory
        try:
            remote_workdir = calcjob.outputs.remote_folder.get_remote_path()
        except Exception:
            print(f"⚠️  {task_name} ({date or 'N/A'}): No remote work directory")
            continue

        # Read value.txt
        value_file = Path(remote_workdir) / f"{task_name}_output" / "value.txt"

        if not value_file.exists():
            print(f"❌ {task_name} ({date or 'N/A'}): Missing {value_file}")
            continue

        try:
            actual = int(value_file.read_text().strip())
        except Exception as e:
            print(f"❌ {task_name} ({date or 'N/A'}): Error reading value: {e}")
            continue

        # Get expected value
        if task_name == 'prepare_next':
            expected = EXPECTED_PREPARE_NEXT.get(date, '?')
        else:
            expected = EXPECTED.get(task_name, '?')

        # Store result
        cycle_key = date or "no_cycle"
        if cycle_key not in results:
            results[cycle_key] = []

        results[cycle_key].append({
            'task': task_name,
            'actual': actual,
            'expected': expected,
            'ok': actual == expected
        })

    # Print results by cycle
    all_ok = True
    for cycle in sorted(results.keys()):
        if cycle != "no_cycle":
            print(f"Cycle: {cycle.replace('_', '-')}")
        else:
            print("Setup (no cycle)")
        print("-" * 60)

        for result in sorted(results[cycle], key=lambda x: list(EXPECTED.keys()).index(x['task']) if x['task'] in EXPECTED else 999):
            symbol = "✓" if result['ok'] else "❌"
            print(f"  {symbol} {result['task']:15} = {result['actual']:4} (expected {result['expected']})")

        if not all(r['ok'] for r in results[cycle]):
            all_ok = False

        print()

    print("=" * 60)
    if all_ok:
        print("✅ All values correct!")
        return True
    else:
        print("❌ Some values incorrect!")
        return False


def main():
    if len(sys.argv) < 2:
        print("Usage: python verify_results.py <workflow_pk>")
        sys.exit(1)

    workflow_pk = int(sys.argv[1])
    success = verify_workflow(workflow_pk)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
