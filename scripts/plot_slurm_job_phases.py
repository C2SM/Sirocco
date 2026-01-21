#!/usr/bin/env python3
"""Plot job timeline from AiiDA WorkGraph showing dependency-blocked, queued, and running phases.

Usage:
    python plot_job_timeline.py <node_pk_or_uuid> [--output timeline.png] [--custom-order]

Options:
    --custom-order: Use custom task ordering for complex workflows (setup → fast → medium → slow → finalize → prepare_next)
                    Default ordering: root first, then alphabetically
"""

import argparse
import csv
import sys
from datetime import datetime
from io import StringIO
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from aiida import orm, load_profile

load_profile()


def parse_sacct_time(time_str: Optional[str]) -> Optional[datetime]:
    """Parse SLURM sacct timestamp string to datetime.

    Handles formats like:
    - "11:18:23" (time only, use today's date)
    - "2024-01-15T10:30:45"
    - "Unknown" or None
    """
    if not time_str or time_str == "Unknown" or not time_str.strip():
        return None

    try:
        # Try ISO format first
        return datetime.fromisoformat(time_str)
    except (ValueError, AttributeError):
        pass

    try:
        # Try time-only format (HH:MM:SS)
        time_obj = datetime.strptime(time_str, "%H:%M:%S").time()
        # Use today's date with the parsed time
        return datetime.combine(datetime.today().date(), time_obj)
    except (ValueError, AttributeError):
        return None


def parse_sacct_output(stdout: str) -> list[dict]:
    """Parse sacct pipe-delimited output into list of dicts.

    Returns list of job records (typically main job + batch + extern steps).
    """
    if not stdout or not stdout.strip():
        return []

    # Parse pipe-delimited CSV
    reader = csv.DictReader(StringIO(stdout), delimiter='|')
    return list(reader)


def collect_job_data(node, custom_order=False):
    """Collect job timing data from WorkGraph node.

    Args:
        node: WorkGraph node PK or UUID
        custom_order: If True, use custom ordering for complex workflows

    Returns list of dicts with job info:
    [
        {
            'name': 'job_name',
            'submit': datetime,
            'eligible': datetime,
            'start': datetime,
            'batch_start': datetime,
            'batch_end': datetime,
            'end': datetime,
        },
        ...
    ]
    """
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine

    # Load the node
    if isinstance(node, (int, str)):
        node = orm.load_node(node)

    jobs = []

    # Traverse the WorkGraph to find all CalcJob nodes
    builder = orm.QueryBuilder()
    builder.append(
        WorkGraphEngine,
        filters={'id': node.pk},
        tag='workgraph'
    )
    builder.append(
        orm.CalcJobNode,
        with_incoming='workgraph',
        project=['*'],
    )

    calcjobs = [row[0] for row in builder.all()]

    if not calcjobs:
        print(f"No CalcJob nodes found under node {node.pk}")
        # Try to look deeper - find sub-workgraphs
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

    print(f"Found {len(calcjobs)} CalcJob nodes")

    for calcjob in calcjobs:
        # Get detailed_job_info from the node
        detailed_job_info = calcjob.base.attributes.get('detailed_job_info', None)

        if not detailed_job_info:
            print(f"Warning: No detailed_job_info for {calcjob.label or calcjob.pk}")
            continue

        # Parse the sacct stdout output
        stdout = detailed_job_info.get('stdout', '')
        if not stdout:
            print(f"Warning: No sacct stdout for {calcjob.label or calcjob.pk}")
            continue

        sacct_records = parse_sacct_output(stdout)
        if not sacct_records:
            print(f"Warning: Could not parse sacct output for {calcjob.label or calcjob.pk}")
            continue

        # Find the main job record and .batch record
        main_record = None
        batch_record = None

        for rec in sacct_records:
            job_id = rec.get('JobID', '')
            if '.batch' in job_id:
                batch_record = rec
            elif '.extern' not in job_id:
                # Main job record (no suffix)
                main_record = rec

        if not main_record:
            print(f"Warning: No main job record found for {calcjob.label or calcjob.pk}")
            continue

        # DEBUG: Print all sacct records to investigate timing discrepancies
        print(f"\nDEBUG: All sacct records for {calcjob.label or calcjob.pk}:")
        for i, rec in enumerate(sacct_records):
            print(f"  Record {i}: JobID={rec.get('JobID')}, Start={rec.get('Start')}, End={rec.get('End')}, Elapsed={rec.get('Elapsed')}")

        # Extract timestamps from main job record
        submit = parse_sacct_time(main_record.get('Submit'))
        eligible = parse_sacct_time(main_record.get('Eligible'))
        start = parse_sacct_time(main_record.get('Start'))
        end = parse_sacct_time(main_record.get('End'))

        # Extract batch script execution times (actual execution)
        batch_start = None
        batch_end = None
        if batch_record:
            batch_start = parse_sacct_time(batch_record.get('Start'))
            batch_end = parse_sacct_time(batch_record.get('End'))
        else:
            print(f"Warning: No .batch record found for {calcjob.label or calcjob.pk}, using main job times")
            batch_start = start
            batch_end = end

        if not submit:
            print(f"Warning: No submit time for {calcjob.label or calcjob.pk}")
            continue

        # Get meaningful job name from caller (launcher WorkGraph)
        job_name = None
        if calcjob.caller:
            caller_label = calcjob.caller.label
            # Extract task name from launcher label
            # e.g., "launch_fast_1_date_2026_01_01_00_00_00" -> "fast_1_date_2026_01_01_00_00_00"
            if caller_label and caller_label.startswith('launch_'):
                job_name = caller_label.replace('launch_', '')
            else:
                job_name = caller_label

        # Fallback to calcjob label or pk
        if not job_name:
            job_name = calcjob.label or f"job-{calcjob.pk}"

        print(f"Job {job_name} (PK={calcjob.pk}):")
        print(f"  Main job: Submit: {submit}, Eligible: {eligible}, Start: {start}, End: {end}")
        print(f"  Batch script: Start: {batch_start}, End: {batch_end}")

        job_data_dict = {
            'name': job_name,
            'pk': calcjob.pk,
            'submit': submit,
            'eligible': eligible or submit,  # If no eligible time, assume same as submit
            'start': start or eligible or submit,
            'batch_start': batch_start or start,
            'batch_end': batch_end or end,
            'end': end,
        }
        jobs.append(job_data_dict)

    # Sort jobs based on ordering mode
    if custom_order:
        # Custom sorting for complex workflow: setup, fast branch, medium branch, slow branch, finalize, prepare_next
        def sort_key_complex(job):
            """Sort jobs by workflow structure.

            Within each branch, sort by:
            1. Task number (fast_1 < fast_2 < fast_3)
            2. Cycle date (2026-01 < 2026-02)
            """
            name = job['name']

            # Define task ordering
            task_order = {
                'setup': 0,
                'fast_1': 1,
                'fast_2': 2,
                'fast_3': 3,
                'medium_1': 4,
                'medium_2': 5,
                'medium_3': 6,
                'slow_1': 7,
                'slow_2': 8,
                'slow_3': 9,
                'finalize': 10,
                'prepare_next': 11,
            }

            # Find which task this is
            task_type = None
            for task_name in task_order.keys():
                if task_name in name:
                    task_type = task_name
                    break

            if task_type is None:
                # Unknown task - put at end
                return (999, name)

            # Extract date for secondary sorting (format: date_YYYY_MM_DD)
            date_str = ''
            if 'date_' in name:
                parts = name.split('date_')
                if len(parts) > 1:
                    # Extract YYYY_MM_DD portion
                    date_parts = parts[1].split('_')[:3]
                    date_str = '_'.join(date_parts)

            return (task_order[task_type], date_str, name)

        jobs.sort(key=sort_key_complex)
    else:
        # Default sorting: root first, then alphabetically
        def sort_key(job):
            name = job['name']
            # Root tasks come first (sort key = 0)
            if 'root' in name.lower():
                return (0, name)
            # All other tasks sorted alphabetically (sort key = 1)
            return (1, name)

        jobs.sort(key=sort_key)

    return jobs


def plot_timeline(jobs, output_file=None):
    """Create Gantt chart showing job timeline with colored phases."""

    if not jobs:
        print("No jobs to plot")
        return

    # Set up the plot
    fig, ax = plt.subplots(figsize=(14, max(6, len(jobs) * 0.4)))

    # Define colors for different phases
    COLORS = {
        'blocked': '#e74c3c',    # Red - blocked by dependencies
        'queued': '#f39c12',     # Orange/Yellow - waiting in queue
        'slurm_overhead': '#95a5a6',  # Gray - SLURM system overhead (setup/cleanup)
        'running': '#27ae60',    # Green - actively running
    }

    # Find time range for x-axis
    all_times = []
    for job in jobs:
        all_times.extend([t for t in [job['submit'], job['eligible'], job['start'], job['batch_start'], job['batch_end'], job['end']] if t])

    if not all_times:
        print("No valid timestamps found")
        return

    min_time = min(all_times)
    max_time = max(all_times)

    # Plot each job as a horizontal bar
    for i, job in enumerate(jobs):
        y_pos = len(jobs) - i - 1  # Reverse order so first job is at top

        submit = job['submit']
        eligible = job['eligible']
        start = job['start']
        batch_start = job['batch_start']
        batch_end = job['batch_end']
        end = job['end']

        # Phase 1: Submit → Eligible (blocked by dependencies) - RED
        if eligible > submit:
            width = (eligible - submit).total_seconds() / 3600  # hours
            rect = Rectangle(
                (mdates.date2num(submit), y_pos - 0.4),
                mdates.date2num(eligible) - mdates.date2num(submit),
                0.8,
                facecolor=COLORS['blocked'],
                edgecolor='black',
                linewidth=0.5,
            )
            ax.add_patch(rect)

        # Phase 2: Eligible → Start (queued) - YELLOW
        if start > eligible:
            rect = Rectangle(
                (mdates.date2num(eligible), y_pos - 0.4),
                mdates.date2num(start) - mdates.date2num(eligible),
                0.8,
                facecolor=COLORS['queued'],
                edgecolor='black',
                linewidth=0.5,
            )
            ax.add_patch(rect)

        # Phase 3: Start → Batch Start (SLURM setup overhead) - GRAY
        if batch_start > start:
            rect = Rectangle(
                (mdates.date2num(start), y_pos - 0.4),
                mdates.date2num(batch_start) - mdates.date2num(start),
                0.8,
                facecolor=COLORS['slurm_overhead'],
                edgecolor='black',
                linewidth=0.5,
            )
            ax.add_patch(rect)

        # Phase 4: Batch Start → Batch End (actual execution) - GREEN
        if batch_end and batch_end > batch_start:
            rect = Rectangle(
                (mdates.date2num(batch_start), y_pos - 0.4),
                mdates.date2num(batch_end) - mdates.date2num(batch_start),
                0.8,
                facecolor=COLORS['running'],
                edgecolor='black',
                linewidth=0.5,
            )
            ax.add_patch(rect)
        elif not batch_end:
            # Job still running - extend to current time
            now = datetime.now()
            rect = Rectangle(
                (mdates.date2num(batch_start), y_pos - 0.4),
                mdates.date2num(now) - mdates.date2num(batch_start),
                0.8,
                facecolor=COLORS['running'],
                edgecolor='black',
                linewidth=0.5,
                alpha=0.5,  # Semi-transparent for running jobs
            )
            ax.add_patch(rect)

        # Phase 5: Batch End → End (SLURM cleanup overhead) - GRAY
        if end and batch_end and end > batch_end:
            rect = Rectangle(
                (mdates.date2num(batch_end), y_pos - 0.4),
                mdates.date2num(end) - mdates.date2num(batch_end),
                0.8,
                facecolor=COLORS['slurm_overhead'],
                edgecolor='black',
                linewidth=0.5,
            )
            ax.add_patch(rect)

    # Configure axes
    ax.set_ylim(-0.5, len(jobs) - 0.5)
    ax.set_yticks(range(len(jobs)))

    # Shorten job names: strip workflow prefix and date suffix
    def extract_task_name(full_name):
        """Extract just the task name from full job name.

        Examples:
        - "dynamic_deps_2026_01_07_14_50_root_date_2026_01_01_00_00_00" -> "root"
        - "dynamic_deps_2026_01_07_14_50_fast_1_date_2026_01_01_00_00_00" -> "fast_1"
        """
        # Remove workflow prefix with timestamp (pattern: workflow_YYYY_MM_DD_HH_MM_)
        parts = full_name.split('_')

        # Find where task name starts (after workflow_name_YYYY_MM_DD_HH_MM)
        # Look for pattern: 5 consecutive numeric parts (YYYY MM DD HH MM)
        task_start_idx = 0
        for i in range(len(parts) - 4):
            # Check if we have 5 consecutive numeric parts
            if all(p.isdigit() for p in parts[i:i+5]):
                task_start_idx = i + 5
                break

        if task_start_idx > 0:
            remaining = '_'.join(parts[task_start_idx:])
        else:
            remaining = full_name

        # Remove date suffix (pattern: _date_YYYY_MM_DD_HH_MM_SS)
        if '_date_' in remaining:
            remaining = remaining.split('_date_')[0]

        return remaining

    ax.set_yticklabels([extract_task_name(job['name']) for job in reversed(jobs)])

    # Format x-axis as datetime
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())

    # Set x-axis limits with some padding
    time_range = (max_time - min_time).total_seconds() / 3600
    padding = time_range * 0.05
    ax.set_xlim(
        mdates.date2num(min_time) - padding / 24,
        mdates.date2num(max_time) + padding / 24
    )

    # Rotate x-axis labels for readability
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # Add legend
    legend_elements = [
        Rectangle((0, 0), 1, 1, fc=COLORS['blocked'], label='Blocked (dependencies)'),
        Rectangle((0, 0), 1, 1, fc=COLORS['queued'], label='Queued'),
        Rectangle((0, 0), 1, 1, fc=COLORS['slurm_overhead'], label='SLURM overhead'),
        Rectangle((0, 0), 1, 1, fc=COLORS['running'], label='Running'),
    ]
    ax.legend(handles=legend_elements, loc='upper right')

    # Labels and title
    ax.set_xlabel('Time')
    ax.set_ylabel('Job')
    ax.set_title('Job Timeline: Dependency-blocked → Queued → SLURM Setup → Running → SLURM Cleanup')

    # Grid
    ax.grid(True, axis='x', alpha=0.3)

    plt.tight_layout()

    if output_file:
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"Plot saved to {output_file}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description='Plot job timeline from AiiDA WorkGraph'
    )
    parser.add_argument(
        'node',
        type=str,
        help='Node PK or UUID of the WorkGraph'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Output file path (default: show plot interactively)'
    )
    parser.add_argument(
        '--custom-order',
        action='store_true',
        help='Use custom task ordering for complex workflows (setup, fast, medium, slow, finalize, prepare_next)'
    )

    args = parser.parse_args()

    try:
        jobs = collect_job_data(args.node, custom_order=args.custom_order)

        if not jobs:
            print("No jobs found to plot")
            sys.exit(1)

        print(f"\nPlotting timeline for {len(jobs)} jobs...")
        plot_timeline(jobs, args.output)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
