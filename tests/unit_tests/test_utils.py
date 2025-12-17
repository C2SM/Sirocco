"""Test utility functions for dynamic level and pre-submission testing.

This module provides reusable helper functions for testing WorkGraph execution,
dynamic level computation, and pre-submission behavior.
"""

from datetime import datetime
from typing import Any

from aiida.orm import ProcessNode
from aiida_workgraph.orm.workgraph import WorkGraphNode


def extract_launcher_times(workgraph_process: ProcessNode) -> dict[str, dict[str, Any]]:
    """Extract launcher WorkGraph creation and completion times.

    Analyzes launcher sub-WorkGraphs to determine when tasks were submitted
    (ctime) and when they completed (mtime). This is essential for validating
    dynamic level computation and pre-submission behavior.

    Args:
        workgraph_process: The main WorkGraph process node

    Returns:
        Dictionary mapping task names to timing info:
        {
            'task_name': {
                'label': 'launch_task_name_...',
                'ctime': datetime,  # When launcher was created (task submitted)
                'mtime': datetime,  # When launcher finished (task completed)
                'branch': 'fast'|'slow'|'medium'|'root',
                'pk': int
            }
        }

    Example:
        >>> times = extract_launcher_times(wg.process)
        >>> fast_3_submit_time = times['fast_3']['ctime']
        >>> slow_3_submit_time = times['slow_3']['ctime']
        >>> assert fast_3_submit_time < slow_3_submit_time  # fast submitted first
    """
    timing_data = {}

    for desc in workgraph_process.called_descendants:
        if isinstance(desc, WorkGraphNode) and desc.label.startswith('launch_'):
            # Parse task name from label: "launch_fast_1_date_..." → "fast_1"
            label_parts = desc.label.split('_')
            if len(label_parts) >= 3:
                # Handle different task naming patterns
                if label_parts[1] in ['root', 'fast', 'slow', 'medium', 'setup', 'finalize']:
                    if label_parts[1] in ['root', 'setup', 'finalize']:
                        task_name = label_parts[1]
                        branch = label_parts[1]
                    else:
                        # For numbered tasks like fast_1, medium_2, slow_3
                        task_name = f"{label_parts[1]}_{label_parts[2]}"
                        branch = label_parts[1]

                    timing_data[task_name] = {
                        'label': desc.label,
                        'ctime': desc.ctime,  # Creation time = task submission
                        'mtime': desc.mtime,  # Modification time = task completion
                        'branch': branch,
                        'pk': desc.pk
                    }

    return timing_data


def extract_task_completion_times(workgraph_process: ProcessNode) -> dict[str, datetime]:
    """Extract actual task execution completion times.

    This extracts the completion times of the actual task ProcessNodes (not
    the launcher WorkGraphs), which represents when the shell job or CalcJob
    finished executing.

    Args:
        workgraph_process: The main WorkGraph process node

    Returns:
        Dictionary mapping task names to completion timestamps:
        {
            'fast_1': datetime,
            'slow_2': datetime,
            ...
        }

    Note:
        This is different from extract_launcher_times() - launcher times show
        when the WorkGraph engine submitted tasks, while completion times show
        when the actual jobs finished.
    """
    task_times = {}

    for node in workgraph_process.called_descendants:
        if isinstance(node, ProcessNode):
            # Get the label which contains the task name
            task_label = getattr(node, 'label', '') or getattr(node, 'process_label', '')
            if task_label:
                # Filter for task nodes (not launcher WorkGraphs)
                prefixes = ["fast_", "slow_", "medium_", "root", "setup", "finalize"]
                if any(task_label.startswith(prefix) for prefix in prefixes):
                    # Only include if it's not a launcher (those have 'launch_' prefix)
                    if not task_label.startswith('launch_'):
                        task_times[task_label] = node.mtime

    return task_times


def compute_relative_times(timing_data: dict[str, dict[str, Any]]) -> dict[str, dict[str, float]]:
    """Convert absolute timestamps to relative times from workflow start.

    Args:
        timing_data: Output from extract_launcher_times()

    Returns:
        Dictionary with relative times in seconds:
        {
            'task_name': {
                'start': float,  # Seconds from workflow start
                'end': float,    # Seconds from workflow start
                'duration': float,  # Task duration in seconds
                'branch': str
            }
        }
    """
    if not timing_data:
        return {}

    # Find workflow start time (earliest ctime)
    workflow_start = min(info['ctime'] for info in timing_data.values())

    relative_times = {}
    for task_name, info in timing_data.items():
        start_rel = (info['ctime'] - workflow_start).total_seconds()
        end_rel = (info['mtime'] - workflow_start).total_seconds()

        relative_times[task_name] = {
            'start': start_rel,
            'end': end_rel,
            'duration': end_rel - start_rel,
            'branch': info['branch']
        }

    return relative_times


def assert_branch_independence(
    timing_data: dict[str, dict[str, Any]],
    fast_branch: str = 'fast',
    slow_branch: str = 'slow',
    message_prefix: str = ""
) -> None:
    """Assert that the fast branch completed before the slow branch.

    This is the key test for dynamic level computation - branches should
    advance independently without waiting for each other at the same
    topological level.

    Args:
        timing_data: Output from extract_launcher_times()
        fast_branch: Name of the fast branch (default: 'fast')
        slow_branch: Name of the slow branch (default: 'slow')
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If fast branch did not complete before slow branch

    Example:
        >>> times = extract_launcher_times(wg.process)
        >>> assert_branch_independence(times)  # Validates fast < slow
    """
    # Find last task from each branch
    fast_tasks = {name: info for name, info in timing_data.items()
                  if info['branch'] == fast_branch}
    slow_tasks = {name: info for name, info in timing_data.items()
                  if info['branch'] == slow_branch}

    assert fast_tasks, f"{message_prefix}No tasks found for branch '{fast_branch}'"
    assert slow_tasks, f"{message_prefix}No tasks found for branch '{slow_branch}'"

    # Get completion times of last tasks
    last_fast_task = max(fast_tasks.items(), key=lambda x: x[1]['mtime'])
    last_slow_task = max(slow_tasks.items(), key=lambda x: x[1]['mtime'])

    last_fast_name, last_fast_info = last_fast_task
    last_slow_name, last_slow_info = last_slow_task

    assert last_fast_info['mtime'] < last_slow_info['mtime'], (
        f"{message_prefix}Fast branch should complete before slow branch. "
        f"Fast: {last_fast_name} at {last_fast_info['mtime']}, "
        f"Slow: {last_slow_name} at {last_slow_info['mtime']}"
    )


def assert_pre_submission_occurred(
    timing_data: dict[str, dict[str, Any]],
    task: str,
    dependency: str,
    message_prefix: str = ""
) -> None:
    """Assert that a task was submitted before its dependency finished.

    This validates pre-submission behavior (window_size > 0), where tasks can
    be submitted before their dependencies complete.

    Args:
        timing_data: Output from extract_launcher_times()
        task: Name of the task that should be pre-submitted
        dependency: Name of the dependency task
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If task was not submitted before dependency finished

    Example:
        >>> times = extract_launcher_times(wg.process)
        >>> # With window_size=1, fast_2 should be submitted before fast_1 finishes
        >>> assert_pre_submission_occurred(times, 'fast_2', 'fast_1')
    """
    assert task in timing_data, f"{message_prefix}Task '{task}' not found in timing data"
    assert dependency in timing_data, f"{message_prefix}Dependency '{dependency}' not found"

    task_submit = timing_data[task]['ctime']
    dep_finish = timing_data[dependency]['mtime']

    assert task_submit < dep_finish, (
        f"{message_prefix}Pre-submission failed: {task} should be submitted before "
        f"{dependency} finishes. Task submitted at {task_submit}, "
        f"dependency finished at {dep_finish}"
    )


def assert_submission_order(
    timing_data: dict[str, dict[str, Any]],
    task_order: list[str],
    message_prefix: str = ""
) -> None:
    """Assert that tasks were submitted in the specified order.

    This is useful for validating that dynamic level computation produces
    the expected submission sequence.

    Args:
        timing_data: Output from extract_launcher_times()
        task_order: List of task names in expected submission order
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If tasks were not submitted in the specified order

    Example:
        >>> times = extract_launcher_times(wg.process)
        >>> # Verify submission order
        >>> assert_submission_order(times, ['root', 'fast_1', 'fast_2', 'fast_3'])
    """
    for i in range(len(task_order) - 1):
        earlier = task_order[i]
        later = task_order[i + 1]

        assert earlier in timing_data, f"{message_prefix}Task '{earlier}' not found"
        assert later in timing_data, f"{message_prefix}Task '{later}' not found"

        earlier_time = timing_data[earlier]['ctime']
        later_time = timing_data[later]['ctime']

        assert earlier_time <= later_time, (
            f"{message_prefix}Submission order violation: {earlier} should be "
            f"submitted before {later}. {earlier} at {earlier_time}, "
            f"{later} at {later_time}"
        )


def assert_cross_dependency_respected(
    timing_data: dict[str, dict[str, Any]],
    dependent_task: str,
    dependency_tasks: list[str],
    message_prefix: str = ""
) -> None:
    """Assert that a task started after ALL its cross-branch dependencies finished.

    Cross-dependencies between branches should be properly enforced even with
    dynamic level computation.

    Args:
        timing_data: Output from extract_launcher_times()
        dependent_task: Task that depends on others
        dependency_tasks: List of tasks that dependent_task depends on
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If dependent task started before all dependencies finished

    Example:
        >>> times = extract_launcher_times(wg.process)
        >>> # medium_2 depends on both medium_1 AND fast_2
        >>> assert_cross_dependency_respected(times, 'medium_2', ['medium_1', 'fast_2'])
    """
    assert dependent_task in timing_data, (
        f"{message_prefix}Task '{dependent_task}' not found"
    )

    dependent_submit = timing_data[dependent_task]['ctime']

    for dep in dependency_tasks:
        assert dep in timing_data, f"{message_prefix}Dependency '{dep}' not found"

        dep_finish = timing_data[dep]['mtime']

        assert dep_finish <= dependent_submit, (
            f"{message_prefix}Cross-dependency violation: {dependent_task} "
            f"submitted before {dep} finished. {dependent_task} at {dependent_submit}, "
            f"{dep} finished at {dep_finish}"
        )


def print_timing_summary(timing_data: dict[str, dict[str, Any]]) -> None:
    """Print a formatted summary of task timing data.

    Useful for debugging test failures or understanding workflow execution.

    Args:
        timing_data: Output from extract_launcher_times()
    """
    relative_times = compute_relative_times(timing_data)

    print("\n" + "="*80)
    print("TASK SUBMISSION AND COMPLETION TIMELINE")
    print("="*80)
    print(f"{'Task':<15} {'Branch':<10} {'Submit (s)':<12} {'Complete (s)':<12} {'Duration (s)':<12}")
    print("-"*80)

    # Sort by submission time
    sorted_tasks = sorted(relative_times.items(), key=lambda x: x[1]['start'])

    for task_name, times in sorted_tasks:
        print(f"{task_name:<15} {times['branch']:<10} {times['start']:>10.1f}  "
              f"{times['end']:>11.1f}  {times['duration']:>11.1f}")

    print("="*80)
