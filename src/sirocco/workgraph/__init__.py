"""Sirocco WorkGraph builder - AiiDA workflow orchestration."""

from __future__ import annotations

from collections import deque
from typing import Any

import aiida.orm
from aiida_workgraph import WorkGraph

from sirocco import core
from sirocco.workgraph.builder import WorkGraphBuilder, build_icon_task_spec, build_shell_task_spec
from sirocco.workgraph.launchers import (
    get_job_data,
    launch_icon_task_with_dependency,
    launch_shell_task_with_dependency,
)

__all__ = [
    "WorkGraphBuilder",
    "build_sirocco_workgraph",
    "submit_sirocco_workgraph",
    "run_sirocco_workgraph",
    "get_job_data",
    "launch_icon_task_with_dependency",
    "launch_shell_task_with_dependency",
    "build_icon_task_spec",
    "build_shell_task_spec",
    "compute_topological_levels",
    "get_task_dependencies_from_workgraph",
]


def build_sirocco_workgraph(
    core_workflow: core.Workflow,
    front_depth: int = 1,
) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows.

    Args:
        core_workflow: The core workflow to convert
        front_depth: Number of topological fronts to keep active (default: 1)
                    0 = sequential (wait for level N to finish before submitting N+1)
                    1 = one front ahead (default)
                    high value = streaming submission

    Returns:
        A WorkGraph ready for submission

    Example::

        from sirocco import core
        from sirocco.workgraph import build_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Build the WorkGraph with front_depth=2
        wg = build_sirocco_workgraph(wf, front_depth=2)

        # Submit to AiiDA daemon
        wg.submit()
    """
    builder = WorkGraphBuilder(core_workflow)
    return builder.build(front_depth)


def submit_sirocco_workgraph(
    core_workflow: core.Workflow,
    *,
    inputs: None | dict[str, Any] = None,
    wait: bool = False,
    timeout: int = 60,
    metadata: None | dict[str, Any] = None,
) -> aiida.orm.Node:
    """Build and submit a Sirocco workflow to the AiiDA daemon.

    Args:
        core_workflow: The core workflow to convert and submit
        inputs: Optional inputs to pass to the workgraph
        wait: Whether to wait for completion
        timeout: Timeout in seconds if wait=True
        metadata: Optional metadata for the workgraph

    Returns:
        The AiiDA process node

    Raises:
        RuntimeError: If submission fails

    Example::

        from sirocco import core
        from sirocco.workgraph import submit_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Submit the workflow
        node = submit_sirocco_workgraph(wf)
        print(f"Submitted as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.submit(inputs=inputs, wait=wait, timeout=timeout, metadata=metadata)

    if (output_node := wg.process) is None:
        msg = "Something went wrong when submitting workgraph. Please contact a developer."
        raise RuntimeError(msg)

    return output_node


def run_sirocco_workgraph(
    core_workflow: core.Workflow,
    inputs: None | dict[str, Any] = None,
    metadata: None | dict[str, Any] = None,
) -> aiida.orm.Node:
    """Build and run a Sirocco workflow in a blocking fashion.

    Args:
        core_workflow: The core workflow to convert and run
        inputs: Optional inputs to pass to the workgraph
        metadata: Optional metadata for the workgraph

    Returns:
        The AiiDA process node

    Raises:
        RuntimeError: If execution fails

    Example::

        from sirocco import core
        from sirocco.workgraph import run_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Run the workflow (blocking)
        node = run_sirocco_workgraph(wf)
        print(f"Completed as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.run(inputs=inputs, metadata=metadata)

    if (output_node := wg.process) is None:
        msg = "Something went wrong when running workgraph. Please contact a developer."
        raise RuntimeError(msg)

    return output_node


# =============================================================================
# Utility Functions for WorkGraph Analysis
# =============================================================================


def get_task_dependencies_from_workgraph(wg: WorkGraph) -> dict[str, list[str]]:
    """Extract dependency graph from WorkGraph."""
    deps: dict[str, list[str]] = {}

    # Precompute: get_job_data_X → corresponding launcher task
    launcher_tasks = {t.name: t for t in wg.tasks if t.name.startswith("launch_")}
    get_job_data_to_launcher = {}
    for t in wg.tasks:
        if t.name.startswith("get_job_data_"):
            task_label = t.name.replace("get_job_data_", "")
            # Find the launcher that ends with this task_label
            for launcher_name in launcher_tasks:
                if launcher_name.endswith(f"_{task_label}"):
                    get_job_data_to_launcher[t.name] = launcher_name
                    break

    # Iterate only over launcher tasks
    for task_ in wg.tasks:
        name = task_.name
        if not name.startswith("launch_"):
            continue

        launcher_deps: list[str] = []
        deps[name] = launcher_deps

        sockets = getattr(task_.inputs, "_sockets", None)
        if not sockets:
            continue

        # Iterate over input links → parent tasks
        for socket in sockets.values():
            for link in getattr(socket, "links", []):
                parent_name = link.from_socket.node.name

                # Only care about get_job_data_* parents
                if not parent_name.startswith("get_job_data_"):
                    continue

                parent_launcher = get_job_data_to_launcher.get(parent_name)
                if parent_launcher and parent_launcher not in launcher_deps:
                    launcher_deps.append(parent_launcher)

    return deps


def compute_topological_levels(task_deps: dict[str, list[str]]) -> dict[str, int]:
    """Compute topological level for each task using BFS.

    Level 0 = no dependencies
    Level k = max(parent levels) + 1

    Args:
        task_deps: Dict mapping task_name -> list of parent task names

    Returns:
        Dict mapping task_name -> topological level
    """
    levels = {}
    in_degree = {task_name: len(parents) for task_name, parents in task_deps.items()}

    # Find all tasks with no dependencies (level 0)
    queue = deque([task_name for task_name, degree in in_degree.items() if degree == 0])
    for task_name in queue:
        levels[task_name] = 0

    # Build reverse dependency graph: task -> list of tasks that depend on it
    children: dict[str, list[str]] = {task_name: [] for task_name in task_deps}
    for task_name, parents in task_deps.items():
        for parent in parents:
            if parent not in children:
                children[parent] = []
            children[parent].append(task_name)

    # Process tasks in topological order
    processed = set()
    while queue:
        current = queue.popleft()
        processed.add(current)

        # Update children's levels
        for child in children.get(current, []):
            parents = task_deps[child]
            # Check if all parents have been processed
            if all(p in processed for p in parents):
                # Level is max of all parent levels + 1
                parent_levels = [levels[p] for p in parents]
                levels[child] = max(parent_levels) + 1
                queue.append(child)

    return levels
