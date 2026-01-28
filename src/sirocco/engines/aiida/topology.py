"""Utility functions for WorkGraph topology analysis."""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiida_workgraph import WorkGraph

__all__ = [
    "compute_topological_levels",
    "get_task_dependencies_from_workgraph",
]


def get_task_dependencies_from_workgraph(wg: WorkGraph) -> dict[str, list[str]]:
    """Extract dependency graph from WorkGraph.

    Args:
        wg: The WorkGraph to analyze

    Returns:
        Dict mapping launcher task names to their parent launcher dependencies
    """
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
