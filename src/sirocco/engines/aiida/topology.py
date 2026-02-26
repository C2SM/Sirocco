"""Utility functions for WorkGraph topology analysis."""

from __future__ import annotations

from collections import deque

__all__ = [
    "compute_topological_levels",
]


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
