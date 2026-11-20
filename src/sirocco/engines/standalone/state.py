"""Run-time state of the standalone engine, kept outside of the core graph.

The core graph (`core.Workflow` with its `core.Task` nodes) is built once
from the config and knows nothing about job ids or front ranks. The engine
keeps track of them here, by task label, which is unique per task (name +
coordinates):

- `JobIdTable`: the job id of each submitted task,
- `Front`: which tasks are in which generation of the front and which are
  completed, from which the rank of each task follows.

Neither holds an entry for tasks the engine has not submitted yet, so no
engine-side copy of the graph is ever built.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sirocco.core.graph_items import Task

NO_ID = "_NO_ID_"


class TaskStatus(enum.Enum):
    """Task status as reported by the scheduler"""

    WAITING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class JobIdTable:
    """Job id of each task, keyed by the task label, `NO_ID` if not submitted"""

    def __init__(self) -> None:
        self._jobids: dict[str, str] = {}

    def __getitem__(self, task: Task) -> str:
        return self._jobids.get(task.label, NO_ID)

    def __setitem__(self, task: Task, jobid: str) -> None:
        self._jobids[task.label] = jobid


class Front:
    """Generations of the propagating front and the tasks it already left behind

    `front[k]` is generation `k`: generation 0 holds the running or queued tasks, the next
    ones the tasks submitted ahead of time (see the orchestration documentation).

    The rank of a task follows from where it is: `k` in generation `k`, -1 once completed
    and `depth` while not submitted yet. Tasks only move through the methods below, so the
    rank of a task and its place in the front always agree.
    """

    def __init__(self, depth: int) -> None:
        self.depth = depth
        self.completed: list[Task] = []
        self._generations: list[list[Task]] = [[] for _ in range(depth)]
        self._ranks: dict[str, int] = {}

    def __getitem__(self, rank: int) -> list[Task]:
        return self._generations[rank]

    def __iter__(self) -> Iterator[list[Task]]:
        return iter(self._generations)

    def rank(self, task: Task) -> int:
        return self._ranks.get(task.label, self.depth)

    def generation_after_parents(self, task: Task) -> int:
        """Lowest front generation `task` can be in: the one after its least advanced parent"""
        return max(self.rank(parent) for parent in task.parents) + 1

    def add(self, task: Task, rank: int) -> None:
        """Add `task` to generation `rank`, or to the completed tasks if `rank` is -1"""
        if rank == -1:
            self.completed.append(task)
        else:
            self._generations[rank].append(task)
        self._ranks[task.label] = rank

    def promote(self, task: Task) -> None:
        """Move `task` from its generation `k` to generation `k - 1`"""
        rank = self.rank(task)
        self._generations[rank].remove(task)
        self.add(task, rank - 1)

    def complete(self, task: Task) -> None:
        """Move `task` from generation 0 to the completed tasks"""
        self._generations[0].remove(task)
        self.add(task, -1)
