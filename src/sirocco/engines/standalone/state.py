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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sirocco.core.graph_items import Task

NO_ID = "_NO_ID_"
COMPLETED = -1
"""Rank of the tasks the front already left behind"""


class TaskStatus(enum.Enum):
    """Task status as reported by the scheduler"""

    WAITING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


@dataclass(kw_only=True)
class JobIdTable:
    """Job id of each task, keyed by the task label, `NO_ID` if not submitted"""

    _jobids: dict[str, str] = field(default_factory=dict, init=False)

    def __getitem__(self, task: Task) -> str:
        return self._jobids.get(task.label, NO_ID)

    def __setitem__(self, task: Task, jobid: str) -> None:
        self._jobids[task.label] = jobid


@dataclass(kw_only=True)
class Front:
    """Generations of the propagating front and the tasks it already left behind

    `front[k]` is generation `k`: generation 0 holds the running or queued tasks, the next
    ones the tasks submitted ahead of time (see the orchestration documentation).

    The rank of a task follows from where it is: `k` in generation `k`, `COMPLETED` (-1) once
    completed and `depth` while not submitted yet. Tasks enter the front through `place` and
    only move through `promote`, so the rank of a task and its place in the front always agree.
    """

    depth: int
    completed: list[Task] = field(default_factory=list, init=False)
    _generations: list[list[Task]] = field(init=False)
    _ranks: dict[str, int] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._generations = [[] for _ in range(self.depth)]

    def __getitem__(self, rank: int) -> list[Task]:
        return self._generations[rank]

    def __iter__(self) -> Iterator[list[Task]]:
        return iter(self._generations)

    def rank(self, task: Task) -> int:
        return self._ranks.get(task.label, self.depth)

    def generation_after_parents(self, task: Task) -> int:
        """Lowest front generation `task` can be in: the one after its least advanced parent"""
        return max(self.rank(parent) for parent in task.parents) + 1

    def place(self, task: Task, rank: int) -> None:
        """Put `task`, not in the front yet, in generation `rank` or among the completed tasks if `rank` is `COMPLETED`"""
        if (current_rank := self.rank(task)) != self.depth:
            msg = f"cannot place {task.label}: already at rank {current_rank}"
            raise ValueError(msg)
        if not COMPLETED <= rank < self.depth:
            msg = f"cannot place {task.label} at rank {rank}"
            raise ValueError(msg)
        self._append(task, rank)

    def promote(self, task: Task) -> None:
        """Move `task` one generation down, or to the completed tasks from generation 0."""
        current_rank = self.rank(task)
        if not COMPLETED < current_rank < self.depth:
            msg = f"cannot promote {task.label} from rank {current_rank}"
            raise ValueError(msg)
        self._generations[current_rank].remove(task)
        self._append(task, current_rank - 1)

    def _append(self, task: Task, rank: int) -> None:
        (self.completed if rank == COMPLETED else self._generations[rank]).append(task)
        self._ranks[task.label] = rank
