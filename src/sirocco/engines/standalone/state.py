"""Run-time state of the standalone engine, kept outside of the core graph.

The core graph (`core.Workflow` with its `core.Task` nodes) is built once
from the config and knows nothing about job ids or front ranks. The engine
keeps that information in a side table keyed by the task label, which is
unique per task (name + coordinates).

Only tasks the engine has touched get an entry with a job id. Untouched tasks
simply report the default state (no job id, rank = front_depth), so no
engine-side copy of the graph is ever built.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sirocco.core.graph_items import Task

NO_ID = "_NO_ID_"


class TaskStatus(enum.Enum):
    """Task status as reported by the scheduler"""

    WAITING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


@dataclass
class TaskState:
    jobid: str = NO_ID
    rank: int = 0


class StateTable:
    def __init__(self, default_rank: int) -> None:
        self.default_rank = default_rank
        self._states: dict[str, TaskState] = {}

    def __getitem__(self, task: Task) -> TaskState:
        """Return the state of `task`, creating the default one on first access."""
        if (state := self._states.get(task.label)) is None:
            state = self._states[task.label] = TaskState(rank=self.default_rank)
        return state
