"""Run-time state of the standalone engine, kept outside of the core graph.

The core graph (`core.Workflow` with its `core.Task` nodes) is built once
from the config and knows nothing about job ids or front ranks. The engine
keeps track of them here, by task label, which is unique per task (name +
coordinates):

- `JobIdTable`: the job id of each submitted task,
- `Front`: which tasks are in which generation of the front and which are
  completed, and so the rank of each task.

Neither holds an entry for tasks the engine has not submitted yet, so no
engine-side copy of the graph is ever built.

`WorkflowState` is the content of the state file itself: a header and task
entries (labels and job ids), without the graph. Stopping a workflow only needs
it, so that it works even when the config is broken or no longer matches the
state. The commands that move the front forward resolve it in the graph.
"""

from __future__ import annotations

import enum
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Self

from ruamel.yaml import YAML

from sirocco.core.graph_items import GraphItem

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from sirocco.core.graph_items import Task

STATE_FILE_NAME = "state.yaml"
NO_ID = "_NO_ID_"
COMPLETED = -1
"""Rank of the tasks the front already left behind"""


class TaskStatus(enum.Enum):
    """Task status as reported by the scheduler"""

    WAITING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class WorkflowStatus(enum.Enum):
    """Status of the workflow, as committed to the state file"""

    RUNNING = 0
    STOPPED = 1
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

    The rank of a task is the generation it is in: `k` in generation `k`, `COMPLETED` (-1) once
    completed and `depth` while not submitted yet. It is recorded only when the task is added
    to a list (`_append`). Tasks enter the front through `place` and only move through
    `promote`, so the rank of a task and its place in the front always agree.
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


@dataclass(kw_only=True)
class StateEntry:
    """Task entry of the state file: the task identity (name and coordinates) and its job id

    Coordinates are kept as written in the file, with dates as ISO strings.
    """

    name: str
    coordinates: dict[str, Any]
    jobid: str

    @property
    def label(self) -> str:
        """Label of the task, as in the graph"""
        return GraphItem.make_label(self.name, self.graph_coordinates)

    @property
    def graph_coordinates(self) -> dict[str, Any]:
        """Coordinates as in the graph, with dates as `datetime`"""
        return {k: datetime.fromisoformat(v) if k == "date" else v for k, v in self.coordinates.items()}

    @classmethod
    def from_task(cls, task: Task, jobid: str) -> Self:
        coordinates = {k: v.isoformat() if k == "date" else v for k, v in task.coordinates.items()}
        return cls(name=task.name, coordinates=coordinates, jobid=jobid)

    @classmethod
    def from_yaml(cls, yaml_entry: dict[str, dict[str, Any]]) -> Self:
        name, specs = next(iter(yaml_entry.items()))
        return cls(name=name, coordinates=dict(specs["coordinates"]), jobid=specs["jobid"])

    def to_yaml(self) -> dict[str, dict[str, Any]]:
        return {self.name: {"coordinates": self.coordinates, "jobid": self.jobid}}


@dataclass(kw_only=True)
class WorkflowState:
    """Content of the state file

    The header identifies the run and its commits:

    - `run`: id of the run, generated by `start`,
    - `revision`: incremented by each commit, from 1 at `start`,
    - `status` and `reason`: status of the workflow and why it stopped when not on user request,
    - `scheduler` and `cluster`: where the jobs of the run live,
    - `committed_by`: transaction id, command and time of the last commit.

    A state file without header, written by an earlier version, reads as revision 0 of an
    unnamed run, with the status derived from the continuation job id.
    """

    run: str | None = None
    revision: int = 0
    status: WorkflowStatus
    reason: str | None = None
    scheduler: str | None = None
    cluster: str | None = None
    committed_by: dict[str, str] | None = None
    sirocco_jobid: str = NO_ID
    completed_tasks: list[StateEntry] = field(default_factory=list)
    cool_down_tasks: list[StateEntry] = field(default_factory=list)
    front: list[list[StateEntry]] = field(default_factory=list)

    @classmethod
    def read(cls, path: Path) -> Self | None:
        """State in `path`, `None` if there is no state file"""
        if not path.exists():
            return None
        yaml_state = YAML(typ="safe", pure=True).load(path)
        front = [[StateEntry.from_yaml(entry) for entry in generation] for generation in yaml_state["front"]]
        sirocco_jobid = yaml_state["sirocco_jobid"]
        if (status := yaml_state.get("status")) is None:
            # No header, written by an earlier version
            if sirocco_jobid != NO_ID:
                status = WorkflowStatus.RUNNING.name
            else:
                status = (WorkflowStatus.STOPPED if any(front) else WorkflowStatus.COMPLETED).name
        return cls(
            run=yaml_state.get("run"),
            revision=yaml_state.get("revision", 0),
            status=WorkflowStatus[status],
            reason=yaml_state.get("reason"),
            scheduler=yaml_state.get("scheduler"),
            cluster=yaml_state.get("cluster"),
            committed_by=yaml_state.get("committed_by"),
            sirocco_jobid=sirocco_jobid,
            completed_tasks=[StateEntry.from_yaml(entry) for entry in yaml_state["completed_tasks"]],
            cool_down_tasks=[StateEntry.from_yaml(entry) for entry in yaml_state["cool_down_tasks"]],
            front=front,
        )

    def write(self, path: Path) -> None:
        """Write the state to `path` atomically

        The state goes to a temporary file, flushed to disk, which then replaces `path`: readers
        see the previous state or the new one, never a partial one.
        """
        yaml_state = {
            "run": self.run,
            "revision": self.revision,
            "status": self.status.name,
            "reason": self.reason,
            "scheduler": self.scheduler,
            "cluster": self.cluster,
            "committed_by": self.committed_by,
            "sirocco_jobid": self.sirocco_jobid,
            "completed_tasks": [entry.to_yaml() for entry in self.completed_tasks],
            "cool_down_tasks": [entry.to_yaml() for entry in self.cool_down_tasks],
            "front": [[entry.to_yaml() for entry in generation] for generation in self.front],
        }
        tmp_path = path.with_name(f".{path.name}.tmp")
        with tmp_path.open("w") as stream:
            YAML().dump(yaml_state, stream)
            stream.flush()
            os.fsync(stream.fileno())
        tmp_path.replace(path)
