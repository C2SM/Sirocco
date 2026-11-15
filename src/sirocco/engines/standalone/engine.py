"""Standalone orchestration of a core workflow.

`StandaloneEngine` wraps a `core.Workflow` by composition. The workflow is
the graph, built once per sirocco command from the config. The engine adds
what only the standalone orchestration needs:

- the state table (job id and front rank per task, see `state.py`),
- the propagating front and the completed / cool-down task lists,
- the scheduler, the lock and the state file,
- the special `SiroccoContinueTask`.

The front holds references to the graph's own `core.Task` objects, so no
engine-side copy of the graph exists.
"""

from __future__ import annotations

import enum
import json
import logging
import os
import subprocess
import sys
from colorsys import hls_to_rgb
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, Self, assert_never

from ruamel.yaml import YAML
from termcolor import colored

from sirocco.core.graph_items import TaskStatus
from sirocco.core.workflow import Workflow
from sirocco.engines.standalone.scheduler import DEPENDENCY_TYPE_T, STDOUTERR_MODE_T, Scheduler
from sirocco.engines.standalone.state import NO_ID, StateTable
from sirocco.engines.standalone.tasks.sirocco_task import SiroccoContinueTask
from sirocco.parsing.cycling import OneOffPoint
from sirocco.parsing.yaml_data_models import ConfigSiroccoTask, ConfigWorkflow

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core import Task


class WorkflowStatus(enum.Enum):
    INIT = 0
    CONTINUE = 1
    COMPLETED = 2
    FAILED = 3
    STOPPED = 4
    STOPPING = 5
    RESTART_FAILED = 6


class StatusPoint:
    BASE = "●"
    HUE_FRONT = 216
    COLOR_COMPLETED = (0, 191, 91)
    COLOR_FAILED = (255, 87, 87)

    @classmethod
    def from_status(cls, status: Literal["COMPLETED", "FRONT", "FAILED"], rank: int | None = None) -> str:
        # We generate the colored point in this classmethod and not as class variables otherwise
        # they are defined at import time so that termcolor wiould not take into account the controlling
        # environment variables FORCE_COLOR and NO_COLOR
        match status:
            case "COMPLETED":
                return colored(cls.BASE, cls.COLOR_COMPLETED)  # type: ignore
            case "FAILED":
                return colored(cls.BASE, cls.COLOR_FAILED)  # type: ignore
            case "FRONT":
                if rank is None:
                    msg = "rank is required when asking for a 'FRONT' StatusPoint"
                    raise ValueError(msg)
                # lightness between 0.7 and 0.3
                lightness = 0.7 - min(rank, 3) / 3 * 0.4
                rgb_norm = hls_to_rgb(cls.HUE_FRONT / 360, lightness, 1)
                return colored(cls.BASE, tuple(round(v * 255) for v in rgb_norm))  # type: ignore
            case _:
                assert_never(status)


class StandaloneEngine:
    STATE_FILE_NAME: str = "state.yaml"

    def __init__(
        self,
        workflow: Workflow,
        scheduler: Scheduler,
        config_sirocco_task: ConfigSiroccoTask | None = None,
    ) -> None:
        self.workflow = workflow
        self.scheduler = scheduler
        self.front_depth: int = workflow.front_depth
        self.state = StateTable(default_rank=self.front_depth)
        self.front: list[list[Task]] = [[] for _ in range(self.front_depth)]
        self.completed_tasks: list[Task] = []
        self.cool_down_tasks: list[Task] = []
        self.status: WorkflowStatus = WorkflowStatus.INIT

        config_kwargs = dict(config_sirocco_task) if config_sirocco_task else {}
        config_kwargs.pop("parameters", None)
        self.sirocco_continue_task = SiroccoContinueTask(
            coordinates={},
            cycle_point=OneOffPoint(),
            config_rootdir=workflow.config_rootdir,
            parents=[],
            **config_kwargs,
        )
        self.state[self.sirocco_continue_task].rank = 0

    @classmethod
    def from_config_workflow(cls, config_workflow: ConfigWorkflow) -> Self:
        """Build the core graph and pick the standalone specific settings from the same config"""
        config_sirocco_task = next(
            (task for task in config_workflow.tasks if isinstance(task, ConfigSiroccoTask)),
            None,
        )
        if (scheduler_cls := Scheduler.scheduler_classes.get(config_workflow.scheduler)) is None:
            msg = f"Scheduler {config_workflow.scheduler!r} is not supported by the standalone engine."
            raise ValueError(msg)
        return cls(
            workflow=Workflow.from_config_workflow(config_workflow),
            scheduler=scheduler_cls(),
            config_sirocco_task=config_sirocco_task,
        )

    @classmethod
    def from_config_path(cls, config_path: str | Path, template_context: dict[str, Any] | None = None) -> Self:
        return cls.from_config_workflow(ConfigWorkflow.from_config_path(config_path, template_context=template_context))

    # =========== State shortcuts ===========

    def jobid(self, task: Task) -> str:
        return self.state[task].jobid

    def rank(self, task: Task) -> int:
        return self.state[task].rank

    # =========== Scheduler interface ===========

    @cached_property
    def base_env(self) -> dict[str, str]:
        """Clean login environment from which all tasks are submitted

        Computed on first submission only, so that read-only commands (e.g. stviz) don't spawn a login shell."""

        current_python = sys.executable
        home = os.environ.get("HOME", "")
        user = os.environ.get("USER", "")
        cmd = [
            "env",
            "-i",
            f"HOME={home}",
            f"USER={user}",
            "bash",
            "--login",
            "-c",
            f"'{current_python}' -c 'import os, json; print(json.dumps(dict(os.environ)))'",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)

    def submit(
        self,
        task: Task,
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> None:
        parent_ids = [self.jobid(parent) for parent in task.parents if self.rank(parent) >= 0]
        self.state[task].jobid = self.scheduler.submit(
            task,
            parent_ids=parent_ids,
            env=self.base_env,
            stdouterr_mode=stdouterr_mode,
            dependency_type=dependency_type,
        )

    def get_status(self, task: Task) -> TaskStatus:
        return self.scheduler.get_status(task, self.jobid(task))

    def cancel(self, task: Task) -> None:
        self.scheduler.cancel(task, self.jobid(task))

    # =========== Paths ===========

    @property
    def config_rootdir(self) -> Path:
        return self.workflow.config_rootdir

    @property
    def rundir(self) -> Path:
        return self.workflow.rundir

    @property
    def state_file(self) -> Path:
        return self.rundir / self.STATE_FILE_NAME

    @property
    def lock_file(self) -> Path:
        return self.rundir / SiroccoContinueTask.LOCK_FILE_NAME

    @property
    def locked(self) -> bool:
        return self.lock_file.exists()

    # =========== Methods to control workflow ===========

    def start(self, log_type: Literal["std", "tee"] = "tee") -> None:
        logger = self.get_logger(log_type)
        self.init_front(logger=logger)
        self.auto_submit()
        self.dump_state()

    def continue_wf(self, log_type: Literal["std", "tee"] = "std") -> None:  # NOTE: cannot use "continue"
        logger = self.get_logger(log_type)
        if not self.locked:
            self.lock()
        else:
            msg = "Workflow getting stopped, not continuing"
            logger.info(msg)
            self.status = WorkflowStatus.STOPPING
            return

        self.load_state()
        self.propagate_front(logger=logger)
        self.auto_submit()
        self.dump_state()
        self.unlock()

    def restart(self, log_type: Literal["std", "tee"] = "tee") -> None:
        self.load_state()
        logger = self.get_logger(log_type)
        if self.jobid(self.sirocco_continue_task) != NO_ID:
            msg = "Workflow ongoing, cannot restart"
            logger.error(msg)
            self.status = WorkflowStatus.RESTART_FAILED
            return
        if not self.rundir.exists():
            msg = "Workflow did not start, cannot restart"
            logger.error(msg)
            self.status = WorkflowStatus.RESTART_FAILED
            return
        self.restart_front(logger=logger)
        self.propagate_front(logger=logger)
        self.auto_submit()
        self.dump_state()

    def stop(
        self,
        mode: Literal["cancel", "cool-down"],
        log_type: Literal["std", "tee"] = "tee",
    ) -> None:
        self.load_state()
        if not self.rundir.exists():
            msg = "Workflow did not start, cannot stop"
            raise ValueError(msg)
        logger = self.get_logger(log_type)
        if not self.locked:
            self.lock()
        else:
            msg = "Workflow continuaiton ongoing, please retry"
            logger.info(msg)
            self.status = WorkflowStatus.CONTINUE
            return
        if self.jobid(self.sirocco_continue_task) == NO_ID:
            msg = "Workflow not running, no need to stop"
            logger.error(msg)
            self.unlock()
            return
        self.cancel(self.sirocco_continue_task)
        self.cancel_all_tasks(mode=mode, logger=logger)
        self.status = WorkflowStatus.STOPPED
        self.state[self.sirocco_continue_task].jobid = NO_ID
        self.dump_state()
        self.unlock()

    # =========== Helper methods for workflow control ===========

    def get_logger(self, log_type: Literal["std", "tee"]) -> logging.Logger:
        """Get a logger

        - either for stdout/stderr
        - or for both stdout/stderr and sirocco log file (like tee)"""

        log_formatter = logging.Formatter("[%(levelname)s]  %(message)s")

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)

        logger = logging.getLogger(log_type)
        logger.setLevel(logging.INFO)
        logger.addHandler(console_handler)

        if log_type == "tee":
            file_handler = logging.FileHandler(self.config_rootdir / SiroccoContinueTask.STDOUTERR_FILENAME)
            file_handler.setFormatter(log_formatter)
            logger.addHandler(file_handler)

        return logger

    def front_msg(self, task: Task, action: str) -> str:
        """Log message for a task in the front"""
        rank = self.rank(task)
        return f"{StatusPoint.from_status('FRONT', rank)} {task.label} ({self.jobid(task)}) {action}"

    def lock(self) -> None:
        self.lock_file.touch()

    def unlock(self) -> None:
        self.lock_file.unlink()

    def dump_state(self) -> None:
        YAML().dump(
            {
                "sirocco_jobid": self.jobid(self.sirocco_continue_task),
                "completed_tasks": [self.state.to_yaml(task) for task in self.completed_tasks],
                "cool_down_tasks": [self.state.to_yaml(task) for task in self.cool_down_tasks],
                "front": [[self.state.to_yaml(task) for task in generation] for generation in self.front],
            },
            self.state_file,
        )

    def load_state(self) -> None:
        yaml_state = YAML().load(self.state_file)
        self.state[self.sirocco_continue_task].jobid = yaml_state["sirocco_jobid"]
        self.completed_tasks = [self.task_from_yaml_state(yaml_task, -1) for yaml_task in yaml_state["completed_tasks"]]
        self.cool_down_tasks = [self.task_from_yaml_state(yaml_task, 0) for yaml_task in yaml_state["cool_down_tasks"]]
        for k, yaml_generation in enumerate(yaml_state["front"]):
            self.front[k] = [self.task_from_yaml_state(yaml_task, k) for yaml_task in yaml_generation]

    def task_from_yaml_state(self, yaml_task: dict[str, dict[str, Any]], rank: int) -> Task:
        name, yaml_specs = next(iter(yaml_task.items()))
        coordinates = {k: datetime.fromisoformat(v) if k == "date" else v for k, v in yaml_specs["coordinates"].items()}
        task = self.workflow.tasks[name, coordinates]
        self.state[task].jobid = yaml_specs["jobid"]
        self.state[task].rank = rank
        return task

    def init_front(self, logger: logging.Logger) -> None:
        """Populate front and submit corresponding tasks"""

        for task in self.workflow.tasks:
            if not task.parents:
                self.state[task].rank = 0
                self.submit(task)
                self.front[0].append(task)
                logger.info(self.front_msg(task, f"SUBMITTED to rank {self.rank(task)}"))
        for k in range(self.front_depth - 1):
            for task in self.front[k]:
                for child in task.children:
                    if (max(self.rank(parent) for parent in child.parents) == k) and (
                        self.rank(child) == self.front_depth
                    ):
                        self.submit(child)
                        self.state[child].rank = k + 1
                        self.front[k + 1].append(child)
                        logger.info(self.front_msg(child, f"SUBMITTED to rank {self.rank(child)}"))

    def restart_front(self, logger: logging.Logger) -> None:
        """Submit all tasks currently in the front"""

        for generation in self.front:
            for task in generation:
                # Do not resubmit tasks in cool down mode
                if (
                    self.rank(task) == 0
                    and task in self.cool_down_tasks
                    and self.get_status(task) in (TaskStatus.COMPLETED, TaskStatus.RUNNING, TaskStatus.WAITING)
                ):
                    self.cool_down_tasks.remove(task)
                    logger.info(self.front_msg(task, "CONTINUED as rank 0 from cool-down"))
                else:
                    self.cancel(task)
                    self.submit(task)
                    logger.info(self.front_msg(task, f"SUBMITTED to rank {self.rank(task)}"))

    def propagate_front(self, logger: logging.Logger) -> None:
        """Propagate front of submitted tasks and submit new ones"""

        # Handle first generation of the front
        to_promote: list[Task] = []
        for task in self.front[0]:
            if (status := self.get_status(task)) == TaskStatus.FAILED:
                msg = f"{StatusPoint.from_status('FAILED')} {task.label} ({self.jobid(task)}) FAILED"
                logger.info(msg)
                self.cancel_all_tasks(mode="cancel", logger=logger)
                msg = f"All workflow tasks canceled because {task.label} failed"
                logger.info(msg)
                self.status = WorkflowStatus.FAILED
                return
            if status == TaskStatus.COMPLETED:
                to_promote.append(task)
        for task in to_promote:
            self.state[task].rank = -1
            self.front[0].remove(task)
            self.completed_tasks.append(task)
            msg = f"{StatusPoint.from_status('COMPLETED')} {task.label} ({self.jobid(task)}) COMPLETED"
            logger.info(msg)

        # Update front rank of tasks currently in the front after the first generation
        for k in range(1, self.front_depth):
            to_promote = [task for task in self.front[k] if max(self.rank(parent) for parent in task.parents) == k - 2]
            for task in to_promote:
                self.state[task].rank = k - 1
                self.front[k].remove(task)
                msg = self.front_msg(task, f"PROMOTED from rank {k} to {k - 1}")
                self.front[k - 1].append(task)
                logger.info(msg)

        # Add new tasks to the last generation of the front
        before_last_generation = to_promote if self.front_depth == 1 else self.front[-2]
        for task in before_last_generation:
            for child in task.children:
                if (
                    self.rank(child) == self.front_depth
                    and max(self.rank(parent) for parent in child.parents) == self.front_depth - 2
                ):
                    self.submit(child)
                    self.state[child].rank = self.front_depth - 1
                    self.front[-1].append(child)
                    logger.info(self.front_msg(child, f"SUBMITTED to rank {self.rank(child)}"))

    def cancel_all_tasks(self, mode: Literal["cancel", "cool-down"], logger: logging.Logger) -> None:
        """Cancel all workflow tasks except cool-down tasks"""

        for generation in self.front:
            for task in generation:
                if (
                    self.rank(task) == 0
                    and mode == "cool-down"
                    and self.get_status(task) in (TaskStatus.COMPLETED, TaskStatus.RUNNING)
                ):
                    self.cool_down_tasks.append(task)
                    logger.info(self.front_msg(task, "COOLING DOWN"))
                else:
                    self.cancel(task)
                    msg = f"{StatusPoint.from_status('FAILED')} {task.label} ({self.jobid(task)}) CANCELED"
                    logger.info(msg)

    def auto_submit(self) -> None:
        """Submit next Sirocco task"""

        if self.status == WorkflowStatus.FAILED:
            self.state[self.sirocco_continue_task].jobid = NO_ID
        elif self.front[0]:
            self.sirocco_continue_task.parents = self.front[0]
            self.submit(self.sirocco_continue_task, stdouterr_mode="append", dependency_type="ANY")
            self.status = WorkflowStatus.CONTINUE
        else:
            self.state[self.sirocco_continue_task].jobid = NO_ID
            self.status = WorkflowStatus.COMPLETED
