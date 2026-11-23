"""Standalone orchestration of a core workflow.

`StandaloneEngine` wraps a `core.Workflow` by composition. The workflow is
the graph, built once per sirocco command from the config. The engine adds
what only the standalone orchestration needs:

- the job id of each task and the propagating front (see `state.py`),
- the task handlers (see `plugin_handlers/`) and the data paths they resolve
  (`TaskHandlerTable` and `DataPathTable` below),
- the cool-down task list,
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

from sirocco.core.graph_items import AvailableData, GeneratedData
from sirocco.core.workflow import Workflow
from sirocco.engines.standalone.plugin_handlers import PluginHandler, SiroccoContinueTask
from sirocco.engines.standalone.scheduler import DEPENDENCY_TYPE_T, STDOUTERR_MODE_T, Scheduler
from sirocco.engines.standalone.state import NO_ID, Front, JobIdTable, TaskStatus
from sirocco.parsing.cycling import OneOffPoint
from sirocco.parsing.yaml_data_models import ConfigSiroccoTask, ConfigWorkflow

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco import core


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


class TaskHandlerTable:
    """Handler of each task, keyed by the task label and created on first access"""

    def __init__(self, engine: StandaloneEngine) -> None:
        self.engine = engine
        self._task_handlers: dict[str, PluginHandler] = {}

    def __getitem__(self, task: core.Task) -> PluginHandler:
        if (task_handler := self._task_handlers.get(task.label)) is None:
            task_handler = self._task_handlers[task.label] = PluginHandler.from_core_task(task, self.engine)
        return task_handler


class DataPathTable:
    """Resolved path of each data node, keyed by the data label

    Paths follow from the config only, so they are recomputed by every sirocco command
    and never stored in the state file.

    Generated data paths are resolved on demand: asking for the path of an output that is
    not known yet asks the handler of its origin task to resolve all of its outputs (see
    `PluginHandler.resolve_output_paths`), so that callers never need to care about the
    order in which tasks are processed.
    """

    def __init__(self, task_handlers: TaskHandlerTable) -> None:
        self.task_handlers = task_handlers
        self._paths: dict[str, Path] = {}

    def __getitem__(self, data: core.Data) -> Path:
        if data.label not in self._paths:
            self._resolve(data)
            if data.label not in self._paths:
                msg = f"path of data {data.label} could not be resolved"
                raise ValueError(msg)
        return self._paths[data.label]

    def __setitem__(self, data: core.Data, path: Path) -> None:
        """Set the resolved path of a data node

        Setting it again is allowed, e.g. ICON output ports are applied both when resolving
        output paths and when preparing the task, but only to the same path: a conflicting
        resolution would otherwise silently override the path already used elsewhere.
        """
        if not path.is_absolute():
            msg = "resolved path must be absolute"
            raise ValueError(msg)
        if (current := self._paths.get(data.label)) is not None and current != path:
            msg = f"data {data.label}: resolved to {path} but already resolved to {current}"
            raise ValueError(msg)
        self._paths[data.label] = path

    def _resolve(self, data: core.Data) -> None:
        match data:
            case AvailableData():
                self[data] = data.path
            case GeneratedData() if data.path is not None and data.path.is_absolute():
                self[data] = data.path
            case GeneratedData():
                self.task_handlers[data.origin_task].resolve_output_paths()
            case _:
                msg = f"standalone engine: cannot resolve the path of {type(data).__name__}"
                raise NotImplementedError(msg)


class StandaloneEngine:
    STATE_FILE_NAME: str = "state.yaml"
    RUN_ROOT_NAME: str = "run"

    def __init__(
        self,
        workflow: Workflow,
        scheduler: Scheduler,
        config_sirocco_task: ConfigSiroccoTask,
    ) -> None:
        self.workflow = workflow
        self.scheduler = scheduler
        self.front_depth: int = workflow.front_depth
        self.jobids = JobIdTable()
        self.front = Front(self.front_depth)
        self.task_handlers = TaskHandlerTable(self)
        self.data_paths = DataPathTable(self.task_handlers)
        self.cool_down_tasks: list[core.Task] = []
        self.status: WorkflowStatus = WorkflowStatus.INIT
        self.logger: logging.Logger = logging.getLogger(__name__)  # set by each command

        config_kwargs = dict(config_sirocco_task)
        config_kwargs.pop("parameters", None)
        self.sirocco_continue_task = SiroccoContinueTask(
            coordinates={},
            cycle_point=OneOffPoint(),
            config_rootdir=workflow.config_rootdir,
            parents=[],
            **config_kwargs,
        )

    @classmethod
    def from_config_workflow(cls, config_workflow: ConfigWorkflow) -> Self:
        """Build the core graph and pick the standalone specific settings from the same config"""
        config_sirocco_task = next(
            (task for task in config_workflow.tasks if isinstance(task, ConfigSiroccoTask)),
            None,
        )
        if config_sirocco_task is None:
            msg = (
                "The standalone engine requires the special SIROCCO task to be defined in the tasks section of the"
                " workflow config. It configures the job that resubmits sirocco (computer, account, walltime, venv, ...)."
            )
            raise ValueError(msg)
        if config_workflow.scheduler is None:
            msg = "The standalone engine requires the scheduler to be specified in the workflow config, e.g. `scheduler: slurm`."
            raise ValueError(msg)
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
        task: core.Task,
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> None:
        parent_ids = [self.jobids[parent] for parent in task.parents if self.front.rank(parent) >= 0]
        self.jobids[task] = self.scheduler.submit(
            self.task_handlers[task],
            parent_ids=parent_ids,
            env=self.base_env,
            stdouterr_mode=stdouterr_mode,
            dependency_type=dependency_type,
        )

    def get_status(self, task: core.Task) -> TaskStatus:
        return self.scheduler.get_status(task, self.jobids[task])

    def cancel(self, task: core.Task) -> None:
        self.scheduler.cancel(task, self.jobids[task])

    # =========== Paths ===========

    @property
    def config_rootdir(self) -> Path:
        return self.workflow.config_rootdir

    @property
    def run_root(self) -> Path:
        """Root of all task run directories"""
        return self.config_rootdir / self.RUN_ROOT_NAME

    @property
    def state_file(self) -> Path:
        return self.run_root / self.STATE_FILE_NAME

    @property
    def lock_file(self) -> Path:
        return self.run_root / SiroccoContinueTask.LOCK_FILE_NAME

    @property
    def locked(self) -> bool:
        return self.lock_file.exists()

    # =========== Methods to control workflow ===========

    def start(self, log_type: Literal["std", "tee"] = "tee") -> None:
        self.logger = self.get_logger(log_type)
        self.init_front()
        self.auto_submit()
        self.dump_state()

    def continue_wf(self, log_type: Literal["std", "tee"] = "std") -> None:  # NOTE: cannot use "continue"
        self.logger = self.get_logger(log_type)
        if not self.locked:
            self.lock()
        else:
            self.logger.info("Workflow getting stopped, not continuing")
            self.status = WorkflowStatus.STOPPING
            return

        self.load_state()
        self.propagate_front()
        self.auto_submit()
        self.dump_state()
        self.unlock()

    def restart(self, log_type: Literal["std", "tee"] = "tee") -> None:
        self.logger = self.get_logger(log_type)
        if not self.run_root.exists():
            self.logger.error("Workflow did not start, cannot restart")
            self.status = WorkflowStatus.RESTART_FAILED
            return
        self.load_state()
        if self.jobids[self.sirocco_continue_task] != NO_ID:
            self.logger.error("Workflow ongoing, cannot restart")
            self.status = WorkflowStatus.RESTART_FAILED
            return
        self.restart_front()
        self.propagate_front()
        self.auto_submit()
        self.dump_state()

    def stop(
        self,
        mode: Literal["cancel", "cool-down"],
        log_type: Literal["std", "tee"] = "tee",
    ) -> None:
        if not self.run_root.exists():
            msg = "Workflow did not start, cannot stop"
            raise ValueError(msg)
        self.load_state()
        self.logger = self.get_logger(log_type)
        if not self.locked:
            self.lock()
        else:
            self.logger.info("Workflow continuaiton ongoing, please retry")
            self.status = WorkflowStatus.CONTINUE
            return
        if self.jobids[self.sirocco_continue_task] == NO_ID:
            self.logger.error("Workflow not running, no need to stop")
            self.unlock()
            return
        self.cancel(self.sirocco_continue_task)
        self.cancel_all_tasks(mode=mode)
        self.status = WorkflowStatus.STOPPED
        self.jobids[self.sirocco_continue_task] = NO_ID
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

    def log_task(self, task: core.Task, action: str, status: Literal["COMPLETED", "FRONT", "FAILED"] = "FRONT") -> None:
        """Log what happens to a task, with a point colored after `status` (and the task rank in the front)"""
        point = StatusPoint.from_status(status, self.front.rank(task) if status == "FRONT" else None)
        self.logger.info(f"{point} {task.label} ({self.jobids[task]}) {action}")  # noqa: G004

    def lock(self) -> None:
        self.lock_file.touch()

    def unlock(self) -> None:
        self.lock_file.unlink()

    def dump_state(self) -> None:
        """Write the front, the completed and cool-down tasks and their job ids to the state file

        Workflows are continued across Sirocco versions through this file, so its format is
        guarded by `test_state_file_format`.
        """
        YAML().dump(
            {
                "sirocco_jobid": self.jobids[self.sirocco_continue_task],
                "completed_tasks": [self.task_to_yaml_state(task) for task in self.front.completed],
                "cool_down_tasks": [self.task_to_yaml_state(task) for task in self.cool_down_tasks],
                "front": [[self.task_to_yaml_state(task) for task in generation] for generation in self.front],
            },
            self.state_file,
        )

    def load_state(self) -> None:
        """Restore the front, the completed and cool-down tasks and their job ids"""
        yaml_state = YAML().load(self.state_file)
        self.jobids[self.sirocco_continue_task] = yaml_state["sirocco_jobid"]
        self.front = Front(self.front_depth)
        for yaml_task in yaml_state["completed_tasks"]:
            self.front.add(self.task_from_yaml_state(yaml_task), -1)
        for k, yaml_generation in enumerate(yaml_state["front"]):
            for yaml_task in yaml_generation:
                self.front.add(self.task_from_yaml_state(yaml_task), k)
        # Cool-down tasks are also listed in the front or among the completed tasks, which give their rank
        self.cool_down_tasks = [self.task_from_yaml_state(yaml_task) for yaml_task in yaml_state["cool_down_tasks"]]

    def task_to_yaml_state(self, task: core.Task) -> dict[str, dict[str, Any]]:
        """Task entry of the state file: the task identity (name and coordinates) and its job id"""
        return {
            task.name: {
                "coordinates": {k: v.isoformat() if k == "date" else v for k, v in task.coordinates.items()},
                "jobid": self.jobids[task],
            }
        }

    def task_from_yaml_state(self, yaml_task: dict[str, dict[str, Any]]) -> core.Task:
        """Graph task of a state file entry, its job id restored"""
        name, yaml_specs = next(iter(yaml_task.items()))
        coordinates = {k: datetime.fromisoformat(v) if k == "date" else v for k, v in yaml_specs["coordinates"].items()}
        task = self.workflow.tasks[name, coordinates]
        self.jobids[task] = yaml_specs["jobid"]
        return task

    def add_to_front(self, task: core.Task, rank: int) -> None:
        """Submit `task` and add it to generation `rank` of the front"""
        self.front.add(task, rank)
        self.submit(task)
        self.log_task(task, f"SUBMITTED to rank {rank}")

    def extend_front(self, rank: int, parents: list[core.Task]) -> None:
        """Add to generation `rank` the children of `parents` that belong there and are not submitted yet"""
        for parent in parents:
            for child in parent.children:
                if self.front.rank(child) == self.front_depth and self.front.generation_after_parents(child) == rank:
                    self.add_to_front(child, rank)

    def init_front(self) -> None:
        """Populate front and submit corresponding tasks"""

        for task in self.workflow.tasks:
            if not task.parents:
                self.add_to_front(task, 0)
        for k in range(1, self.front_depth):
            self.extend_front(k, self.front[k - 1])

    def restart_front(self) -> None:
        """Submit all tasks currently in the front"""

        for generation in self.front:
            for task in generation:
                # Do not resubmit tasks in cool down mode
                if (
                    self.front.rank(task) == 0
                    and task in self.cool_down_tasks
                    and self.get_status(task) in (TaskStatus.COMPLETED, TaskStatus.RUNNING, TaskStatus.WAITING)
                ):
                    self.cool_down_tasks.remove(task)
                    self.log_task(task, "CONTINUED as rank 0 from cool-down")
                else:
                    self.cancel(task)
                    self.submit(task)
                    self.log_task(task, f"SUBMITTED to rank {self.front.rank(task)}")

    def propagate_front(self) -> None:
        """Propagate front of submitted tasks and submit new ones"""

        # Handle first generation of the front
        newly_completed: list[core.Task] = []
        for task in self.front[0]:
            if (status := self.get_status(task)) == TaskStatus.FAILED:
                self.log_task(task, "FAILED", status="FAILED")
                self.cancel_all_tasks(mode="cancel")
                self.logger.info(f"All workflow tasks canceled because {task.label} failed")  # noqa: G004
                self.status = WorkflowStatus.FAILED
                return
            if status == TaskStatus.COMPLETED:
                newly_completed.append(task)
        for task in newly_completed:
            self.front.complete(task)
            self.log_task(task, "COMPLETED", status="COMPLETED")

        # Promote tasks of the next generations by one generation once their parents allow it
        for k in range(1, self.front_depth):
            to_promote = [task for task in self.front[k] if self.front.generation_after_parents(task) == k - 1]
            for task in to_promote:
                self.front.promote(task)
                self.log_task(task, f"PROMOTED from rank {k} to {k - 1}")

        # Add new tasks to the last generation of the front, from the generation before it.
        # With a front of depth 1, that generation is the one of the tasks that just completed.
        last = self.front_depth - 1
        self.extend_front(last, newly_completed if last == 0 else self.front[last - 1])

    def cancel_all_tasks(self, mode: Literal["cancel", "cool-down"]) -> None:
        """Cancel all workflow tasks except cool-down tasks"""

        for generation in self.front:
            for task in generation:
                if (
                    self.front.rank(task) == 0
                    and mode == "cool-down"
                    and self.get_status(task) in (TaskStatus.COMPLETED, TaskStatus.RUNNING)
                ):
                    self.cool_down_tasks.append(task)
                    self.log_task(task, "COOLING DOWN")
                else:
                    self.cancel(task)
                    self.log_task(task, "CANCELED", status="FAILED")

    def auto_submit(self) -> None:
        """Submit next Sirocco task"""

        if self.status == WorkflowStatus.FAILED:
            self.jobids[self.sirocco_continue_task] = NO_ID
        elif self.front[0]:
            self.sirocco_continue_task.parents = self.front[0]
            self.submit(self.sirocco_continue_task, stdouterr_mode="append", dependency_type="ANY")
            self.status = WorkflowStatus.CONTINUE
        else:
            self.jobids[self.sirocco_continue_task] = NO_ID
            self.status = WorkflowStatus.COMPLETED
