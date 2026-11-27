"""Standalone orchestration of a core workflow.

`StandaloneEngine` wraps a `core.Workflow` by composition. The workflow is
the graph, built once per sirocco command from the config. The engine adds
what only the standalone orchestration needs:

- the job id of each task and the propagating front (see `state.py`),
- the task handlers (see `plugin_handlers/`) and the data paths they resolve
  (see `tables.py`),
- the cool-down task list,
- the scheduler and the special `SiroccoContinueTask`.

The front holds references to the graph's own `core.Task` objects, so no
engine-side copy of the graph exists.

Each command runs in a transaction (see `transaction.py`), which holds the lock and
commits the state. The commands that move the front forward (`start`, `continue`,
`restart`) are engine methods, as they need the graph. `stop` and the cleanup of
`start --cleanup` are functions working on the state file alone, so that they work
even when the config is broken or no longer matches the state.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from colorsys import hls_to_rgb
from dataclasses import dataclass, field, replace
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, assert_never

from termcolor import colored

from sirocco.core.workflow import Workflow
from sirocco.engines.standalone.plugin_handlers import SiroccoContinueTask
from sirocco.engines.standalone.scheduler import DEPENDENCY_TYPE_T, STDOUTERR_MODE_T, Scheduler, UnknownJobError
from sirocco.engines.standalone.state import (
    COMPLETED,
    NO_ID,
    STATE_FILE_NAME,
    Front,
    JobIdTable,
    StateEntry,
    TaskStatus,
    WorkflowState,
    WorkflowStatus,
)
from sirocco.engines.standalone.tables import DataPathTable, TaskHandlerTable
from sirocco.engines.standalone.transaction import CONTROL_DIR_NAME, CommandOutcome, Outcome
from sirocco.parsing.cycling import OneOffPoint
from sirocco.parsing.yaml_data_models import ConfigSiroccoTask, ConfigWorkflow

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco import core
    from sirocco.engines.standalone.transaction import Transaction

type LOG_TYPE_T = Literal["std", "tee"]
type STOP_MODE_T = Literal["cancel", "cool-down"]
type POINT_STATUS_T = Literal["COMPLETED", "FRONT", "FAILED"]


class StatusPoint:
    BASE = "●"
    HUE_FRONT = 216
    COLOR_COMPLETED = (0, 191, 91)
    COLOR_FAILED = (255, 87, 87)

    @classmethod
    def from_status(cls, status: POINT_STATUS_T, rank: int | None = None) -> str:
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


def get_logger(log_type: LOG_TYPE_T, config_rootdir: Path) -> logging.Logger:
    """Get the logger of a command, configured anew at each call

    - either for stdout/stderr
    - or for both stdout/stderr and sirocco log file (like tee)

    Call it once per command and pass the logger down: the handlers of a previous call, maybe for another
    `config_rootdir`, are removed.
    """
    logger = logging.getLogger(log_type)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    log_formatter = logging.Formatter("[%(levelname)s]  %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    logger.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    if log_type == "tee":
        file_handler = logging.FileHandler(config_rootdir / SiroccoContinueTask.STDOUTERR_FILENAME)
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    return logger


def task_line(label: str, jobid: str, action: str, status: POINT_STATUS_T, rank: int) -> str:
    """Log line of what happens to a task, with a point colored after `status` (and `rank` in the front)"""
    point = StatusPoint.from_status(status, rank if status == "FRONT" else None)
    return f"{point} {label} ({jobid}) {action}"


@dataclass(kw_only=True)
class StandaloneEngine:
    STATE_FILE_NAME: ClassVar[str] = STATE_FILE_NAME
    RUN_ROOT_NAME: ClassVar[str] = "run"

    workflow: Workflow = field(repr=False)
    scheduler: Scheduler
    sirocco_continue_task: SiroccoContinueTask = field(repr=False)

    jobids: JobIdTable = field(default_factory=JobIdTable, init=False)
    front: Front = field(init=False)
    task_handlers: TaskHandlerTable = field(init=False, repr=False)
    data_paths: DataPathTable = field(init=False, repr=False)
    cool_down_tasks: list[core.Task] = field(default_factory=list, init=False)
    status: WorkflowStatus = field(default=WorkflowStatus.RUNNING, init=False)
    logger: logging.Logger = field(
        default_factory=lambda: logging.getLogger(__name__), init=False, repr=False
    )  # set by each command

    def __post_init__(self) -> None:
        self.front = Front(depth=self.front_depth)
        self.data_paths = DataPathTable()
        self.task_handlers = TaskHandlerTable(run_root=self.run_root, data_paths=self.data_paths)
        self.data_paths.task_handlers = self.task_handlers

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
        scheduler = scheduler_class(config_workflow.scheduler)()
        workflow = Workflow.from_config_workflow(config_workflow)
        config_kwargs = dict(config_sirocco_task)
        config_kwargs.pop("parameters", None)
        sirocco_continue_task = SiroccoContinueTask(
            coordinates={},
            cycle_point=OneOffPoint(),
            config_rootdir=workflow.config_rootdir,
            parents=[],
            **config_kwargs,
        )
        return cls(workflow=workflow, scheduler=scheduler, sirocco_continue_task=sirocco_continue_task)

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
        return self.scheduler.get_status(self.jobids[task])

    def cancel(self, task: core.Task) -> None:
        if (jobid := self.jobids[task]) == NO_ID:
            msg = f"task {task.label} cannot be canceled as it does not have a jobid"
            raise ValueError(msg)
        self.scheduler.cancel(jobid)

    # =========== Paths ===========

    @property
    def front_depth(self) -> int:
        return self.workflow.front_depth

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

    # =========== Methods to control the workflow, each in a transaction ===========

    def start(self, transaction: Transaction, logger: logging.Logger) -> WorkflowState:
        self.logger = logger
        if transaction.state is not None:
            msg = "Workflow already exists, cannot start. Use --cleanup to clean up before starting."
            raise CommandOutcome(Outcome.REFUSED, msg)
        self.init_front()
        self.auto_submit()
        return transaction.commit(self.workflow_state())

    def continue_wf(self, transaction: Transaction, logger: logging.Logger) -> WorkflowState:
        """Continue the workflow, from its continuation job only

        The continuation job only acts if the committed state names it: a continuation job of a stopped workflow,
        or submitted by a command that failed before committing, is a no-op.
        """  # NOTE: cannot use "continue"
        self.logger = logger
        if (state := transaction.state) is None:
            raise CommandOutcome(Outcome.REFUSED, "Workflow did not start, cannot continue")
        if (jobid := Scheduler.current_jobid()) is None:
            msg = "`sirocco continue` only runs in the continuation job of the workflow"
            raise CommandOutcome(Outcome.REFUSED, msg)
        if state.sirocco_jobid != jobid:
            msg = f"Continuation job {jobid} superseded, the workflow is {state.status.name.lower()}"
            if state.sirocco_jobid != NO_ID:
                msg += f" with continuation job {state.sirocco_jobid}"
            raise CommandOutcome(Outcome.NO_OP, msg)
        self.load_state(state)
        self.propagate_front()
        self.auto_submit()
        return transaction.commit(self.workflow_state())

    def restart(self, transaction: Transaction, logger: logging.Logger) -> WorkflowState:
        self.logger = logger
        if (state := transaction.state) is None:
            raise CommandOutcome(Outcome.REFUSED, "Workflow did not start, cannot restart")
        if state.status == WorkflowStatus.RUNNING:
            raise CommandOutcome(Outcome.REFUSED, "Workflow ongoing, cannot restart")
        self.load_state(state)
        self.restart_front()
        self.propagate_front()
        self.auto_submit()
        return transaction.commit(self.workflow_state())

    # =========== Helper methods for workflow control ===========

    def log_task(self, task: core.Task, action: str, status: POINT_STATUS_T = "FRONT") -> None:
        """Log what happens to a task, with a point colored after `status` (and the task rank in the front)"""
        self.logger.info(task_line(task.label, self.jobids[task], action, status, self.front.rank(task)))

    def workflow_state(self) -> WorkflowState:
        """State to commit: front, completed and cool-down tasks and their job ids

        The transaction sets the run, revision and commit of the header.
        """
        return WorkflowState(
            status=self.status,
            scheduler=self.scheduler.name,
            cluster=self.scheduler.cluster(),
            sirocco_jobid=self.jobids[self.sirocco_continue_task],
            completed_tasks=[self.state_entry(task) for task in self.front.completed],
            cool_down_tasks=[self.state_entry(task) for task in self.cool_down_tasks],
            front=[[self.state_entry(task) for task in generation] for generation in self.front],
        )

    def load_state(self, state: WorkflowState) -> None:
        """Restore the front, the completed and cool-down tasks and their job ids from `state`"""
        self.jobids[self.sirocco_continue_task] = state.sirocco_jobid
        self.status = state.status
        self.front = Front(depth=self.front_depth)
        for entry in state.completed_tasks:
            self.front.place(self.task_of(entry), COMPLETED)
        for k, generation in enumerate(state.front):
            for entry in generation:
                self.front.place(self.task_of(entry), k)
        # Cool-down tasks are also listed in the front or among the completed tasks, which give their rank
        self.cool_down_tasks = [self.task_of(entry) for entry in state.cool_down_tasks]

    def state_entry(self, task: core.Task) -> StateEntry:
        """State file entry of `task`, with its job id"""
        return StateEntry.from_task(task, self.jobids[task])

    def task_of(self, entry: StateEntry) -> core.Task:
        """Graph task of a state file entry, its job id restored"""
        try:
            task = self.workflow.tasks[entry.name, entry.graph_coordinates]
        except (KeyError, ValueError) as e:
            msg = (
                f"The workflow state names task {entry.label}, which the config doesn't define anymore."
                " Start the workflow again with --cleanup."
            )
            raise CommandOutcome(Outcome.REFUSED, msg) from e
        self.jobids[task] = entry.jobid
        return task

    def add_to_front(self, task: core.Task, rank: int) -> None:
        """Submit `task` and add it to generation `rank` of the front"""
        self.front.place(task, rank)
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
                self.cancel_front()
                self.logger.info(f"All workflow tasks canceled because {task.label} failed")  # noqa: G004
                self.status = WorkflowStatus.FAILED
                return
            if status == TaskStatus.COMPLETED:
                newly_completed.append(task)
        for task in newly_completed:
            self.front.promote(task)
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

    def cancel_front(self) -> None:
        """Cancel all tasks of the front"""
        for generation in self.front:
            for task in generation:
                self.cancel(task)
                self.log_task(task, "CANCELED", status="FAILED")

    def auto_submit(self) -> None:
        """Submit next Sirocco task"""
        if self.status == WorkflowStatus.FAILED:
            self.jobids[self.sirocco_continue_task] = NO_ID
        elif self.front[0]:
            self.sirocco_continue_task.parents = self.front[0]
            self.submit(self.sirocco_continue_task, stdouterr_mode="append", dependency_type="ANY")
            self.status = WorkflowStatus.RUNNING
        else:
            self.jobids[self.sirocco_continue_task] = NO_ID
            self.status = WorkflowStatus.COMPLETED


# =========== Commands working on the state file alone ===========


def scheduler_class(name: str | None) -> type[Scheduler]:
    if name is None:
        msg = "The standalone engine requires the scheduler to be specified in the workflow config, e.g. `scheduler: slurm`."
        raise ValueError(msg)
    if (scheduler_cls := Scheduler.scheduler_classes.get(name)) is None:
        msg = f"Scheduler {name!r} is not supported by the standalone engine."
        raise ValueError(msg)
    return scheduler_cls


def state_scheduler(state: WorkflowState, config_rootdir: Path) -> Scheduler:
    """Scheduler holding the jobs of `state`, from the config for a state file without header"""
    name = state.scheduler if state.scheduler is not None else ConfigWorkflow.from_config_path(config_rootdir).scheduler
    return scheduler_class(name)()


def stop(transaction: Transaction, mode: STOP_MODE_T, logger: logging.Logger) -> WorkflowState:
    """Stop the workflow: cancel its continuation job and its front tasks

    In cool-down mode, rank 0 tasks that run or completed are kept. Works on the state file alone.
    """
    config_rootdir = transaction.run_root.parent
    if (state := transaction.state) is None:
        raise CommandOutcome(Outcome.REFUSED, "Workflow did not start, cannot stop")
    if state.sirocco_jobid == NO_ID:
        raise CommandOutcome(Outcome.NO_OP, "Workflow not running, no need to stop")
    scheduler = state_scheduler(state, config_rootdir)
    scheduler.cancel(state.sirocco_jobid)
    cool_down_tasks = list(state.cool_down_tasks)
    for rank, generation in enumerate(state.front):
        for entry in generation:
            if (
                rank == 0
                and mode == "cool-down"
                and scheduler.get_status(entry.jobid) in (TaskStatus.COMPLETED, TaskStatus.RUNNING)
            ):
                cool_down_tasks.append(entry)
                logger.info(task_line(entry.label, entry.jobid, "COOLING DOWN", "FRONT", rank))
            else:
                scheduler.cancel(entry.jobid)
                logger.info(task_line(entry.label, entry.jobid, "CANCELED", "FAILED", rank))
    stopped = replace(
        state, status=WorkflowStatus.STOPPED, reason=None, sirocco_jobid=NO_ID, cool_down_tasks=cool_down_tasks
    )
    return transaction.commit(stopped)


def clean_up(transaction: Transaction) -> None:
    """Delete the state and everything else the workflow generated, for `start --cleanup`

    Refused while jobs of the workflow may run. Works on the state file alone.
    """
    config_rootdir = transaction.run_root.parent
    if (state := transaction.state) is not None:
        if state.status == WorkflowStatus.RUNNING:
            raise CommandOutcome(Outcome.REFUSED, "Workflow running, stop it before cleaning it up")
        scheduler = state_scheduler(state, config_rootdir)
        for entry in state.cool_down_tasks:
            try:
                status = scheduler.get_status(entry.jobid)
            except UnknownJobError:
                continue
            if status in (TaskStatus.WAITING, TaskStatus.RUNNING):
                msg = f"Task {entry.label} (job {entry.jobid}) still runs since a cool-down stop, cannot clean up"
                raise CommandOutcome(Outcome.REFUSED, msg)
    # The state first: an interrupted cleanup leaves a workflow that didn't start
    transaction.discard_state()
    for item in transaction.run_root.iterdir():
        if item.name == CONTROL_DIR_NAME:
            continue
        if item.is_dir() and not item.is_symlink():
            shutil.rmtree(item)
        else:
            item.unlink()
    (config_rootdir / SiroccoContinueTask.STDOUTERR_FILENAME).unlink(missing_ok=True)
    (config_rootdir / SiroccoContinueTask.SUBMIT_FILENAME).unlink(missing_ok=True)
