from __future__ import annotations

import enum
import logging
from datetime import datetime
from itertools import chain, product
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from ruamel.yaml import YAML

from sirocco.core._tasks.sirocco_task import SiroccoContinueTask
from sirocco.core.graph_items import Cycle, Data, Store, Task, TaskStatus
from sirocco.core.scheduler import Scheduler
from sirocco.parsing.cycling import DateCyclePoint, OneOffPoint
from sirocco.parsing.yaml_data_models import (
    ConfigBaseData,
    ConfigRootTask,
    ConfigSiroccoTask,
    ConfigWorkflow,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sirocco.parsing.cycling import CyclePoint
    from sirocco.parsing.yaml_data_models import (
        ConfigCycle,
        ConfigData,
        ConfigTask,
    )


class WorkflowStatus(enum.Enum):
    INIT = 0
    CONTINUE = 1
    COMPLETED = 2
    FAILED = 3
    STOPPED = 4
    STOPPING = 5
    RESTART_FAILED = 6


class Workflow:
    """Internal representation of a workflow"""

    RUN_ROOT: str = Task.RUN_ROOT
    STATE_FILE_NAME: str = "state.yaml"

    def __init__(
        self,
        name: str,
        config_rootdir: Path,
        config_filename: str,
        scheduler: Scheduler,
        config_cycles: list[ConfigCycle],
        config_tasks: list[ConfigTask],
        config_data: ConfigData,
        front_depth: int,
        parameters: dict[str, list],
        config_sirocco_task: ConfigSiroccoTask | None = None,
        resolved_config_path: str | None = None,
    ) -> None:
        self.name: str = name
        self._config_rootdir: Path = config_rootdir
        self.config_filename = config_filename
        self.resolved_config_path = resolved_config_path
        self.scheduler = scheduler
        self.front_depth = front_depth
        self.front: list[list[Task]] = [[] for _ in range(self.front_depth)]
        self.completed_tasks: list[Task] = []
        self.cool_down_tasks: list[Task] = []

        self.tasks: Store[Task] = Store()
        self.data: Store[Data] = Store()
        self.cycles: Store[Cycle] = Store()
        self.status: WorkflowStatus = WorkflowStatus.INIT

        config_data_dict: dict[str, ConfigBaseData] = {
            data.name: data for data in chain(config_data.available, config_data.generated)
        }
        config_task_dict: dict[str, ConfigTask] = {task.name: task for task in config_tasks}

        # Function to iterate over date and parameter combinations
        def iter_coordinates(cycle_point: CyclePoint, param_refs: list[str]) -> Iterator[dict]:
            axes = {k: parameters[k] for k in param_refs}
            if isinstance(cycle_point, DateCyclePoint):
                axes["date"] = [cycle_point.chunk_start_date]
            yield from (dict(zip(axes.keys(), x, strict=False)) for x in product(*axes.values()))

        # 1 - create available data nodes
        for available_data_config in config_data.available:
            for coordinates in iter_coordinates(OneOffPoint(), available_data_config.parameters):
                self.data.add(Data.from_config(config=available_data_config, coordinates=coordinates))

        # 2 - create output data nodes
        for cycle_config in config_cycles:
            for cycle_point in cycle_config.cycling.iter_cycle_points():
                for task_ref in cycle_config.tasks:
                    for data_ref in task_ref.outputs:
                        data_config = config_data_dict[data_ref.name]
                        for coordinates in iter_coordinates(cycle_point, data_config.parameters):
                            self.data.add(Data.from_config(config=data_config, coordinates=coordinates))

        # 3 - create cycles and tasks
        for cycle_config in config_cycles:
            cycle_name = cycle_config.name
            for cycle_point in cycle_config.cycling.iter_cycle_points():
                cycle_tasks = []
                for task_graph_spec in cycle_config.tasks:
                    task_name = task_graph_spec.name
                    task_config = config_task_dict[task_name]
                    for coordinates in iter_coordinates(cycle_point, task_config.parameters):
                        if isinstance(task_config, ConfigRootTask | ConfigSiroccoTask):
                            msg = "ROOT and SIROCCO tasks are special tasks that cannot be included in the graph"
                            raise TypeError(msg)
                        task = Task.from_config(
                            config=task_config,
                            config_rootdir=self._config_rootdir,
                            cycle_point=cycle_point,
                            coordinates=coordinates,
                            datastore=self.data,
                            graph_spec=task_graph_spec,
                        )
                        task.rank = self.front_depth
                        self.tasks.add(task)
                        cycle_tasks.append(task)
                self.cycles.add(
                    Cycle(
                        name=cycle_name,
                        tasks=cycle_tasks,
                        coordinates={"date": cycle_point.chunk_start_date}
                        if isinstance(cycle_point, DateCyclePoint)
                        else {},
                    )
                )

        # 4 - Link wait on tasks
        for task in self.tasks:
            task.link_wait_on_tasks(self.tasks)

        # 5 - Link parent and children tasks
        for task in self.tasks:
            task.link_parents_children()

        # 6 - Create Sirocco continuation task
        config_kwargs = dict(config_sirocco_task) if config_sirocco_task else {}
        if config_kwargs:
            del config_kwargs["parameters"]
        self.sirocco_continue_task = SiroccoContinueTask(
            coordinates={},
            cycle_point=OneOffPoint(),
            config_rootdir=self.config_rootdir,
            config_filename=self.config_filename,
            parents=[],
            **config_kwargs,
        )

    @property
    def config_rootdir(self) -> Path:
        return self._config_rootdir

    @property
    def rundir(self) -> Path:
        return self.config_rootdir / self.RUN_ROOT

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
        if self.sirocco_continue_task.jobid != "_NO_ID_":
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

    def stop(self, mode: Literal["cancel", "cool-down"], log_type: Literal["std", "tee"] = "tee") -> None:
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
        if self.sirocco_continue_task.jobid == "_NO_ID_":
            msg = "Workflow not running, no need to stop"
            logger.error(msg)
            self.unlock()
            return
        self.scheduler.cancel(self.sirocco_continue_task)
        self.cancel_all_tasks(mode=mode, logger=logger)
        self.status = WorkflowStatus.STOPPED
        self.sirocco_continue_task.jobid = "_NO_ID_"
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

    def lock(self) -> None:
        self.lock_file.touch()

    def unlock(self) -> None:
        self.lock_file.unlink()

    def dump_state(self) -> None:
        yaml_front: list[list[dict[str, dict[str, Any]]]] = [[] for _ in range(self.front_depth)]
        yaml_completed_tasks = [task.to_yaml_state() for task in self.completed_tasks]
        yaml_cool_down_tasks = [task.to_yaml_state() for task in self.cool_down_tasks]
        yaml_front = [[task.to_yaml_state() for task in generation] for generation in self.front]
        YAML().dump(
            {
                "sirocco_jobid": self.sirocco_continue_task.jobid,
                "completed_tasks": yaml_completed_tasks,
                "cool_down_tasks": yaml_cool_down_tasks,
                "front": yaml_front,
            },
            self.state_file,
        )

    def load_state(self) -> None:
        yaml_state = YAML().load(self.state_file)
        self.sirocco_continue_task.jobid = yaml_state["sirocco_jobid"]
        self.completed_tasks = [self.task_from_yaml_state(yaml_task, -1) for yaml_task in yaml_state["completed_tasks"]]
        self.cool_down_tasks = [self.task_from_yaml_state(yaml_task, 0) for yaml_task in yaml_state["cool_down_tasks"]]
        for k, yaml_generation in enumerate(yaml_state["front"]):
            self.front[k] = [self.task_from_yaml_state(yaml_task, k) for yaml_task in yaml_generation]

    def task_from_yaml_state(self, yaml_task: dict[str, dict[str, Any]], rank: int) -> Task:
        name, yaml_specs = next(iter(yaml_task.items()))
        coordinates = {k: datetime.fromisoformat(v) if k == "date" else v for k, v in yaml_specs["coordinates"].items()}
        task = self.tasks[name, coordinates]
        task.jobid = yaml_specs["jobid"]
        task.rank = rank
        return task

    def init_front(self, logger: logging.Logger) -> None:
        """Populate front and submit corresponding tasks"""

        for task in self.tasks:
            if not task.parents:
                task.rank = 0
                self.scheduler.submit(task)
                self.front[0].append(task)
                msg = f"{task.label} ({task.jobid}) SUBMITTED to rank {task.rank}"
                logger.info(msg)
        for k in range(self.front_depth - 1):
            for task in self.front[k]:
                for child in task.children:
                    if max(parent.rank for parent in child.parents) == k:
                        self.scheduler.submit(child)
                        child.rank = k + 1
                        self.front[k + 1].append(child)
                        msg = f"{child.label} ({child.jobid}) SUBMITTED to rank {child.rank}"
                        logger.info(msg)

    def restart_front(self, logger: logging.Logger) -> None:
        """Submit all tasks currently in the front"""

        for generation in self.front:
            for task in generation:
                # Do not resubmit tasks in cool down mode
                if (
                    task.rank == 0
                    and task in self.cool_down_tasks
                    and self.scheduler.get_status(task)
                    in (TaskStatus.COMPLETED, TaskStatus.RUNNING, TaskStatus.WAITING)
                ):
                    self.cool_down_tasks.remove(task)
                    msg = f"{task.label} ({task.jobid}) CONTINUED as rank 0 from cool-down"
                    logger.info(msg)
                else:
                    self.scheduler.cancel(task)
                    self.scheduler.submit(task)
                    msg = f"{task.label} ({task.jobid}) SUBMITTED to rank {task.rank}"
                    logger.info(msg)

    def propagate_front(self, logger: logging.Logger) -> None:
        """Propagate front of submitted tasks and submit new ones"""

        # Handle first generation of the front
        to_promote: list[Task] = []
        for task in self.front[0]:
            if (status := self.scheduler.get_status(task)) == TaskStatus.FAILED:
                msg = f"{task.label} ({task.jobid}) FAILED"
                logger.info(msg)
                self.cancel_all_tasks(mode="cancel", logger=logger)
                msg = f"All workflow tasks canceled because {task.label} failed"
                logger.info(msg)
                self.status = WorkflowStatus.FAILED
                return
            if status == TaskStatus.COMPLETED:
                to_promote.append(task)
        for task in to_promote:
            task.rank = -1
            self.front[0].remove(task)
            self.completed_tasks.append(task)
            msg = f"{task.label} ({task.jobid}) COMPLETED"
            logger.info(msg)

        # Update front rank of tasks currently in the front after the first generation
        for k in range(1, self.front_depth):
            to_promote = [task for task in self.front[k] if max(parent.rank for parent in task.parents) == k - 2]
            for task in to_promote:
                task.rank = k - 1
                self.front[k].remove(task)
                self.front[k - 1].append(task)
                msg = f"{task.label} ({task.jobid}) PROMOTED from rank {k} to {k - 1}"
                logger.info(msg)

        # Add new tasks to the last generation of the front
        before_last_generation = to_promote if self.front_depth == 1 else self.front[-2]
        for task in before_last_generation:
            for child in task.children:
                if (
                    child.rank == self.front_depth
                    and max(parent.rank for parent in child.parents) == self.front_depth - 2
                ):
                    self.scheduler.submit(child)
                    child.rank = self.front_depth - 1
                    self.front[-1].append(child)
                    msg = f"{child.label} ({child.jobid}) SUBMITTED to rank {child.rank}"
                    logger.info(msg)

    def cancel_all_tasks(self, mode: Literal["cancel", "cool-down"], logger: logging.Logger) -> None:
        """Cancel all workflow tasks except cool-down tasks"""

        for generation in self.front:
            for task in generation:
                if (
                    task.rank == 0
                    and mode == "cool-down"
                    and self.scheduler.get_status(task) in (TaskStatus.COMPLETED, TaskStatus.RUNNING)
                ):
                    self.cool_down_tasks.append(task)
                    msg = f"{task.label} ({task.jobid}) COOLING DOWN"
                    logger.info(msg)
                else:
                    self.scheduler.cancel(task)
                    msg = f"{task.label} ({task.jobid}) CANCELED"
                    logger.info(msg)

    def auto_submit(self) -> None:
        """Submit next Sirocco task"""

        if self.status == WorkflowStatus.FAILED:
            self.sirocco_continue_task.jobid = "_NO_ID_"
        elif self.front[0]:
            self.sirocco_continue_task.parents = self.front[0]
            self.scheduler.submit(self.sirocco_continue_task, output_mode="append", dependency_type="ANY")
            self.status = WorkflowStatus.CONTINUE
        else:
            self.sirocco_continue_task.jobid = "_NO_ID_"
            self.status = WorkflowStatus.COMPLETED

    @overload
    @classmethod
    def from_config_file(
        cls: type[Self],
        config_path: str | Path,
        template_context: None = None,
    ) -> Self: ...

    @overload
    @classmethod
    def from_config_file(
        cls: type[Self],
        config_path: str | Path,
        template_context: dict[str, Any],
    ) -> Self: ...

    @overload
    @classmethod
    def from_config_file(
        cls: type[Self],
        config_path: str | Path,
        template_context: str | Path,
    ) -> Self: ...

    @classmethod
    def from_config_file(
        cls: type[Self],
        config_path: str | Path,
        template_context: dict[str, Any] | str | Path | None = None,
    ) -> Self:
        """Load workflow from a config file.

        Args:
            config_path: Path to the config YAML file
            template_context: Either a dict of inline context variables, path to a variables file, or None

        Returns:
            Workflow instance
        """
        if isinstance(template_context, dict):
            # Inline context dict (for tests/programmatic use)
            return cls.from_config_workflow(
                ConfigWorkflow.from_config_file(config_path, template_context=template_context)
            )
        if isinstance(template_context, (str, Path)):
            # Path to variables file (file-based approach)
            return cls.from_config_workflow(
                ConfigWorkflow.from_config_file(config_path, jinja_vars_file_path=template_context)
            )
        # No context
        return cls.from_config_workflow(ConfigWorkflow.from_config_file(config_path))

    @classmethod
    def from_config_str(
        cls: type[Self],
        content: str,
        template_context: dict[str, Any] | None = None,
        name: str | None = None,
        rootdir: Path | None = None,
    ) -> Self:
        """Load workflow from a YAML string (for testing/programmatic use).

        Args:
            content: YAML string containing the workflow definition
            template_context: Optional dict of context variables for Jinja2 template rendering
            name: Optional workflow name (defaults to "workflow")
            rootdir: Optional root directory for relative paths (defaults to cwd)

        Returns:
            Workflow instance
        """
        return cls.from_config_workflow(
            ConfigWorkflow.from_config_str(content, template_context=template_context, name=name, rootdir=rootdir)
        )

    @classmethod
    def from_config_workflow(
        cls: type[Self],
        config_workflow: ConfigWorkflow,
    ) -> Self:
        sirocco_task_config: ConfigSiroccoTask | None = None
        for task in config_workflow.tasks:
            if isinstance(task, ConfigSiroccoTask):
                sirocco_task_config = task
        return cls(
            name=config_workflow.name,
            config_rootdir=config_workflow.rootdir,
            config_filename=config_workflow.config_filename,
            scheduler=Scheduler.create(config_workflow.scheduler),
            config_cycles=config_workflow.cycles,
            config_tasks=config_workflow.tasks,
            config_data=config_workflow.data,
            parameters=config_workflow.parameters,
            front_depth=config_workflow.front_depth,
            config_sirocco_task=sirocco_task_config,
            resolved_config_path=config_workflow.resolved_config_path,
        )
