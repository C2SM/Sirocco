from __future__ import annotations

import logging
from itertools import chain, product
from typing import TYPE_CHECKING, Literal, Self

from sirocco.core._tasks.sirocco_task import SiroccoContinueTask
from sirocco.core.graph_items import Cycle, Data, Status, Store, Task
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
    from pathlib import Path

    from sirocco.parsing.cycling import CyclePoint
    from sirocco.parsing.yaml_data_models import (
        ConfigCycle,
        ConfigData,
        ConfigTask,
    )


class Workflow:
    """Internal representation of a workflow"""

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
    ) -> None:
        self.name: str = name
        self._config_rootdir: Path = config_rootdir
        self.config_filename = config_filename
        self.scheduler = scheduler
        self.front_depth = front_depth
        self.front: list[list[Task]] = [[] for _ in range(self.front_depth)]

        self.tasks: Store[Task] = Store()
        self.data: Store[Data] = Store()
        self.cycles: Store[Cycle] = Store()

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

        # 1 - create availalbe data nodes
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
            parents=self.front[0],
            **config_kwargs,
        )

    # =========== Methods to control workflow ===========
    #
    def start(self, log_type: Literal["std", "tee"] = "tee") -> None:
        if (self.config_rootdir / "run").exists():
            msg = "Workflow already exists, cannot start"
            raise ValueError(msg)
        self.init_front(log_type=log_type)
        self.auto_submit()

    def continue_wf(self, log_type: Literal["std", "tee"] = "std") -> None:  # NOTE: cannot use "continue"
        self.load_front()
        if self.propagate_front(log_type=log_type):
            self.auto_submit()

    def restart(self, log_type: Literal["std", "tee"] = "tee") -> None:
        if not (self.config_rootdir / "run").exists():
            msg = "Workflow did not start, cannot stop"
            raise ValueError(msg)
        self.load_front()
        self.restart_front(log_type=log_type)
        if self.propagate_front(log_type=log_type):
            self.auto_submit()

    def stop(self, mode: Literal["cancel", "cool-down"], log_type: Literal["std", "tee"] = "tee") -> None:
        if not (self.config_rootdir / "run").exists():
            msg = "Workflow did not start, cannot stop"
            raise ValueError(msg)
        if self.sirocco_continue_task.load_jobid_and_rank():
            self.scheduler.cancel(self.sirocco_continue_task)
        self.load_front()
        self.cancel_all_tasks(mode=mode, log_type=log_type)

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

    def load_front(self) -> None:
        """Update taks ids and populate front from serialized information"""
        for task in self.tasks:
            if task.load_jobid_and_rank() and task.rank >= 0:
                self.front[task.rank].append(task)

    def init_front(self, log_type: Literal["std", "tee"] = "std") -> None:
        """Populate front and submit corresponding tasks"""

        logger = self.get_logger(log_type)
        for task in self.tasks:
            if not task.parents:
                task.rank = 0
                self.scheduler.submit(task)
                self.front[0].append(task)
                task.dump_jobid_and_rank()
                msg = f"{task.label} (jobid:{task.jobid}) SUBMITTED"
                logger.info(msg)
        for k in range(self.front_depth - 1):
            for task in self.front[k]:
                for child in task.children:
                    if max(parent.rank for parent in child.parents) == k:
                        self.scheduler.submit(child)
                        child.rank = k + 1
                        self.front[k + 1].append(child)
                        child.dump_jobid_and_rank()
                        msg = f"{child.label} (jobid:{child.jobid}) SUBMITTED"
                        logger.info(msg)

    def restart_front(self, log_type: Literal["std", "tee"] = "std") -> None:
        """Submit all tasks currently in the front"""

        logger = self.get_logger(log_type)
        for generation in self.front:
            for task in generation:
                # Do not resubmit tasks in cool down mode
                if (
                    task.rank == 0
                    and task.cool_down_path.exists()
                    and self.scheduler.get_status(task) in (Status.COMPLETED, Status.ONGOING)
                ):
                    task.cool_down_path.unlink()
                else:
                    self.scheduler.submit(task)
                    task.dump_jobid_and_rank()
                    msg = f"{task.label} (jobid:{task.jobid}) SUBMITTED"
                    logger.info(msg)

    def propagate_front(self, log_type: Literal["std", "tee"] = "std") -> bool:
        """Propagate front of submitted tasks and submit new ones

        return False if a task failed, otherwise True"""

        logger = self.get_logger(log_type)
        # Handle first generation of the front
        just_finished: list[Task] = []  # only needed for a front depth of 1
        for task in self.front[0]:
            if (status := self.scheduler.get_status(task)) == Status.FAILED:
                self.cancel_all_tasks(mode="cancel")
                msg = f"All workflow tasks canceled because {task.label} failed"
                logger.info(msg)
                return False
            if status == Status.COMPLETED:
                if self.front_depth == 1:
                    just_finished.append(task)
                task.rank = -1
                self.front[0].remove(task)
                task.dump_jobid_and_rank()
                msg = f"{task.label} (jobid:{task.jobid}) COMPLETED"
                logger.info(msg)
        return True

        # Update front rank of tasks currently in the front after the first generation
        for k in range(1, self.front_depth):
            for task in self.front[k]:
                if max(parent.rank for parent in task.parents) == k - 2:
                    task.rank = k - 1
                    self.front[k].remove(task)
                    self.front[k - 1].append(task)
                    task.dump_jobid_and_rank()
                    msg = f"{task.label} (jobid:{task.jobid}) PROMOTED from rank {k} to {k-1}"
                    logger.info(msg)

        # Add new tasks to the last generation of the front
        before_last_generation = just_finished if self.front_depth == 1 else self.front[-2]
        for task in before_last_generation:
            for child in task.children:
                if (
                    child.rank == self.front_depth
                    and max(parent.rank for parent in child.parents) == self.front_depth - 2
                ):
                    self.scheduler.submit(child)
                    child.rank = self.front_depth - 1
                    self.front[-1].append(child)
                    child.dump_jobid_and_rank()
                    msg = f"{child.label} (jobid:{child.jobid}) SUBMITTED"
                    logger.info(msg)

    def cancel_all_tasks(self, mode: Literal["cancel", "cool-down"], log_type: Literal["std", "tee"] = "std") -> None:
        """Cancel all workflow tasks except cool-down tasks"""

        logger = self.get_logger(log_type)
        for generation in self.front:
            for task in generation:
                if (
                    task.rank == 0
                    and mode == "cool-down"
                    and self.scheduler.get_status(task) in (Status.COMPLETED, Status.ONGOING)
                ):
                    task.cool_down_path.touch()
                    msg = f"{task.label} (jobid:{task.jobid}) COOLING DOWN"
                    logger.info(msg)
                    continue
                self.scheduler.cancel(task)
                msg = f"{task.label} (jobid:{task.jobid}) CANCELED"
                logger.info(msg)

    def auto_submit(self) -> None:
        """Submit next Sirocco task"""
        if self.front[0]:
            self.scheduler.submit(self.sirocco_continue_task, output_mode="append", dependency_type="ANY")

    @property
    def config_rootdir(self) -> Path:
        return self._config_rootdir

    @classmethod
    def from_config_file(
        cls: type[Self],
        config_path: str | Path,
    ) -> Self:
        """
        Loads a python representation of a workflow config file.

        :param config_path: the string to the config yaml file containing the workflow definition
        """
        return cls.from_config_workflow(ConfigWorkflow.from_config_file(config_path))

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
        )
