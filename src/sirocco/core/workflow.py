from __future__ import annotations

import enum
import pickle
from itertools import chain, product
from pathlib import Path
from typing import TYPE_CHECKING, Self

from sirocco.core._tasks.sirocco_task import SiroccoTask
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

    from sirocco.parsing.cycling import CyclePoint
    from sirocco.parsing.yaml_data_models import (
        ConfigCycle,
        ConfigData,
        ConfigTask,
    )


class Mode(enum.Enum):
    INIT = 1
    PROPAGATE = 2
    RESTART = 3


class Workflow:
    """Internal representation of a workflow"""

    _STATE_FILENAME_ = ".sirocco.state"
    _CONFIG_FILENAME_ = "sirocco.yaml"

    def __init__(
        self,
        name: str,
        config_rootdir: Path,
        scheduler: Scheduler,
        config_cycles: list[ConfigCycle],
        config_tasks: list[ConfigTask],
        config_data: ConfigData,
        front_depth: int,
        parameters: dict[str, list],
    ) -> None:
        self.name: str = name
        self._config_rootdir: Path = config_rootdir
        self.scheduler = scheduler
        self.mode = Mode.INIT
        self.status_file = config_rootdir / self._STATE_FILENAME_
        self.front_depth = front_depth
        self.front: list[list[Task]] = [[]] * self.front_depth

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
                        task.front_rank = self.front_depth
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

    def step_forward(self) -> None:
        match self.mode:
            case Mode.INIT:
                self.init_front()
            case Mode.RESTART:
                self.restart_front()
            case Mode.PROPAGATE:
                self.propagate_front()

        self.mode = Mode.PROPAGATE
        self.auto_submit()
        self.dump()

    def restart_front(self) -> None:
        for generation in self.front:
            for task in generation:
                self.scheduler.submit_task(task)

    def init_front(self) -> None:
        """Populate front and submit corresponding tasks"""

        for task in self.tasks:
            if not task.parents:
                task.front_rank = 0
                self.scheduler.submit_task(task)
                self.front[0].append(task)
        for k in range(self.front_depth - 1):
            for task in self.front[k]:
                for child in task.children:
                    if max(parent.front_rank for parent in child.parents) == k:
                        self.scheduler.submit_task(child)
                        child.front_rank = k + 1
                        self.front[k + 1].append(child)

    def propagate_front(self) -> None:
        """Propagate front of submitted tasks and submit new ones"""

        # Handle first generation of the front
        just_finished: list[Task] = []  # only needed for a front depth of 1
        for task in self.front[0]:
            if self.scheduler.update_status(task) == Status.FAILED:
                self.cancel_all_tasks()
                msg = "All workflow tasks canceled because task failed"
                raise ValueError(msg)
            if task.status == Status.COMPLETED:
                if self.front_depth == 1:
                    just_finished.append(task)
                task.front_rank = -1
                self.front[0].remove(task)

        # Update front rank of tasks currently in the front after the first generation
        for k in range(1, self.front_depth):
            for task in self.front[k]:
                if max(parent.front_rank for parent in task.parents) == k - 2:
                    task.front_rank = k - 1
                    self.front[k].remove(task)
                    self.front[k - 1].append(task)

        # Add new tasks to the last generation of the front
        before_last_generation = just_finished if self.front_depth == 1 else self.front[-2]
        for task in before_last_generation:
            for child in task.children:
                if (
                    child.front_rank == self.front_depth
                    and max(parent.front_rank for parent in child.parents) == self.front_depth - 2
                ):
                    self.scheduler.submit_task(child)
                    child.front_rank = self.front_depth - 1
                    self.front[-1].append(child)

    def auto_submit(self) -> None:
        """Submit next Sirocco task"""

        if not self.front[0]:
            return
        self.sirocco_task = SiroccoTask(
            coordinates={}, cycle_point=OneOffPoint(), config_rootdir=self.config_rootdir, parents=self.front[0]
        )
        self.scheduler.submit_task(self.sirocco_task, dependency_type="ANY")

    def cancel_all_tasks(self) -> None:
        for generation in self.front:
            for task in generation:
                self.scheduler.cancel_task(task)

    def dump(self) -> None:
        with self.status_file.open("wb") as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    @property
    def config_rootdir(self) -> Path:
        return self._config_rootdir

    @classmethod
    def from_config_file(cls: type[Self], config_path: str) -> Self:
        """
        Loads a python representation of a workflow config file.

        :param config_path: the string to the config yaml file containing the workflow definition
        """
        return cls.from_config_workflow(ConfigWorkflow.from_config_file(config_path))

    @classmethod
    def from_config_workflow(cls: type[Self], config_workflow: ConfigWorkflow) -> Self:
        return cls(
            name=config_workflow.name,
            config_rootdir=config_workflow.rootdir,
            scheduler=Scheduler.create(config_workflow.scheduler),
            config_cycles=config_workflow.cycles,
            config_tasks=config_workflow.tasks,
            config_data=config_workflow.data,
            parameters=config_workflow.parameters,
            front_depth=config_workflow.front_depth,
        )

    @classmethod
    def load(cls, root_dir: Path | str) -> Self:
        """Load workflow from state file"""

        state_file = Path(root_dir) / cls._STATE_FILENAME_
        if not state_file.is_file():
            msg = f"{root_dir} does not seem to be a workflow directory, {cls._STATE_FILENAME_} not found there."
            raise ValueError(msg)
        with state_file.open("rb") as f:
            return pickle.load(f)  # noqa S301
