from __future__ import annotations

from itertools import chain, product
from typing import TYPE_CHECKING, Self

from sirocco.core.graph_items import Cycle, Data, Store, Task
from sirocco.parsing._yaml_data_models import (
    ConfigWorkflow,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    from pathlib import Path

    from sirocco.parsing._yaml_data_models import (
        ConfigAvailableData,
        ConfigCycle,
        ConfigData,
        ConfigGeneratedData,
        ConfigTask,
    )


class Workflow:
    """Internal representation of a workflow"""

    def __init__(
        self,
        name: str,
        config_rootdir: Path,
        cycles: list[ConfigCycle],
        tasks: list[ConfigTask],
        data: ConfigData,
        parameters: dict[str, list],
    ) -> None:
        self.name: str = name
        self.config_rootdir: Path = config_rootdir

        self.tasks: Store = Store()
        self.data: Store = Store()
        self.cycles: Store = Store()

        data_dict: dict[str, ConfigAvailableData | ConfigGeneratedData] = {
            data.name: data for data in chain(data.available, data.generated)
        }
        task_dict: dict[str, ConfigTask] = {task.name: task for task in tasks}

        # Function to iterate over date and parameter combinations
        def iter_coordinates(param_refs: list, date: datetime | None = None) -> Iterator[dict]:
            space = ({} if date is None else {"date": [date]}) | {k: parameters[k] for k in param_refs}
            yield from (dict(zip(space.keys(), x, strict=False)) for x in product(*space.values()))

        # 1 - create availalbe data nodes
        for data_config in data.available:
            for coordinates in iter_coordinates(param_refs=data_config.parameters, date=None):
                self.data.add(Data.from_config(config=data_config, coordinates=coordinates))

        # 2 - create output data nodes
        for cycle_config in cycles:
            for date in self.cycle_dates(cycle_config):
                for task_ref in cycle_config.tasks:
                    for data_ref in task_ref.outputs:
                        data_name = data_ref.name
                        data_config = data_dict[data_name]
                        for coordinates in iter_coordinates(param_refs=data_config.parameters, date=date):
                            self.data.add(Data.from_config(config=data_config, coordinates=coordinates))

        # 3 - create cycles and tasks
        for cycle_config in cycles:
            cycle_name = cycle_config.name
            for date in self.cycle_dates(cycle_config):
                cycle_tasks = []
                for task_graph_spec in cycle_config.tasks:
                    task_name = task_graph_spec.name
                    task_config = task_dict[task_name]

                    for coordinates in iter_coordinates(param_refs=task_config.parameters, date=date):
                        task = Task.from_config(
                            config=task_config,
                            config_rootdir=self.config_rootdir,
                            start_date=cycle_config.start_date,
                            end_date=cycle_config.end_date,
                            coordinates=coordinates,
                            datastore=self.data,
                            graph_spec=task_graph_spec,
                        )
                        self.tasks.add(task)
                        cycle_tasks.append(task)
                self.cycles.add(
                    Cycle(
                        name=cycle_name,
                        tasks=cycle_tasks,
                        coordinates={} if date is None else {"date": date},
                    )
                )

        # 4 - Link wait on tasks
        for task in self.tasks:
            task.link_wait_on_tasks(self.tasks)

    @staticmethod
    def cycle_dates(cycle_config: ConfigCycle) -> Iterator[datetime]:
        yield (date := cycle_config.start_date)
        if cycle_config.period is not None:
            while (date := date + cycle_config.period) < cycle_config.end_date:
                yield date

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
            cycles=config_workflow.cycles,
            tasks=config_workflow.tasks,
            data=config_workflow.data,
            parameters=config_workflow.parameters,
        )
