from __future__ import annotations

from itertools import product
from typing import TYPE_CHECKING, Self

from sirocco.core.graph_items import Cycle, Data, Store, Task
from sirocco.parsing._yaml_data_models import (
    CanonicalWorkflow,
    load_workflow_config,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    from pathlib import Path

    from sirocco.parsing._yaml_data_models import ConfigCycle


class Workflow:
    """Internal representation of a workflow"""

    def __init__(self, workflow_config: CanonicalWorkflow) -> None:
        self.name: str = workflow_config.name
        self.config_rootdir: Path = workflow_config.rootdir
        self.tasks: Store = Store()
        self.data: Store = Store()
        self.cycles: Store = Store()

        # Function to iterate over date and parameter combinations
        def iter_coordinates(param_refs: list, date: datetime | None = None) -> Iterator[dict]:
            space = ({} if date is None else {"date": [date]}) | {k: workflow_config.parameters[k] for k in param_refs}
            yield from (dict(zip(space.keys(), x)) for x in product(*space.values()))

        # 1 - create availalbe data nodes
        for data_config in workflow_config.data.available:
            for coordinates in iter_coordinates(param_refs=data_config.parameters, date=None):
                self.data.add(Data.from_config(config=data_config, coordinates=coordinates))

        # 2 - create output data nodes
        for cycle_config in workflow_config.cycles:
            for date in self.cycle_dates(cycle_config):
                for task_ref in cycle_config.tasks:
                    for data_ref in task_ref.outputs:
                        data_name = data_ref.name
                        data_config = workflow_config.data_dict[data_name]
                        for coordinates in iter_coordinates(param_refs=data_config.parameters, date=date):
                            self.data.add(Data.from_config(config=data_config, coordinates=coordinates))

        # 3 - create cycles and tasks
        for cycle_config in workflow_config.cycles:
            cycle_name = cycle_config.name
            for date in self.cycle_dates(cycle_config):
                cycle_tasks = []
                for task_graph_spec in cycle_config.tasks:
                    task_name = task_graph_spec.name
                    task_config = workflow_config.task_dict[task_name]

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
    def from_yaml(cls: type[Self], config_path: str) -> Self:
        return cls(load_workflow_config(config_path))
