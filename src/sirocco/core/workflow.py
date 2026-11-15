from __future__ import annotations

from itertools import chain, product
from typing import TYPE_CHECKING, Any, Self

from sirocco.core.graph_items import Cycle, Data, Store, Task
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
        config_cycles: list[ConfigCycle],
        config_tasks: list[ConfigTask],
        config_data: ConfigData,
        front_depth: int,
        parameters: dict[str, list],
    ) -> None:
        self.name: str = name
        self.config_rootdir: Path = config_rootdir
        # NOTE: front_depth is the only engine setting kept in core as it is shared by all engines
        self.front_depth = front_depth

        self.tasks: Store[Task] = Store()
        self.data: Store[Data] = Store()
        self.cycles: Store[Cycle] = Store()

        config_data_dict: dict[str, ConfigBaseData] = {
            data.name: data for data in chain(config_data.available, config_data.generated)
        }
        config_task_dict: dict[str, ConfigTask] = {task.name: task for task in config_tasks}

        # Function to iterate over date and parameter combinations
        def iter_coordinates(cycle_point: CyclePoint, param_refs: list[str]) -> Iterator[dict]:
            axes: dict[str, Any] = (
                {"date": [cycle_point.chunk_start_date]} if isinstance(cycle_point, DateCyclePoint) else {}
            )
            axes.update({k: parameters[k] for k in sorted(param_refs)})

            yield from (dict(zip(axes.keys(), x, strict=False)) for x in product(*axes.values()))

        # 1 - create available data nodes
        for available_data_config in config_data.available:
            for coordinates in iter_coordinates(OneOffPoint(), available_data_config.parameters):
                self.data.add(Data.from_config(config=available_data_config, coordinates=coordinates))

        # 2 - create output data nodes
        for cycle_config in config_cycles:
            for cycle_point in cycle_config.cycling.iter_cycle_points():
                for task_ref in cycle_config.tasks:
                    for data_ref in chain(*chain(*(comp.outputs.values() for comp in task_ref.components.values()))):
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
                            config_rootdir=self.config_rootdir,
                            cycle_point=cycle_point,
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

    @classmethod
    def from_config_path(
        cls: type[Self],
        config_path: str | Path,
        template_context: dict[str, Any] | None = None,
    ) -> Self:
        """Load workflow from a config file.

        Args:
            config_path: Path to the config YAML file
            template_context: dict of inline context variables or None

        Returns:
            Workflow instance
        """
        return cls.from_config_workflow(ConfigWorkflow.from_config_path(config_path, template_context=template_context))

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
        return cls(
            name=config_workflow.name,
            config_rootdir=config_workflow.rootdir,
            config_cycles=config_workflow.cycles,
            config_tasks=config_workflow.tasks,
            config_data=config_workflow.data,
            parameters=config_workflow.parameters,
            front_depth=config_workflow.front_depth,
        )
