"""Task pair creation - Launcher and monitor task pair management."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from aiida_workgraph import WorkGraph

    from sirocco.engines.aiida.models import (
        AiidaIconTaskSpec,
        AiidaShellTaskSpec,
        DependencyMapping,
    )
    from sirocco.engines.aiida.types import (
        PortDataMapping,
        TaskDependencyMapping,
        TaskMonitorOutputsMapping,
        WgTaskProtocol,
    )

LOGGER = logging.getLogger(__name__)

from sirocco.engines.aiida.monitoring import get_job_data
from sirocco.engines.aiida.spec_resolvers import (
    build_icon_task_with_dependencies,
    build_shell_task_with_dependencies,
)

# Task name prefixes for WorkGraph tasks
LAUNCHER_PREFIX = "launch_"
MONITOR_PREFIX = "get_job_data_"


class TaskPairContext:
    """Context for adding task pairs to a WorkGraph.

    This class encapsulates the state needed when adding launcher/monitor task
    pairs to a WorkGraph.

    The context maintains:
    - The WorkGraph being built
    - The workflow name (for task naming)
    - Dependency tracking state (outputs and tasks)

    Usage:
        ctx = TaskPairContext(workgraph, workflow_name)
        ctx.add_icon_task_pair(task_label, task_spec, input_data, dependencies)
        # Access results
        outputs = ctx.get_dependency_outputs()
    """

    def __init__(self, workgraph: WorkGraph, workflow_name: str):
        """Initialize the task pair context.

        Args:
            workgraph: The WorkGraph to add tasks to
            workflow_name: The workflow name (with timestamp) for task naming
        """
        self._wg = workgraph
        self._wg_name = workflow_name
        self._dependency_outputs: TaskMonitorOutputsMapping = {}
        self._dependency_tasks: TaskDependencyMapping = {}

    def get_dependency_outputs(self) -> TaskMonitorOutputsMapping:
        """Get the accumulated dependency outputs.

        Returns:
            Dict mapping task labels to their output namespaces
        """
        return self._dependency_outputs

    def get_dependency_tasks(self) -> TaskDependencyMapping:
        """Get the accumulated dependency tasks.

        Returns:
            Dict mapping task labels to their dependency monitor tasks
        """
        return self._dependency_tasks

    def add_icon_task_pair(
        self,
        task_label: str,
        task_spec: AiidaIconTaskSpec,
        input_data_for_task: PortDataMapping,
        dependencies: DependencyMapping,
    ) -> None:
        """Add ICON launcher and monitor tasks to the WorkGraph.

        Args:
            task_label: Label for the task
            task_spec: ICON task specification
            input_data_for_task: Dict of AvailableData inputs
            dependencies: Dependency mapping with parent folders, job IDs, and ports
        """
        self._add_task_pair(
            task_label=task_label,
            task_spec=task_spec,
            build_task_func=build_icon_task_with_dependencies,
            input_data_for_task=input_data_for_task,
            dependencies=dependencies,
        )

    def add_shell_task_pair(
        self,
        task_label: str,
        task_spec: AiidaShellTaskSpec,
        input_data_for_task: PortDataMapping,
        dependencies: DependencyMapping,
    ) -> None:
        """Add Shell launcher and monitor tasks to the WorkGraph.

        Args:
            task_label: Label for the task
            task_spec: Shell task specification
            input_data_for_task: Dict of AvailableData inputs
            dependencies: Dependency mapping with parent folders, job IDs, and ports
        """
        self._add_task_pair(
            task_label=task_label,
            task_spec=task_spec,
            build_task_func=build_shell_task_with_dependencies,
            input_data_for_task=input_data_for_task,
            dependencies=dependencies,
        )

    def _add_task_pair(
        self,
        task_label: str,
        task_spec: AiidaIconTaskSpec | AiidaShellTaskSpec,
        build_task_func: Callable,
        input_data_for_task: PortDataMapping,
        dependencies: DependencyMapping,
    ) -> None:
        """Common logic for adding launcher + monitor task pairs.

        This is the core implementation that both ICON and Shell task pairs use.

        Args:
            task_label: Label for the task
            task_spec: Task specification (ICON or Shell)
            build_task_func: The task builder function (build_icon_task_with_dependencies
                or build_shell_task_with_dependencies)
            input_data_for_task: Dict of AvailableData inputs
            dependencies: Dependency mapping with parent folders, job IDs, and port dependencies
        """
        launcher_name = f"{LAUNCHER_PREFIX}{self._wg_name}_{task_label}"

        # Add port_dependency_mapping to task_spec
        port_dep_dict = {port: [dep.model_dump() for dep in deps] for port, deps in dependencies.port_mapping.items()}
        task_spec_with_deps = task_spec.model_copy(update={"port_dependency_mapping": port_dep_dict})

        # Create launcher task - serialize model to dict for WorkGraph
        self._wg.add_task(
            build_task_func,
            name=launcher_name,
            task_spec=task_spec_with_deps.model_dump(mode="python"),
            input_data_nodes=input_data_for_task if input_data_for_task else None,
            task_folders=dependencies.task_folders if dependencies.task_folders else None,
            task_job_ids=dependencies.task_job_ids if dependencies.task_job_ids else None,
        )

        # Calculate timeout based on walltime + buffer for queuing
        # Default to 1 hour if walltime not specified
        walltime_seconds = 3600
        if task_spec.metadata and task_spec.metadata.options and task_spec.metadata.options.max_wallclock_seconds:
            walltime_seconds = task_spec.metadata.options.max_wallclock_seconds
        # Add 5 minute buffer for job submission and queuing
        timeout = walltime_seconds + 300

        # Create get_job_data task
        dep_task: WgTaskProtocol = self._wg.add_task(
            get_job_data,
            name=f"{MONITOR_PREFIX}{task_label}",
            workgraph_name=launcher_name,
            task_name=task_label,
            timeout_seconds=timeout,
        )

        # Store the outputs namespace for dependent tasks
        self._dependency_outputs[task_label] = dep_task.outputs

        # Chain with previous dependency tasks
        for dep_label in dependencies.task_folders:
            if dep_label in self._dependency_tasks:
                _ = self._dependency_tasks[dep_label] >> dep_task

        # Store for next iteration
        self._dependency_tasks[task_label] = dep_task
