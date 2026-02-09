"""WorkGraph builder - orchestrates WorkGraph construction from core workflows."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import aiida.orm
from aiida.common import validate_link_label
from aiida_workgraph import WorkGraph
from aiida_workgraph.manager import set_current_graph

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.dependencies import (
    build_dependency_mapping,
    collect_available_data_inputs,
)
from sirocco.engines.aiida.tasks import (
    LAUNCHER_PREFIX,
    add_icon_task_pair,
    add_shell_task_pair,
    build_icon_task_spec,
    build_shell_task_spec,
)

if TYPE_CHECKING:
    from sirocco.engines.aiida.models import (
        AiidaIconTaskSpec,
        AiidaShellTaskSpec,
    )
    from sirocco.engines.aiida.types import (
        FileNode,
        TaskDependencyMapping,
        TaskMonitorOutputsMapping,
    )

LOGGER = logging.getLogger(__name__)

DEFAULT_FRONT_DEPTH = 1
"""Default front depth for workflow execution.

A value of 1 means no pre-submission: wait for topological level N to finish
before submitting level N+1. This is the safest/most conservative strategy.

Increase to 2+ for more aggressive pre-submission and better parallelism,
at the cost of potentially wasted submissions if dependencies fail.
"""


# NOTE: should front_depth not be set in the constructor?


def build_sirocco_workgraph(
    core_workflow: core.Workflow,
    front_depth: int = DEFAULT_FRONT_DEPTH,
) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows.

    Args:
        core_workflow: The core workflow to convert
        front_depth: Number of topological levels to keep active (default: 1, must be >= 1)
                    1 = no pre-submission - wait for level N to finish before submitting N+1
                    2 = one level ahead
                    higher values = more aggressive streaming submission

    Returns:
        A WorkGraph ready for submission

    Raises:
        ValueError: If workflow cannot run on AiiDA (e.g., missing computers)

    Note:
        The resolved config path (for provenance) is read from core_workflow.resolved_config_path
        and will be stored in the WorkGraph extras.

    Example::

        from sirocco import core
        from sirocco.engines.aiida import build_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Build the WorkGraph with front_depth=2
        wg = build_sirocco_workgraph(wf, front_depth=2)

        # Submit to AiiDA daemon
        wg.submit()
    """
    # Validate workflow can run on AiiDA (fail fast with clear errors)
    AiidaAdapter.validate_workflow(core_workflow)

    builder = WorkGraphBuilder(core_workflow)
    return builder.build(front_depth)


class WorkGraphBuilder:
    """Builds AiiDA WorkGraphs from Sirocco core workflows.

    This builder encapsulates the orchestration state and construction
    process while delegating transformations to the adapter and pure
    functions.

    Attributes:
        workflow: Core workflow to convert
        adapter: AiiDA adapter for domain translations
        resolved_config_path: Path to resolved config file (read from workflow.resolved_config_path).
            This is purely for provenance, to store the config with the workflow.
        data_nodes: Maps data labels to AiiDA nodes
        shell_specs: Pre-computed shell task specifications
        icon_specs: Pre-computed ICON task specifications
        task_outputs: Maps task_label -> dep_task.outputs namespace
        get_job_tasks: Maps task_label -> get_job_data task
        launcher_parents: Maps launcher_name -> [parent_launcher_names]
    """

    def __init__(self, core_workflow: core.Workflow):
        self.workflow = core_workflow
        self.resolved_config_path = core_workflow.resolved_config_path

        # Pre-computed static configuration
        self.data_nodes: dict[str, FileNode] = {}
        self.shell_specs: dict[str, AiidaShellTaskSpec] = {}
        self.icon_specs: dict[str, AiidaIconTaskSpec] = {}

        # Dynamic orchestration state
        self.dependency_outputs: TaskMonitorOutputsMapping = {}
        self.dependency_tasks: TaskDependencyMapping = {}
        self.launcher_parents: dict[str, list[str]] = {}  # launcher_name -> [parents]

        self._wg: WorkGraph | None = None
        self._wg_name: str | None = None

    def build(self, front_depth: int = DEFAULT_FRONT_DEPTH) -> WorkGraph:
        """Main entry point - orchestrates the build process.

        Args:
            front_depth: Number of topological levels to keep active (must be >= 1)
                1 = no pre-submission (default) - wait for level N to finish before submitting N+1)
                2 = one level ahead
                higher values = more aggressive streaming submission

        Returns:
            A WorkGraph ready for submission with window_config in extras
        """
        self._validate_labels()
        self._prepare_data_nodes()
        self._build_task_specs()
        wg = self._create_workgraph()
        self._store_window_config(wg, front_depth)
        return wg

    def _validate_labels(self) -> None:
        """Validate all workflow labels are AiiDA-compatible."""
        for core_task in self.workflow.tasks:
            try:
                validate_link_label(core_task.name)
            except ValueError as exception:
                msg = f"Raised error when validating task name '{core_task.name}': {exception.args[0]}"
                raise ValueError(msg) from exception
            for input_ in core_task.input_data_nodes():
                try:
                    validate_link_label(input_.name)
                except ValueError as exception:
                    msg = f"Raised error when validating input name '{input_.name}': {exception.args[0]}"
                    raise ValueError(msg) from exception
            for output in core_task.output_data_nodes():
                try:
                    validate_link_label(output.name)
                except ValueError as exception:
                    msg = f"Raised error when validating output name '{output.name}': {exception.args[0]}"
                    raise ValueError(msg) from exception

    def _prepare_data_nodes(self) -> None:
        """Create AiiDA data nodes for available data."""
        for data in self.workflow.data:
            if isinstance(data, core.AvailableData):
                label = AiidaAdapter.build_graph_item_label(data)
                # Check if any ICON task uses this data
                used_by_icon = any(
                    isinstance(task, core.IconTask) and data in task.input_data_nodes() for task in self.workflow.tasks
                )
                self.data_nodes[label] = AiidaAdapter.create_input_data_node(data, used_by_icon=used_by_icon)

    def _build_task_specs(self) -> None:
        """Build specifications for all tasks."""
        for task in self.workflow.tasks:
            label = AiidaAdapter.build_graph_item_label(task)
            match task:
                case core.ShellTask():
                    self.shell_specs[label] = build_shell_task_spec(task)
                case core.IconTask():
                    self.icon_specs[label] = build_icon_task_spec(task)

    def _create_workgraph(self) -> WorkGraph:
        """Create the WorkGraph with launcher tasks."""
        self._wg_name = self._generate_workgraph_name()
        self._wg = WorkGraph(self._wg_name)
        set_current_graph(self._wg)

        # Process all tasks in cycle order
        for cycle in self.workflow.cycles:
            for task in cycle.tasks:
                self._add_task_pair(task)

        return self._wg

    def _add_task_pair(self, task: core.Task) -> None:
        """Add launcher + monitor pair for a task.

        This creates two WorkGraph tasks:
        1. launch_{wg_name}_{task_label} - The actual computation launcher
        2. get_job_data_{task_label} - Monitors for job_id and remote_folder

        Also tracks launcher dependencies for window control.
        """
        task_label = AiidaAdapter.build_graph_item_label(task)

        # Collect inputs
        input_data = collect_available_data_inputs(task, self.data_nodes)

        # Build dependency mapping
        dependencies = build_dependency_mapping(task, self.workflow, self.dependency_outputs)

        # Track launcher dependencies for windowing
        # FIXME: No hard-coding, use actual distinction between Monitor and normal tasks
        launcher_name = f"{LAUNCHER_PREFIX}{self._wg_name}_{task_label}"
        self.launcher_parents[launcher_name] = [
            f"{LAUNCHER_PREFIX}{self._wg_name}_{AiidaAdapter.build_graph_item_label(dep_task)}"
            for dep_label in dependencies.task_folders
            # Find the task object from the label
            for dep_task in self.workflow.tasks
            if AiidaAdapter.build_graph_item_label(dep_task) == dep_label
        ]

        # Create launcher pair based on task type
        match task:
            case core.IconTask():
                add_icon_task_pair(
                    self._wg,
                    self._wg_name,  # type: ignore[arg-type]
                    task_label,
                    self.icon_specs[task_label],
                    input_data,
                    dependencies,
                    self.dependency_outputs,
                    self.dependency_tasks,
                )
            case core.ShellTask():
                add_shell_task_pair(
                    self._wg,
                    self._wg_name,  # type: ignore[arg-type]
                    task_label,
                    self.shell_specs[task_label],
                    input_data,
                    dependencies,
                    self.dependency_outputs,
                    self.dependency_tasks,
                )

    # TODO: Check if WG has a specific `Monitor` task, then we don't have to hard-code `launch_` anymore
    def _generate_workgraph_name(self) -> str:
        """Generate unique WorkGraph name with timestamp."""
        base_name = self.workflow.name or "SIROCCO_WF"
        timestamp = datetime.now(ZoneInfo("Europe/Zurich")).strftime("%Y_%m_%d_%H_%M")
        return f"{base_name}_{timestamp}"

    def _store_window_config(self, wg: WorkGraph, front_depth: int) -> None:
        """Store window configuration and resolved config in WorkGraph extras.

        Args:
            front_depth: Must be >= 1. 1=sequential, 2+=lookahead
        """
        if front_depth < 1:
            msg = f"front_depth must be >= 1, got {front_depth}"
            raise ValueError(msg)

        window_config = {
            "enabled": True,  # NOTE: Always enabled now, thus, even needed?
            "front_depth": front_depth,
            "task_dependencies": self.launcher_parents,
        }

        extras: dict[str, Any] = {"window_config": window_config}

        # Store resolved config as SinglefileData if available
        if self.resolved_config_path and Path(self.resolved_config_path).exists():
            resolved_config_node = aiida.orm.SinglefileData(file=self.resolved_config_path)
            resolved_config_node.label = f"resolved_config_{self.workflow.name}"
            resolved_config_node.description = (
                f"Resolved configuration file (with Jinja2 variables replaced) for workflow {self.workflow.name}"
            )
            resolved_config_node.store()
            node_pk = resolved_config_node.pk
            if node_pk is not None:
                extras["resolved_config_pk"] = node_pk
                LOGGER.info("Stored resolved config as SinglefileData (PK: %s)", node_pk)

        wg.extras = extras
