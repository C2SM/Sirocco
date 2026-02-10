"""WorkGraph builder - orchestrates WorkGraph construction from core workflows."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import aiida.orm
from aiida_workgraph import WorkGraph
from aiida_workgraph.manager import set_current_graph

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.dependency_resolvers import (
    build_dependency_mapping,
    resolve_available_data_inputs,
)
from sirocco.engines.aiida.spec_builders import (
    IconTaskSpecBuilder,
    ShellTaskSpecBuilder,
)
from sirocco.engines.aiida.task_pairs import (
    LAUNCHER_PREFIX,
    TaskPairContext,
)

if TYPE_CHECKING:
    from sirocco.engines.aiida.models import (
        AiidaIconTaskSpec,
        AiidaShellTaskSpec,
    )
    from sirocco.engines.aiida.types import (
        FileNode,
    )

LOGGER = logging.getLogger(__name__)

# TODO: Check if WG has a specific `Monitor` task, then we don't have to hard-code `launch_` anymore


def build_sirocco_workgraph(
    core_workflow: core.Workflow,
) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows.

    Args:
        core_workflow: The core workflow to convert (front_depth is read from workflow.front_depth)

    Returns:
        A WorkGraph ready for submission

    Raises:
        ValueError: If workflow cannot run on AiiDA (e.g., missing computers, invalid labels)

    Note:
        The resolved config path (for provenance) is read from core_workflow.resolved_config_path
        and will be stored in the WorkGraph extras.

        The front_depth configuration (number of topological levels to keep active) is read from
        core_workflow.front_depth:
            1 = no pre-submission - wait for level N to finish before submitting N+1
            2 = one level ahead
            higher values = more aggressive streaming submission

    Example::

        from sirocco import core
        from sirocco.engines.aiida import build_sirocco_workgraph

        # Build your core workflow (front_depth is configured in the YAML)
        wf = core.Workflow.from_config_file("workflow.yml")

        # Build the WorkGraph
        wg = build_sirocco_workgraph(wf)

        # Submit to AiiDA daemon
        wg.submit()
    """
    builder = WorkGraphBuilder(core_workflow)
    return builder.build()


class WorkGraphBuilder:
    """Builds AiiDA WorkGraphs from Sirocco core workflows.

    This builder encapsulates the orchestration state and construction
    process while delegating transformations to the adapter and pure
    functions.

    Attributes:
        workflow: Core workflow to convert
        resolved_config_path: Path to resolved config file (read from workflow.resolved_config_path).
            This is purely for provenance, to store the config with the workflow.
        data_nodes: Maps data labels to AiiDA nodes
        shell_specs: Pre-computed shell task specifications
        icon_specs: Pre-computed ICON task specifications
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
        self.launcher_parents: dict[str, list[str]] = {}  # launcher_name -> [parents]

        self._wg: WorkGraph | None = None
        self._wg_name: str | None = None
        self._task_pair_context: TaskPairContext  # Initialized in _create_workgraph()

    def build(self) -> WorkGraph:
        """Main entry point - orchestrates the build process.

        Returns:
            A WorkGraph ready for submission with window_config in extras

        Raises:
            ValueError: If workflow cannot run on AiiDA (e.g., missing computers, invalid labels)

        Note:
            front_depth is read from self.workflow.front_depth
        """
        # Validate workflow can run on AiiDA (fail fast with clear errors)
        AiidaAdapter.validate_workflow(self.workflow)

        self._prepare_data_nodes()
        self._build_task_specs()
        wg = self._create_workgraph()
        self._store_window_config(wg, self.workflow.front_depth)
        return wg

    def _prepare_data_nodes(self) -> None:
        """Create AiiDA data nodes for available data."""
        for data in self.workflow.data:
            if isinstance(data, core.AvailableData):
                label = AiidaAdapter.build_label_from_graph_item(data)
                # Check if any ICON task uses this data
                used_by_icon = any(
                    isinstance(task, core.IconTask) and data in task.input_data_nodes() for task in self.workflow.tasks
                )
                self.data_nodes[label] = AiidaAdapter.create_input_data_node(data, used_by_icon=used_by_icon)

    def _build_task_specs(self) -> None:
        """Build specifications for all tasks."""
        for task in self.workflow.tasks:
            label = AiidaAdapter.build_label_from_graph_item(task)
            match task:
                case core.ShellTask():
                    self.shell_specs[label] = ShellTaskSpecBuilder(task).build_spec()
                case core.IconTask():
                    self.icon_specs[label] = IconTaskSpecBuilder(task).build_spec()

    def _create_workgraph(self) -> WorkGraph:
        """Create the WorkGraph with launcher tasks."""
        self._wg_name = self._generate_workgraph_name()
        self._wg = WorkGraph(self._wg_name)
        set_current_graph(self._wg)

        # Create task pair context for managing task creation
        self._task_pair_context = TaskPairContext(self._wg, self._wg_name)

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
        task_label = AiidaAdapter.build_label_from_graph_item(task)

        # Collect inputs
        input_data = resolve_available_data_inputs(task, self.data_nodes)

        # Build dependency mapping using context's accumulated outputs
        dependency_outputs = self._task_pair_context.get_dependency_outputs()
        dependencies = build_dependency_mapping(task, self.workflow, dependency_outputs)

        # Track launcher dependencies for windowing
        # FIXME: No hard-coding, use actual distinction between Monitor and normal tasks
        launcher_name = f"{LAUNCHER_PREFIX}{self._wg_name}_{task_label}"
        self.launcher_parents[launcher_name] = [
            f"{LAUNCHER_PREFIX}{self._wg_name}_{AiidaAdapter.build_label_from_graph_item(dep_task)}"
            for dep_label in dependencies.task_folders
            # Find the task object from the label
            for dep_task in self.workflow.tasks
            if AiidaAdapter.build_label_from_graph_item(dep_task) == dep_label
        ]

        # Create launcher pair based on task type using context
        match task:
            case core.IconTask():
                self._task_pair_context.add_icon_task_pair(
                    task_label,
                    self.icon_specs[task_label],
                    input_data,
                    dependencies,
                )
            case core.ShellTask():
                self._task_pair_context.add_shell_task_pair(
                    task_label,
                    self.shell_specs[task_label],
                    input_data,
                    dependencies,
                )

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
