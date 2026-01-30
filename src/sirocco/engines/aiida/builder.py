"""WorkGraph builder - orchestrates WorkGraph construction from core workflows."""

from __future__ import annotations

import hashlib
import io
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import aiida.orm
from aiida.common import validate_link_label
from aiida.common.exceptions import NotExistent
from aiida_icon.iconutils.namelists import create_namelist_singlefiledata_from_content
from aiida_shell.parsers.shell import ShellParser
from aiida_workgraph import WorkGraph
from aiida_workgraph.manager import set_current_graph

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.dependencies import (
    build_dependency_mapping,
    collect_available_data_inputs,
)
from sirocco.engines.aiida.launcher import (
    create_icon_launcher_pair,
    create_shell_launcher_pair,
)
from sirocco.engines.aiida.types import (
    AiidaIconTaskSpec,
    AiidaShellTaskSpec,
    InputDataInfo,
    OutputDataInfo,
)
from sirocco.engines.aiida.utils import (
    get_wrapper_script_aiida_data,
    serialize_coordinates,
    split_cmd_arg,
)

if TYPE_CHECKING:
    from sirocco.engines.aiida.types import (
        AiidaFileNode,
        DependencyOutputs,
        DependencyTasks,
    )

LOGGER = logging.getLogger(__name__)


def build_sirocco_workgraph(
    core_workflow: core.Workflow,
    front_depth: int = 1,
    resolved_config_path: str | None = None,
) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows.

    Args:
        core_workflow: The core workflow to convert
        front_depth: Number of topological levels to keep active (default: 1, must be >= 1)
                    1 = no pre-submission - wait for level N to finish before submitting N+1
                    2 = one level ahead
                    higher values = more aggressive streaming submission
        resolved_config_path: Optional path to the resolved config file (with Jinja2 variables replaced).
            Purely for provenance - stores the config with the workflow.

    Returns:
        A WorkGraph ready for submission

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
    builder = WorkGraphBuilder(core_workflow, resolved_config_path=resolved_config_path)
    return builder.build(front_depth)


class WorkGraphBuilder:
    """Builds AiiDA WorkGraphs from Sirocco core workflows.

    This builder encapsulates the orchestration state and construction
    process while delegating transformations to the adapter and pure
    functions.

    Attributes:
        workflow: Core workflow to convert
        adapter: AiiDA adapter for domain translations
        resolved_config_path: Optional path to resolved config file
            (this is purely for provenance, to store the config with the workflow)
        data_nodes: Maps data labels to AiiDA nodes
        shell_specs: Pre-computed shell task specifications
        icon_specs: Pre-computed ICON task specifications
        task_outputs: Maps task_label -> dep_task.outputs namespace
        get_job_tasks: Maps task_label -> get_job_data task
        launcher_deps: Maps launcher_name -> [parent_launcher_names]
    """

    def __init__(self, core_workflow: core.Workflow, resolved_config_path: str | None = None):
        self.workflow = core_workflow
        self.adapter = AiidaAdapter(core_workflow)
        self.resolved_config_path = resolved_config_path

        # Pre-computed static configuration
        self.data_nodes: dict[str, AiidaFileNode] = {}
        self.shell_specs: dict[str, AiidaShellTaskSpec] = {}
        self.icon_specs: dict[str, AiidaIconTaskSpec] = {}

        # Dynamic orchestration state
        self.dependency_outputs: DependencyOutputs = {}
        self.dependency_tasks: DependencyTasks = {}
        self.launcher_deps: dict[str, list[str]] = {}  # launcher_name -> [parents]

        self._wg: WorkGraph | None = None
        self._wg_name: str | None = None

    def build(self, front_depth: int = 1) -> WorkGraph:
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
                label = self.adapter.get_graph_item_label(data)
                self.data_nodes[label] = self.adapter.create_input_data_node(data)

    def _build_task_specs(self) -> None:
        """Build specifications for all tasks."""
        for task in self.workflow.tasks:
            label = self.adapter.get_graph_item_label(task)
            match task:
                case core.ShellTask():
                    self.shell_specs[label] = self.build_shell_task_spec(task, self.adapter)
                case core.IconTask():
                    self.icon_specs[label] = self.build_icon_task_spec(task, self.adapter)

    def _create_workgraph(self) -> WorkGraph:
        """Create the WorkGraph with launcher tasks."""
        self._wg_name = self._generate_workgraph_name()
        self._wg = WorkGraph(self._wg_name)
        set_current_graph(self._wg)

        # Process all tasks in cycle order
        for cycle in self.workflow.cycles:
            for task in cycle.tasks:
                self._add_launcher_pair(task)

        return self._wg

    def _add_launcher_pair(self, task: core.Task) -> None:
        """Add launcher + monitor pair for a task.

        This creates two WorkGraph tasks:
        1. launch_{wg_name}_{task_label} - The actual computation launcher
        2. get_job_data_{task_label} - Monitors for job_id and remote_folder

        Also tracks launcher dependencies for window control.
        """
        task_label = self.adapter.get_graph_item_label(task)

        # Collect inputs
        input_data = collect_available_data_inputs(task, self.data_nodes)

        # Build dependency mapping
        port_to_dep, parent_folders, job_ids = build_dependency_mapping(task, self.workflow, self.dependency_outputs)

        # Track launcher dependencies for windowing
        # FIXME: No hard-coding, use actual distinction between Monitor and normal tasks
        launcher_name = f"launch_{self._wg_name}_{task_label}"
        self.launcher_deps[launcher_name] = [
            f"launch_{self._wg_name}_{self.adapter.get_graph_item_label(dep_task)}"
            for dep_label in parent_folders
            # Find the task object from the label
            for dep_task in self.workflow.tasks
            if self.adapter.get_graph_item_label(dep_task) == dep_label
        ]

        # Create launcher pair based on task type
        match task:
            case core.IconTask():
                self.dependency_outputs, self.dependency_tasks = create_icon_launcher_pair(
                    self._wg,
                    self._wg_name,  # type: ignore[arg-type]
                    task_label,
                    self.icon_specs[task_label],
                    input_data,
                    parent_folders,
                    job_ids,
                    port_to_dep,
                    self.dependency_outputs,
                    self.dependency_tasks,
                )
            case core.ShellTask():
                self.dependency_outputs, self.dependency_tasks = create_shell_launcher_pair(
                    self._wg,
                    self._wg_name,  # type: ignore[arg-type]
                    task_label,
                    self.shell_specs[task_label],
                    input_data,
                    parent_folders,
                    job_ids,
                    port_to_dep,
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
            "task_dependencies": self.launcher_deps,
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

    @staticmethod
    def build_shell_task_spec(task: core.ShellTask, adapter: AiidaAdapter) -> AiidaShellTaskSpec:
        """Build all parameters needed to create a shell task.

        Returns a dict with keys: label, code, nodes, metadata,
        arguments_template, filenames, outputs, input_data_info, output_data_info

        NOTE: Job dependencies are NOT included here - they're added at runtime.

        Args:
            task: The ShellTask to build spec for
            adapter: AiiDA adapter for translations

        Returns:
            Dict containing all shell task parameters
        """
        label = AiidaAdapter.get_graph_item_label(task)

        # Get computer
        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        # Build base metadata (no job dependencies yet)
        metadata = adapter.build_metadata(task)

        # Add shell-specific metadata options
        if metadata.options:
            metadata = metadata.model_copy(
                update={"options": metadata.options.model_copy(update={"use_symlinks": True})}
            )

        # Create or load code
        code = adapter.create_shell_code(task, computer)

        # Pre-compute input data information using dataclasses
        input_data_info: list[InputDataInfo] = []
        for port_name, input_ in task.input_data_items():
            input_info = InputDataInfo(
                port=port_name,
                name=input_.name,
                coordinates=serialize_coordinates(input_.coordinates),
                label=AiidaAdapter.get_graph_item_label(input_),
                is_available=isinstance(input_, core.AvailableData),
                path=str(input_.path) if input_.path is not None else "",  # type: ignore[attr-defined]
            )
            input_data_info.append(input_info)

        # Build input labels for argument resolution
        input_labels: dict[str, list[str]] = {}
        for input_info in input_data_info:
            port_name = input_info.port
            input_label = input_info.label
            if port_name not in input_labels:
                input_labels[port_name] = []
            # For AvailableData with a path, use the actual path directly in command arguments
            # instead of creating a placeholder, since these are pre-existing files/directories
            if input_info.is_available and input_info.path:
                input_labels[port_name].append(input_info.path)
            else:
                input_labels[port_name].append(f"{{{input_label}}}")

        # Pre-compute output data information using dataclasses
        output_data_info: list[OutputDataInfo] = []
        for port_name, output in task.output_data_items():  # type: ignore[assignment]
            output_info = OutputDataInfo(
                name=output.name,
                coordinates=serialize_coordinates(output.coordinates),
                label=AiidaAdapter.get_graph_item_label(output),
                path=str(output.path) if output.path is not None else "",  # type: ignore[attr-defined]
                port=port_name,
            )
            output_data_info.append(output_info)

        # Build output labels
        output_labels: dict[str, list[str]] = {}
        for output_info in output_data_info:
            port_name = output_info.port  # type: ignore[assignment]
            output_label = output_info.label
            if port_name not in output_labels:
                output_labels[port_name] = []
            # For AvailableData with a path, use the actual path directly in command arguments
            if output_info.path:
                output_labels[port_name].append(output_info.path)
            else:
                output_labels[port_name].append(f"{{{output_label}}}")

        # Pre-scan command template to find all referenced ports
        # This ensures optional/missing ports are included with empty lists
        for port_match in task.port_pattern.finditer(task.command):
            port_name = port_match.group(2)
            if port_name and port_name not in input_labels:
                input_labels[port_name] = []

        # Pre-resolve arguments template
        # Get script name from task.path for proper command splitting
        script_name = Path(task.path).name if task.path else None

        # Merge output labels into input labels for resolution
        input_labels.update(output_labels)
        arguments_with_placeholders = task.resolve_ports(input_labels)  # type: ignore[arg-type]

        _, resolved_arguments_template = split_cmd_arg(arguments_with_placeholders, script_name)

        # Build filenames mapping
        filenames = {}
        for input_info in input_data_info:
            input_label = input_info.label
            if input_info.is_available:
                filenames[input_info.name] = Path(input_info.path).name if input_info.path else input_info.name  # type: ignore[arg-type]
            else:
                # Count how many inputs have the same name
                same_name_count = sum(1 for info in input_data_info if info.name == input_info.name)
                if same_name_count > 1:
                    filenames[input_label] = input_label
                else:
                    filenames[input_label] = Path(input_info.path).name if input_info.path else input_info.name  # type: ignore[arg-type]

        # Build outputs list - but DON'T retrieve, just verify existence
        outputs = []  # type: ignore[var-annotated]

        # Build output port mapping: data_name -> shell output link_label
        output_port_mapping = {}
        for output_info in output_data_info:
            if output_info.path:
                link_label = ShellParser.format_link_label(output_info.path)  # type: ignore[arg-type]
                output_port_mapping[output_info.name] = link_label

        if code.pk is None:
            msg = f"Code for task {label} must be stored before creating task spec"
            raise RuntimeError(msg)

        return AiidaShellTaskSpec(
            label=label,
            code_pk=code.pk,
            node_pks={},
            metadata=metadata,
            arguments_template=resolved_arguments_template,
            filenames=filenames,
            outputs=outputs,
            input_data_info=[info.model_dump() for info in input_data_info],
            output_data_info=[info.model_dump() for info in output_data_info],
            output_port_mapping=output_port_mapping,
        )

    @staticmethod
    def build_icon_task_spec(task: core.IconTask, adapter: AiidaAdapter) -> AiidaIconTaskSpec:
        """Build all parameters needed to create an ICON task.

        Returns a dict with keys: label, builder, output_ports

        Note: Job dependencies are NOT included here - they're added at runtime.

        Args:
            task: The IconTask to build spec for
            adapter: AiiDA adapter for translations

        Returns:
            Dict containing all ICON task parameters
        """
        task_label = AiidaAdapter.get_graph_item_label(task)

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        # Create or load ICON code with unique label based on executable path
        bin_hash = hashlib.sha256(str(task.bin).encode()).hexdigest()[:8]
        icon_code_label = f"icon-{bin_hash}"
        try:
            icon_code = aiida.orm.load_code(f"{icon_code_label}@{computer.label}")
        except NotExistent:
            icon_code = aiida.orm.InstalledCode(
                label=icon_code_label,
                description=f"ICON executable: {task.bin}",
                default_calc_job_plugin="icon.icon",
                computer=computer,
                filepath_executable=str(task.bin),
                with_mpi=True,
                use_double_quotes=True,
            )
            icon_code.store()

        # Build base metadata (no job dependencies yet)
        metadata = adapter.build_metadata(task)

        # Update task namelists
        task.update_icon_namelists_from_workflow()

        # Master namelist - store as PK with parsed content for queryability
        with io.StringIO() as buffer:
            task.master_namelist.namelist.write(buffer)
            content = buffer.getvalue()
            master_namelist_node = create_namelist_singlefiledata_from_content(
                content, task.master_namelist.name, store=True
            )

        # Model namelists - store as PKs with parsed content for queryability
        model_namelist_pks = {}
        for model_name, model_nml in task.model_namelists.items():
            with io.StringIO() as buffer:
                model_nml.namelist.write(buffer)
                content = buffer.getvalue()
                model_node = create_namelist_singlefiledata_from_content(content, model_nml.name, store=True)
                model_namelist_pks[model_name] = model_node.pk

        # Wrapper script - store as PK if present
        wrapper_script_pk = None
        wrapper_script_data = get_wrapper_script_aiida_data(task)
        if wrapper_script_data is not None:
            wrapper_script_data.store()
            wrapper_script_pk = wrapper_script_data.pk

        # Pre-compute output port mapping: data_name -> icon_port_name
        output_port_mapping: dict[str, str] = {}
        for port_name, output_list in task.outputs.items():
            if port_name is not None:
                for data in output_list:
                    output_port_mapping[data.name] = port_name

        # Pydantic validation will check that PKs are not None
        return AiidaIconTaskSpec(
            label=task_label,
            code_pk=icon_code.pk,  # type: ignore[arg-type]
            master_namelist_pk=master_namelist_node.pk,  # type: ignore[arg-type]
            model_namelist_pks=model_namelist_pks,  # type: ignore[arg-type]
            wrapper_script_pk=wrapper_script_pk,
            metadata=metadata,
            output_port_mapping=output_port_mapping,
        )
