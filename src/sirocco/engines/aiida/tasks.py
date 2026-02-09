"""Launcher task creation and monitoring functions."""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import aiida.orm
import yaml
from aiida.common.exceptions import NotExistent
from aiida.orm.utils.serialize import AiiDALoader
from aiida_icon.calculations import IconCalculation
from aiida_icon.iconutils.namelists import create_namelist_singlefiledata_from_content
from aiida_shell.parsers.shell import ShellParser
from aiida_workgraph import dynamic, get_current_graph, namespace, task

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.code_factory import CodeFactory
from sirocco.engines.aiida.dependencies import (
    add_slurm_dependencies_to_metadata,
    build_icon_calcjob_inputs,
    resolve_icon_dependency_mapping,
    resolve_shell_dependency_mappings,
)
from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    AiidaShellTaskSpec,
    DependencyInfo,
    DependencyMapping,
    InputDataInfo,
    OutputDataInfo,
)
from sirocco.engines.aiida.utils import serialize_coordinates, split_cmd_arg

if TYPE_CHECKING:
    from collections.abc import Callable

    from aiida_workgraph import WorkGraph

    from sirocco.engines.aiida.types import (
        PortDataMapping,
        TaskDependencyMapping,
        TaskMonitorOutputsMapping,
        WgTaskProtocol,
    )

LOGGER = logging.getLogger(__name__)

# Task name prefixes for WorkGraph tasks
# TODO: Check if WG has a specific `Monitor` task type to avoid hard-coding
LAUNCHER_PREFIX = "launch_"
MONITOR_PREFIX = "get_job_data_"


class TaskSpecBuilder(ABC):
    """Base class for building task specifications.

    Provides common logic for label generation, computer loading, and metadata
    building that is shared between Shell and ICON task spec builders.

    The builder pattern extracts common initialization steps and defers
    task-specific logic (code creation, input/output handling) to subclasses.

    Attributes:
        task: The core task to build a spec for
        label: AiiDA-compatible label for the task
        computer: AiiDA computer object
        metadata: AiiDA metadata with scheduler options
    """

    def __init__(self, task: core.Task):
        """Initialize common task spec components.

        Args:
            task: The core task (ShellTask or IconTask)
        """
        self.task = task
        self.label = AiidaAdapter.build_graph_item_label(task)
        self.computer = aiida.orm.Computer.collection.get(label=task.computer)
        self.metadata = AiidaAdapter.build_metadata(task)

    @abstractmethod
    def create_or_load_code(self) -> aiida.orm.Code:
        """Create or load the appropriate AiiDA Code for this task.

        Returns:
            AiiDA Code object (PortableCode, InstalledCode, or ShellCode)
        """
        ...

    @abstractmethod
    def build_output_port_mapping(self) -> dict[str, str]:
        """Build mapping from data names to output port names.

        Returns:
            Dict mapping output data names to their port names
        """
        ...

    @abstractmethod
    def build_spec(self) -> AiidaIconTaskSpec | AiidaShellTaskSpec:
        """Build the complete task specification.

        Returns:
            Task specification model (IconTaskSpec or ShellTaskSpec)
        """
        ...


class ShellTaskSpecBuilder(TaskSpecBuilder):
    """Builder for shell task specifications.

    Handles all Shell-specific logic including:
    - Creating/loading Shell codes (portable or installed)
    - Processing command arguments and placeholders
    - Building input/output data information
    - Creating filename mappings
    """

    task: core.ShellTask

    def __init__(self, task: core.ShellTask):
        """Initialize shell task spec builder.

        Args:
            task: The ShellTask to build spec for
        """
        super().__init__(task)
        self.task = task  # Narrow type from Task to ShellTask
        # Add shell-specific metadata option
        if self.metadata.options:
            self.metadata = self.metadata.model_copy(
                update={"options": self.metadata.options.model_copy(update={"use_symlinks": True})}
            )

    def create_or_load_code(self) -> aiida.orm.Code:
        """Create or load Shell code (PortableCode or InstalledCode).

        Returns:
            AiiDA Code for the shell script/executable
        """
        return CodeFactory.create_shell_code(self.task, self.computer)

    def build_input_data_info(self) -> list[InputDataInfo]:
        """Build input data information for all input ports.

        Returns:
            List of InputDataInfo objects describing each input
        """
        input_data_info = []
        for port_name, input_ in self.task.input_data_items():
            input_info = InputDataInfo(
                port=port_name,
                name=input_.name,
                coordinates=serialize_coordinates(input_.coordinates),
                label=AiidaAdapter.build_graph_item_label(input_),
                is_available=isinstance(input_, core.AvailableData),
                path=str(input_.path) if input_.path is not None else "",  # type: ignore[attr-defined]
            )
            input_data_info.append(input_info)
        return input_data_info

    def build_output_data_info(self) -> list[OutputDataInfo]:
        """Build output data information for all output ports.

        Returns:
            List of OutputDataInfo objects describing each output
        """
        output_data_info = []
        for port_name, output in self.task.output_data_items():
            output_info = OutputDataInfo(
                name=output.name,
                coordinates=serialize_coordinates(output.coordinates),
                label=AiidaAdapter.build_graph_item_label(output),
                path=str(output.path) if output.path is not None else "",
                port=port_name,
            )
            output_data_info.append(output_info)
        return output_data_info

    def build_output_port_mapping(self) -> dict[str, str]:
        """Build mapping from output data names to shell parser link labels.

        Returns:
            Dict mapping data names to shell output link labels
        """
        output_port_mapping = {}
        output_data_info = self.build_output_data_info()
        for output_info in output_data_info:
            if output_info.path:
                link_label = ShellParser.format_link_label(output_info.path)
                output_port_mapping[output_info.name] = link_label
        return output_port_mapping

    def build_spec(self) -> AiidaShellTaskSpec:
        """Build the complete shell task specification.

        Returns:
            AiidaShellTaskSpec with all parameters needed to launch the task
        """
        # Create or load code
        code = self.create_or_load_code()

        # Build input and output data info
        input_data_info = self.build_input_data_info()
        output_data_info = self.build_output_data_info()

        # Build input labels for argument resolution
        input_labels: dict[str, list[str]] = {}
        for input_info in input_data_info:
            port_name = input_info.port
            input_label = input_info.label
            if port_name not in input_labels:
                input_labels[port_name] = []
            # For AvailableData with a path, use the actual path directly
            if input_info.is_available and input_info.path:
                input_labels[port_name].append(input_info.path)
            else:
                input_labels[port_name].append(f"{{{input_label}}}")

        # Build output labels
        output_labels: dict[str, list[str]] = {}
        for output_info in output_data_info:
            if output_info.port is None:
                continue
            port_name = output_info.port
            output_label = output_info.label
            if port_name not in output_labels:
                output_labels[port_name] = []
            if output_info.path:
                output_labels[port_name].append(output_info.path)
            else:
                output_labels[port_name].append(f"{{{output_label}}}")

        # Pre-scan command template to find all referenced ports
        for port_match in self.task.port_pattern.finditer(self.task.command):
            port_name = port_match.group(2)
            if port_name and port_name not in input_labels:
                input_labels[port_name] = []

        # Pre-resolve arguments template
        script_name = Path(self.task.path).name if self.task.path else None
        input_labels.update(output_labels)
        arguments_with_placeholders = self.task.resolve_ports(input_labels)  # type: ignore[arg-type]
        _, resolved_arguments_template = split_cmd_arg(arguments_with_placeholders, script_name)

        # Build filenames mapping
        filenames = {}
        for input_info in input_data_info:
            input_label = input_info.label
            if input_info.is_available:
                filenames[input_info.name] = Path(input_info.path).name if input_info.path else input_info.name
            else:
                same_name_count = sum(1 for info in input_data_info if info.name == input_info.name)
                if same_name_count > 1:
                    filenames[input_label] = input_label
                else:
                    filenames[input_label] = Path(input_info.path).name if input_info.path else input_info.name

        # Build output port mapping
        output_port_mapping = self.build_output_port_mapping()

        if code.pk is None:
            msg = f"Code for task {self.label} must be stored before creating task spec"
            raise RuntimeError(msg)

        return AiidaShellTaskSpec(
            label=self.label,
            code_pk=code.pk,
            node_pks={},
            metadata=self.metadata,
            arguments_template=resolved_arguments_template,
            filenames=filenames,
            outputs=[],
            input_data_info=[info.model_dump() for info in input_data_info],
            output_data_info=[info.model_dump() for info in output_data_info],
            output_port_mapping=output_port_mapping,
        )


class IconTaskSpecBuilder(TaskSpecBuilder):
    """Builder for ICON task specifications.

    Handles all ICON-specific logic including:
    - Creating/loading ICON codes
    - Processing namelists (master and model)
    - Handling wrapper scripts
    - Building output port mapping from task outputs
    """

    task: core.IconTask

    def __init__(self, task: core.IconTask):
        """Initialize ICON task spec builder.

        Args:
            task: The IconTask to build spec for
        """
        super().__init__(task)
        self.task = task  # Narrow type from Task to IconTask

    def create_or_load_code(self) -> aiida.orm.Code:
        """Create or load ICON InstalledCode.

        Returns:
            AiiDA InstalledCode for ICON executable
        """
        bin_hash = hashlib.sha256(str(self.task.bin).encode()).hexdigest()[:8]
        icon_code_label = f"icon-{bin_hash}"
        try:
            return aiida.orm.load_code(f"{icon_code_label}@{self.computer.label}")
        except NotExistent:
            icon_code = aiida.orm.InstalledCode(
                label=icon_code_label,
                description=f"ICON executable: {self.task.bin}",
                default_calc_job_plugin="icon.icon",
                computer=self.computer,
                filepath_executable=str(self.task.bin),
                with_mpi=True,
                use_double_quotes=True,
            )
            icon_code.store()
            return icon_code

    def build_output_port_mapping(self) -> dict[str, str]:
        """Build mapping from output data names to ICON port names.

        Returns:
            Dict mapping data names to their ICON output port names
        """
        output_port_mapping: dict[str, str] = {}
        for port_name, output_list in self.task.outputs.items():
            if port_name is not None:
                for data in output_list:
                    output_port_mapping[data.name] = port_name
        return output_port_mapping

    def build_spec(self) -> AiidaIconTaskSpec:
        """Build the complete ICON task specification.

        Returns:
            AiidaIconTaskSpec with all parameters needed to launch the task
        """
        # Create or load code
        icon_code = self.create_or_load_code()

        # Update task namelists
        self.task.update_icon_namelists_from_workflow()

        # Master namelist - store as PK with parsed content
        with io.StringIO() as buffer:
            self.task.master_namelist.namelist.write(buffer)
            content = buffer.getvalue()
            master_namelist_node = create_namelist_singlefiledata_from_content(
                content, self.task.master_namelist.name, store=True
            )

        # Model namelists - store as PKs with parsed content
        model_namelist_pks = {}
        for model_name, model_nml in self.task.model_namelists.items():
            with io.StringIO() as buffer:
                model_nml.namelist.write(buffer)
                content = buffer.getvalue()
                model_node = create_namelist_singlefiledata_from_content(content, model_nml.name, store=True)
                model_namelist_pks[model_name] = model_node.pk

        # Wrapper script - store as PK if present
        wrapper_script_pk = None
        wrapper_script_data = AiidaAdapter.get_wrapper_script_data(self.task)
        if wrapper_script_data is not None:
            wrapper_script_data.store()
            wrapper_script_pk = wrapper_script_data.pk

        # Build output port mapping
        output_port_mapping = self.build_output_port_mapping()

        # Pydantic validation will check that PKs are not None
        return AiidaIconTaskSpec(
            label=self.label,
            code_pk=icon_code.pk,  # type: ignore[arg-type]
            master_namelist_pk=master_namelist_node.pk,  # type: ignore[arg-type]
            model_namelist_pks=model_namelist_pks,  # type: ignore[arg-type]
            wrapper_script_pk=wrapper_script_pk,
            metadata=self.metadata,
            output_port_mapping=output_port_mapping,
        )


class SpecResolver(ABC):
    """Base class for resolving task specifications at runtime.

    Separates high-level orchestration from low-level implementation details
    by providing a clear structure for runtime task execution. Subclasses
    implement task-specific logic for loading resources, resolving dependencies,
    and creating WorkGraph tasks.

    Attributes:
        spec: The loaded and validated task specification model
    """

    def __init__(self, task_spec_dict: dict):
        """Initialize resolver with task specification.

        Args:
            task_spec_dict: Serialized task specification from WorkGraph
        """
        self.spec = self._load_spec(task_spec_dict)

    @abstractmethod
    def _load_spec(self, spec_dict: dict) -> AiidaIconTaskSpec | AiidaShellTaskSpec:
        """Load and validate the task specification.

        Args:
            spec_dict: Serialized specification dictionary

        Returns:
            Validated Pydantic model for the task spec
        """
        ...

    @abstractmethod
    def execute(
        self,
        input_data_nodes: dict | None = None,
        task_folders: dict | None = None,
        task_job_ids: dict | None = None,
    ) -> Annotated[dict, dynamic()]:
        """Execute the task with given inputs and dependencies.

        Args:
            input_data_nodes: Dict mapping port names to AvailableData nodes
            task_folders: Dict of {task_label: remote_folder_pk} from upstream tasks
            task_job_ids: Dict of {task_label: job_id} from upstream tasks

        Returns:
            WorkGraph task outputs
        """
        ...


class ShellSpecResolver(SpecResolver):
    """Resolver for Shell tasks at runtime.

    Handles all runtime logic for shell task execution including:
    - Loading code and nodes from PKs
    - Remapping input data nodes to match argument placeholders
    - Resolving dependency mappings
    - Building metadata with SLURM dependencies
    - Creating WorkGraph shell task
    """

    spec: AiidaShellTaskSpec

    def _load_spec(self, spec_dict: dict) -> AiidaShellTaskSpec:
        """Load Shell task specification.

        Args:
            spec_dict: Serialized AiidaShellTaskSpec

        Returns:
            Validated AiidaShellTaskSpec model
        """
        return AiidaShellTaskSpec(**spec_dict)

    def _load_code(self) -> aiida.orm.Code:
        """Load the code from its stored PK.

        Returns:
            AiiDA Code node
        """
        return aiida.orm.load_code(pk=self.spec.code_pk)

    def _build_base_nodes(self) -> dict[str, aiida.orm.Data]:
        """Load all pre-stored nodes from their PKs.

        Returns:
            Dict mapping node keys to AiiDA Data nodes
        """
        return {key: aiida.orm.load_node(pk) for key, pk in self.spec.node_pks.items()}  # type: ignore[misc]

    def _build_port_to_label_mapping(self) -> dict[str, str]:
        """Build mapping from input port names to data labels.

        This is needed because WorkGraph passes nodes by port name, but
        argument placeholders use data labels.

        Returns:
            Dict mapping port names to data labels for AvailableData
        """
        port_to_label = {}
        for input_info in self.spec.input_data_info:
            if input_info["is_available"]:
                port_to_label[input_info["port"]] = input_info["label"]
        return port_to_label

    def _add_input_data_nodes(
        self, all_nodes: dict[str, aiida.orm.Data], input_data_nodes: dict | None
    ) -> dict[str, str]:
        """Add AvailableData nodes and build placeholder mapping.

        Args:
            all_nodes: Existing node dictionary (mutated in place)
            input_data_nodes: Dict of port_name -> node from WorkGraph

        Returns:
            Dict mapping data labels to node keys for placeholder substitution
        """
        placeholder_to_node_key: dict[str, str] = {}

        if not input_data_nodes:
            return placeholder_to_node_key

        port_to_label = self._build_port_to_label_mapping()

        for port_name, node in input_data_nodes.items():
            data_label = port_to_label.get(port_name, port_name)
            all_nodes[data_label] = node  # type: ignore[arg-type]
            placeholder_to_node_key[data_label] = data_label  # type: ignore[arg-type]

        return placeholder_to_node_key

    def _resolve_dependencies(
        self, all_nodes: dict[str, aiida.orm.Data], task_folders: dict | None
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Resolve dependency mappings and update nodes.

        Args:
            all_nodes: Node dictionary (mutated in place)
            task_folders: Dict of {task_label: remote_folder_pk}

        Returns:
            Tuple of (placeholder_to_node_key, filenames) mappings
        """
        if not task_folders:
            return {}, {}

        # Convert port_dependency_mapping from dict back to PortDependencyMapping
        port_dep_dict = self.spec.port_dependency_mapping or {}
        port_dependencies = {port: [DependencyInfo(**dep) for dep in deps] for port, deps in port_dep_dict.items()}

        dep_nodes, placeholder_to_node_key, filenames = resolve_shell_dependency_mappings(
            task_folders,
            port_dependencies,
            self.spec.filenames,
        )
        all_nodes.update(dep_nodes)

        return placeholder_to_node_key, filenames

    def _build_metadata(self, task_job_ids: dict | None) -> AiidaMetadata:
        """Build metadata with SLURM job dependencies.

        Args:
            task_job_ids: Dict of {task_label: job_id} for SLURM dependencies

        Returns:
            AiidaMetadata with scheduler dependencies added
        """
        computer = (
            aiida.orm.Computer.collection.get(label=self.spec.metadata.computer_label)
            if self.spec.metadata.computer_label
            else None
        )
        return add_slurm_dependencies_to_metadata(self.spec.metadata, task_job_ids, computer)

    def _build_arguments(self, placeholder_to_node_key: dict[str, str]) -> list[str]:
        """Build final arguments with placeholders substituted.

        Args:
            placeholder_to_node_key: Mapping from data labels to node keys

        Returns:
            List of resolved argument strings
        """
        return AiidaAdapter.substitute_argument_placeholders(self.spec.arguments_template, placeholder_to_node_key)

    def _create_workgraph_task(
        self,
        code: aiida.orm.Code,
        all_nodes: dict[str, aiida.orm.Data],
        filenames: dict[str, str],
        metadata: AiidaMetadata,
        arguments: list[str],
    ) -> Annotated[dict, dynamic()]:
        """Create and return the WorkGraph shell task.

        Args:
            code: AiiDA Code to execute
            all_nodes: All input data nodes
            filenames: Filename mappings
            metadata: Task metadata with dependencies
            arguments: Resolved command arguments

        Returns:
            WorkGraph task outputs
        """
        from aiida_workgraph.tasks.shelljob_task import _build_shelljob_TaskSpec

        # Build the ShellJob TaskSpec
        parser_outputs = [output_info["name"] for output_info in self.spec.output_data_info if output_info["path"]]

        spec = _build_shelljob_TaskSpec(
            identifier=f"shelljob_{self.spec.label}",
            outputs=self.spec.outputs,
            parser_outputs=parser_outputs,
        )

        # Create the shell task
        wg = get_current_graph()

        shell_task = wg.add_task(
            spec,
            name=self.spec.label,
            command=code,
            arguments=arguments,
            nodes=all_nodes,
            outputs=self.spec.outputs,
            filenames=filenames,
            metadata=metadata.model_dump(mode="python", exclude_none=True),
            resolve_command=False,
        )

        return shell_task.outputs

    def execute(
        self,
        input_data_nodes: dict | None = None,
        task_folders: dict | None = None,
        task_job_ids: dict | None = None,
    ) -> Annotated[dict, dynamic()]:
        """Execute the shell task with given inputs and dependencies.

        This is the main orchestration method that coordinates all steps
        of shell task execution in the correct order.

        Args:
            input_data_nodes: Dict mapping port names to AvailableData nodes
            task_folders: Dict of {task_label: remote_folder_pk} from upstream tasks
            task_job_ids: Dict of {task_label: job_id} from upstream tasks

        Returns:
            WorkGraph task outputs
        """
        # Load resources
        code = self._load_code()
        all_nodes = self._build_base_nodes()

        # Process input data nodes
        placeholder_mapping = self._add_input_data_nodes(all_nodes, input_data_nodes)

        # Resolve dependencies
        dep_placeholder_mapping, filenames = self._resolve_dependencies(all_nodes, task_folders)
        placeholder_mapping.update(dep_placeholder_mapping)

        # Build metadata and arguments
        metadata = self._build_metadata(task_job_ids)
        arguments = self._build_arguments(placeholder_mapping)

        # Create and return task
        return self._create_workgraph_task(code, all_nodes, filenames, metadata, arguments)


class IconSpecResolver(SpecResolver):
    """Resolver for ICON tasks at runtime.

    Handles all runtime logic for ICON task execution including:
    - Loading task specification
    - Resolving remote data dependencies
    - Building metadata with SLURM dependencies
    - Creating WorkGraph ICON task
    """

    spec: AiidaIconTaskSpec

    def _load_spec(self, spec_dict: dict) -> AiidaIconTaskSpec:
        """Load ICON task specification.

        Args:
            spec_dict: Serialized AiidaIconTaskSpec

        Returns:
            Validated AiidaIconTaskSpec model
        """
        return AiidaIconTaskSpec(**spec_dict)

    def _resolve_dependencies(self, task_folders: dict | None) -> dict[str, aiida.orm.RemoteData]:
        """Resolve remote data dependencies from parent tasks.

        Args:
            task_folders: Dict of {task_label: remote_folder_pk}

        Returns:
            Dict of resolved RemoteData nodes for dependencies
        """
        if not task_folders:
            return {}

        # Convert port_dependency_mapping from dict back to typed mapping
        port_dep_dict = self.spec.port_dependency_mapping or {}
        port_dependencies = {port: [DependencyInfo(**dep) for dep in deps] for port, deps in port_dep_dict.items()}

        return resolve_icon_dependency_mapping(
            task_folders,
            port_dependencies,
            self.spec.master_namelist_pk,
            self.spec.model_namelist_pks,
        )

    def _build_metadata(self, task_job_ids: dict | None) -> AiidaMetadata:
        """Build metadata with SLURM job dependencies.

        Args:
            task_job_ids: Dict of {task_label: job_id} for SLURM dependencies

        Returns:
            AiidaMetadata with scheduler dependencies added
        """
        computer_label = self.spec.metadata.computer_label if self.spec.metadata else None
        computer = aiida.orm.Computer.collection.get(label=computer_label) if computer_label else None
        return add_slurm_dependencies_to_metadata(self.spec.metadata, task_job_ids, computer, self.spec.label)

    def execute(
        self,
        input_data_nodes: dict | None = None,
        task_folders: dict | None = None,
        task_job_ids: dict | None = None,
    ) -> Annotated[dict, dynamic()]:
        """Execute the ICON task with given inputs and dependencies.

        This is the main orchestration method that coordinates all steps
        of ICON task execution in the correct order.

        Args:
            input_data_nodes: Dict mapping port names to AvailableData nodes
            task_folders: Dict of {task_label: remote_folder_pk} from upstream tasks
            task_job_ids: Dict of {task_label: job_id} from upstream tasks

        Returns:
            WorkGraph ICON task outputs
        """
        # Initialize input data nodes
        if input_data_nodes is None:
            input_data_nodes = {}

        # Resolve dependencies and add to inputs
        remote_data_nodes = self._resolve_dependencies(task_folders)
        input_data_nodes.update(remote_data_nodes)

        # Build metadata with dependencies
        metadata = self._build_metadata(task_job_ids)

        # Build complete inputs dict for IconCalculation
        inputs = build_icon_calcjob_inputs(self.spec, input_data_nodes, metadata)

        # Call IconTask directly (NOT wg.add_task!)
        return task(IconCalculation)(**inputs)


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


async def _poll_for_job_data(workgraph_name: str, task_name: str, interval: int = 10) -> dict:
    """Poll for job data until available.

    Args:
        workgraph_name: Name of the WorkGraph to monitor
        task_name: Name of the task within the WorkGraph
        interval: Polling interval in seconds

    Returns:
        Dict with job_id and remote_folder PK
    """
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine

    while True:
        # Query for WorkGraphs
        builder = orm.QueryBuilder()
        builder.append(
            WorkGraphEngine,
            filters={
                "attributes.process_label": {"==": f"WorkGraph<{workgraph_name}>"},
            },
        )

        if builder.count() == 0:
            await asyncio.sleep(interval)
            continue

        workgraph_node = builder.all()[-1][0]
        node_data = workgraph_node.task_processes.get(task_name)
        if not node_data:
            await asyncio.sleep(interval)
            continue

        node = yaml.load(node_data, Loader=AiiDALoader)  # noqa: S506
        if not node:
            await asyncio.sleep(interval)
            continue

        job_id = node.get_job_id()
        if job_id is None:
            await asyncio.sleep(interval)
            continue

        # SUCCESS — return early
        remote_pk = node.outputs.remote_folder.pk
        return {"job_id": int(job_id), "remote_folder": remote_pk}


@task(outputs=namespace(job_id=int, remote_folder=int))
async def get_job_data(
    workgraph_name: str,
    task_name: str,
    timeout_seconds: int,
    interval: int = 10,
):
    """Monitor CalcJob and return job_id and remote_folder PK when available.

    Args:
        workgraph_name: Name of the WorkGraph to monitor
        task_name: Name of the task within the WorkGraph
        timeout_seconds: Timeout in seconds (based on task walltime + buffer)
        interval: Polling interval in seconds

    Returns:
        Dict with job_id and remote_folder PK

    Raises:
        TimeoutError: If job data is not available within timeout period
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            return await _poll_for_job_data(workgraph_name, task_name, interval)
    except TimeoutError as err:
        msg = f"Timeout waiting for job_id for task {task_name} after {timeout_seconds}s"
        raise TimeoutError(msg) from err


def build_shell_task_spec(task: core.ShellTask) -> AiidaShellTaskSpec:
    """Build all parameters needed to create a shell task.

    Uses ShellTaskSpecBuilder to construct the specification with common
    logic shared across task types.

    NOTE: Job dependencies are NOT included here - they're added at runtime.

    Args:
        task: The ShellTask to build spec for

    Returns:
        AiidaShellTaskSpec containing all shell task parameters
    """
    builder = ShellTaskSpecBuilder(task)
    return builder.build_spec()


def build_icon_task_spec(task: core.IconTask) -> AiidaIconTaskSpec:
    """Build all parameters needed to create an ICON task.

    Uses IconTaskSpecBuilder to construct the specification with common
    logic shared across task types.

    Note: Job dependencies are NOT included here - they're added at runtime.

    Args:
        task: The IconTask to build spec for

    Returns:
        AiidaIconTaskSpec containing all ICON task parameters
    """
    builder = IconTaskSpecBuilder(task)
    return builder.build_spec()


@task.graph()
def build_shell_task_with_dependencies(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    task_folders: Annotated[dict, dynamic(int)] | None = None,
    task_job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic()]:
    """Launch a shell task with optional SLURM job dependencies.

    This function provides a clean interface for WorkGraph by delegating
    all execution logic to ShellSpecResolver.

    Args:
        task_spec: Serialized AiidaShellTaskSpec from WorkGraph
        input_data_nodes: Dict mapping port names to AvailableData nodes
        task_folders: Dict of {task_label: remote_folder_pk} from get_job_data
        task_job_ids: Dict of {task_label: job_id} from get_job_data

    Returns:
        Shell task outputs
    """
    resolver = ShellSpecResolver(task_spec)
    return resolver.execute(input_data_nodes, task_folders, task_job_ids)


@task.graph(outputs=task(IconCalculation).outputs)
def build_icon_task_with_dependencies(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    task_folders: Annotated[dict, dynamic(int)] | None = None,
    task_job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic()]:
    """Launch an ICON task with SLURM dependencies from upstream tasks.

    This function provides a clean interface for WorkGraph by delegating
    all execution logic to IconSpecResolver.

    Args:
        task_spec: Dict from WorkGraph (serialized AiidaIconTaskSpec)
        input_data_nodes: Dict mapping port names to AvailableData nodes
        task_folders: Dict of {task_label: remote_folder_pk} from get_job_data
        task_job_ids: Dict of {task_label: job_id} from get_job_data

    Returns:
        ICON task outputs
    """
    resolver = IconSpecResolver(task_spec)
    return resolver.execute(input_data_nodes, task_folders, task_job_ids)


def _add_task_pair_common(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: AiidaIconTaskSpec | AiidaShellTaskSpec,
    build_task_func,
    input_data_for_task: PortDataMapping,
    dependencies: DependencyMapping,
    task_monitor_outputs: TaskMonitorOutputsMapping,
    dependency_tasks: TaskDependencyMapping,
) -> None:
    """Common logic for adding launcher + monitor task pairs.

    Extracted helper to reduce code duplication between add_icon_task_pair and add_shell_task_pair.

    Args:
        wg: WorkGraph to add tasks to
        wg_name: Parent WorkGraph name (with timestamp)
        task_label: Label for the task
        task_spec: Task specification (ICON or Shell)
        build_task_func: The task builder function (build_icon_task_with_dependencies or build_shell_task_with_dependencies)
        input_data_for_task: Dict of AvailableData inputs
        dependencies: Dependency mapping with parent folders, job IDs, and port dependencies
        task_monitor_outputs: Dict to update with new task outputs (mutated in place)
        dependency_tasks: Dict to update with new dependency task (mutated in place)
    """
    launcher_name = f"{LAUNCHER_PREFIX}{wg_name}_{task_label}"

    # Add port_dependency_mapping to task_spec
    port_dep_dict = {port: [dep.model_dump() for dep in deps] for port, deps in dependencies.port_mapping.items()}
    task_spec_with_deps = task_spec.model_copy(update={"port_dependency_mapping": port_dep_dict})

    # Create launcher task - serialize model to dict for WorkGraph
    wg.add_task(
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
    dep_task: WgTaskProtocol = wg.add_task(  # type: ignore[assignment]
        get_job_data,
        name=f"{MONITOR_PREFIX}{task_label}",
        workgraph_name=launcher_name,
        task_name=task_label,
        timeout_seconds=timeout,
    )

    # Store the outputs namespace for dependent tasks
    task_monitor_outputs[task_label] = dep_task.outputs

    # Chain with previous dependency tasks
    for dep_label in dependencies.task_folders:
        if dep_label in dependency_tasks:
            _ = dependency_tasks[dep_label] >> dep_task

    # Store for next iteration
    dependency_tasks[task_label] = dep_task


def add_icon_task_pair(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: AiidaIconTaskSpec,
    input_data_for_task: PortDataMapping,
    dependencies: DependencyMapping,
    task_monitor_outputs: TaskMonitorOutputsMapping,
    dependency_tasks: TaskDependencyMapping,
) -> None:
    """Add ICON launcher and get_job_data tasks to WorkGraph.

    Mutates both the WorkGraph and the dependency tracking dicts in place.

    Args:
        wg: WorkGraph to add tasks to
        wg_name: Parent WorkGraph name (with timestamp)
        task_label: Label for the task
        task_spec: Task specification dict
        input_data_for_task: Dict of AvailableData inputs
        dependencies: Dependency mapping with parent folders, job IDs, and port dependencies
        task_monitor_outputs: Dict to update with new task outputs (mutated in place)
        dependency_tasks: Dict to update with new dependency task (mutated in place)
    """
    _add_task_pair_common(
        wg,
        wg_name,
        task_label,
        task_spec,
        build_icon_task_with_dependencies,
        input_data_for_task,
        dependencies,
        task_monitor_outputs,
        dependency_tasks,
    )


def add_shell_task_pair(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: AiidaShellTaskSpec,
    input_data_for_task: PortDataMapping,
    dependencies: DependencyMapping,
    task_monitor_outputs: TaskMonitorOutputsMapping,
    dependency_tasks: TaskDependencyMapping,
) -> None:
    """Add Shell launcher and get_job_data tasks to WorkGraph.

    Mutates both the WorkGraph and the dependency tracking dicts in place.

    Args:
        wg: WorkGraph to add tasks to
        wg_name: Parent WorkGraph name (with timestamp)
        task_label: Label for the task
        task_spec: Task specification dict
        input_data_for_task: Dict of AvailableData inputs
        dependencies: Dependency mapping with parent folders, job IDs, and port dependencies
        task_monitor_outputs: Dict to update with new task outputs (mutated in place)
        dependency_tasks: Dict to update with new dependency task (mutated in place)
    """
    _add_task_pair_common(
        wg,
        wg_name,
        task_label,
        task_spec,
        build_shell_task_with_dependencies,
        input_data_for_task,
        dependencies,
        task_monitor_outputs,
        dependency_tasks,
    )
