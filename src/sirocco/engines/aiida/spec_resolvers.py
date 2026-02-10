"""Task specification resolvers - Runtime spec resolution and execution."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Annotated

import aiida.orm
from aiida_icon.calculations import IconCalculation
from aiida_workgraph import dynamic, get_current_graph, task

from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.calcjob_builders import (
    add_slurm_dependencies_to_metadata,
    build_icon_calcjob_inputs,
)
from sirocco.engines.aiida.dependency_resolvers import (
    resolve_icon_dependency_mapping,
    resolve_shell_dependency_mappings,
)
from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    AiidaShellTaskSpec,
    DependencyInfo,
)

LOGGER = logging.getLogger(__name__)


class TaskSpecResolver(ABC):
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
        """Create the WG task with given inputs and dependencies.

        Args:
            input_data_nodes: Dict mapping port names to AvailableData nodes
            task_folders: Dict of {task_label: remote_folder_pk} from upstream tasks
            task_job_ids: Dict of {task_label: job_id} from upstream tasks

        Returns:
            WorkGraph task outputs
        """
        ...


class ShellTaskSpecResolver(TaskSpecResolver):
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

        mappings = resolve_shell_dependency_mappings(
            task_folders,
            port_dependencies,
            self.spec.filenames,
        )
        all_nodes.update(mappings.nodes)

        return mappings.placeholders, mappings.filenames

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
        """Create the shell task with given inputs and dependencies.

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


class IconTaskSpecResolver(TaskSpecResolver):
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
        """Create the ICON WG task with given inputs and dependencies.

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


@task.graph()
def build_shell_task_with_dependencies(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    task_folders: Annotated[dict, dynamic(int)] | None = None,
    task_job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic()]:
    """Build a WG shell task with optional SLURM job dependencies.

    This function provides a clean interface for WorkGraph by delegating
    all execution logic to ShellTaskSpecResolver.

    Args:
        task_spec: Serialized AiidaShellTaskSpec from WorkGraph
        input_data_nodes: Dict mapping port names to AvailableData nodes
        task_folders: Dict of {task_label: remote_folder_pk} from get_job_data
        task_job_ids: Dict of {task_label: job_id} from get_job_data

    Returns:
        Shell task outputs
    """
    resolver = ShellTaskSpecResolver(task_spec)
    return resolver.execute(input_data_nodes, task_folders, task_job_ids)


@task.graph(outputs=task(IconCalculation).outputs)
def build_icon_task_with_dependencies(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    task_folders: Annotated[dict, dynamic(int)] | None = None,
    task_job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic()]:
    """Build an ICON WG task with SLURM dependencies from upstream tasks.

    This function provides a clean interface for WorkGraph by delegating
    all execution logic to IconTaskSpecResolver.

    Args:
        task_spec: Dict from WorkGraph (serialized AiidaIconTaskSpec)
        input_data_nodes: Dict mapping port names to AvailableData nodes
        task_folders: Dict of {task_label: remote_folder_pk} from get_job_data
        task_job_ids: Dict of {task_label: job_id} from get_job_data

    Returns:
        ICON task outputs
    """
    resolver = IconTaskSpecResolver(task_spec)
    return resolver.execute(input_data_nodes, task_folders, task_job_ids)
