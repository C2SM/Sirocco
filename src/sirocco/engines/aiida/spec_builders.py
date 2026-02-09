"""Task specification builders - Pre-runtime spec construction."""

from __future__ import annotations

import hashlib
import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path

import aiida.orm
from aiida.common.exceptions import NotExistent
from aiida_icon.iconutils.namelists import create_namelist_singlefiledata_from_content
from aiida_shell.parsers.shell import ShellParser

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.code_factory import CodeFactory
from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaShellTaskSpec,
    InputDataInfo,
    OutputDataInfo,
)
from sirocco.engines.aiida.utils import serialize_coordinates, split_cmd_arg

LOGGER = logging.getLogger(__name__)


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
