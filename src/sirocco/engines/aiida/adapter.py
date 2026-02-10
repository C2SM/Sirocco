"""AiiDA adapter - translates core domain to AiiDA representations."""

from __future__ import annotations

from typing import TYPE_CHECKING, assert_never

import aiida.orm
from aiida.common.exceptions import NotExistent
from aiida.transports.plugins.local import LocalTransport

from sirocco import core
from sirocco.engines.aiida.calcjob_builders import SlurmDirectiveBuilder
from sirocco.engines.aiida.models import (
    AiidaMetadata,
    AiidaMetadataOptions,
    AiidaResources,
)
from sirocco.parsing._utils import TimeUtils
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    from sirocco.engines.aiida.types import FileNode


class AiidaAdapter:
    """Collection of utilities for translating core domain to AiiDA representations.

    This class provides static methods for AiiDA-specific transformations,
    keeping the core domain AiiDA-agnostic.
    """

    @staticmethod
    def validate_workflow(workflow: core.Workflow) -> None:
        """Validate that workflow can execute on AiiDA.

        Performs upfront validation of AiiDA-specific requirements before
        attempting to build the WorkGraph. This provides fail-fast behavior
        with clear error messages.

        Checks:
        - All referenced computers exist in AiiDA database (from tasks and data)
        - All task and data labels are valid AiiDA link labels

        Args:
            workflow: The workflow to validate

        Raises:
            ValueError: If workflow cannot run on AiiDA, with details about what's missing
        """
        from aiida.common import validate_link_label

        # Validate task and data labels are AiiDA-compatible
        for task in workflow.tasks:
            try:
                validate_link_label(task.name)
            except ValueError as exception:
                msg = f"Raised error when validating task name '{task.name}': {exception.args[0]}"
                raise ValueError(msg) from exception
            for input_ in task.input_data_nodes():
                try:
                    validate_link_label(input_.name)
                except ValueError as exception:
                    msg = f"Raised error when validating input name '{input_.name}': {exception.args[0]}"
                    raise ValueError(msg) from exception
            for output in task.output_data_nodes():
                try:
                    validate_link_label(output.name)
                except ValueError as exception:
                    msg = f"Raised error when validating output name '{output.name}': {exception.args[0]}"
                    raise ValueError(msg) from exception

        # Collect all computers referenced by tasks
        computers = set()
        for task in workflow.tasks:
            if hasattr(task, "computer") and task.computer:
                computers.add(task.computer)

        # Collect all computers referenced by data
        for data in workflow.data:
            if hasattr(data, "computer") and data.computer:
                computers.add(data.computer)

        # Validate each computer exists
        missing_computers = []
        for computer_label in computers:
            try:
                aiida.orm.load_computer(computer_label)
            except NotExistent:
                missing_computers.append(computer_label)

        if missing_computers:
            msg = (
                f"The following computers referenced in workflow are not found in AiiDA database: "
                f"{', '.join(repr(c) for c in sorted(missing_computers))}. "
                f"Please create them using 'verdi computer setup' or check your workflow configuration."
            )
            raise ValueError(msg)

    @staticmethod
    def sanitize_label(label: str) -> str:
        """Replace characters that are invalid for AiiDA labels.

        AiiDA labels cannot contain: "-", " ", ":", "."
        These are replaced with underscores.
        """
        invalid_chars = ["-", " ", ":", "."]
        for invalid_char in invalid_chars:
            label = label.replace(invalid_char, "_")
        return label

    @staticmethod
    def build_label_from_graph_item(graph_item: core.GraphItem) -> str:
        """Returns a unique AiiDA label for the given graph item.

        The graph item object is uniquely determined by its name and its coordinates.
        """
        return AiidaAdapter.sanitize_label(
            f"{graph_item.name}" + "__".join(f"_{key}_{value}" for key, value in graph_item.coordinates.items())
        )

    @staticmethod
    def create_input_data_node(core_data: core.AvailableData, *, used_by_icon: bool = False) -> FileNode:
        """Create an AiiDA data node from AvailableData.

        Args:
            core_data: The AvailableData to create a node for
            used_by_icon: Whether this data is used by an ICON task (affects node type selection)

        Returns:
            AiiDA data node (RemoteData, SinglefileData, or FolderData)
        """
        label = AiidaAdapter.build_label_from_graph_item(core_data)

        try:
            computer = aiida.orm.load_computer(core_data.computer)
        except NotExistent as err:
            msg = f"Could not find computer {core_data.computer!r} for input {core_data}."
            raise ValueError(msg) from err

        # Check remote path exists
        transport = computer.get_transport()
        with transport:
            if not transport.path_exists(str(core_data.path)):
                msg = f"Could not find available data {core_data.name} in path {core_data.path} on computer {core_data.computer}."
                raise FileNotFoundError(msg)

        # Create the appropriate data node based on usage and transport
        if used_by_icon:
            return aiida.orm.RemoteData(remote_path=str(core_data.path), label=label, computer=computer)
        if computer.get_transport_class() is LocalTransport:
            if core_data.path.is_file():
                return aiida.orm.SinglefileData(file=str(core_data.path), label=label)
            return aiida.orm.FolderData(tree=str(core_data.path), label=label)
        return aiida.orm.RemoteData(remote_path=str(core_data.path), label=label, computer=computer)

    @staticmethod
    def build_scheduler_options(task: core.Task) -> AiidaMetadataOptions:
        """Extract HPC scheduler options from task.

        Args:
            task: The task to extract options from

        Returns:
            AiidaMetadataOptions model
        """
        # Build custom scheduler commands using fluent builder
        builder = SlurmDirectiveBuilder()
        if isinstance(task, (core.IconTask, core.ShellTask)):
            builder.add_uenv(task.uenv).add_view(task.view)

        # Build resources
        resources = None
        if task.nodes is not None or task.ntasks_per_node is not None or task.cpus_per_task is not None:
            resources = AiidaResources(
                num_machines=task.nodes,
                num_mpiprocs_per_machine=task.ntasks_per_node,
                num_cores_per_mpiproc=task.cpus_per_task,
            )

        return AiidaMetadataOptions(
            max_wallclock_seconds=TimeUtils.walltime_to_seconds(task.walltime) if task.walltime else None,
            max_memory_kb=task.mem * 1024 if task.mem else None,
            queue_name=task.partition,
            custom_scheduler_commands=builder.build(),
            resources=resources,
        )

    @staticmethod
    def build_metadata(task: core.Task) -> AiidaMetadata:
        """Build base metadata with scheduler options.

        Args:
            task: The task to build metadata for

        Returns:
            AiidaMetadata model with computer_label and options
        """
        # Get scheduler options
        scheduler_opts = AiidaAdapter.build_scheduler_options(task)

        # Merge with account and retrieve list
        options = scheduler_opts.model_copy(
            update={
                "account": task.account,
                "additional_retrieve_list": [
                    "_scheduler-stdout.txt",
                    "_scheduler-stderr.txt",
                ],
            }
        )

        # Add date cycling prepend text if needed
        if isinstance(task.cycle_point, DateCyclePoint):
            options = AiidaAdapter._add_sirocco_time_prepend_text(options, task)

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        return AiidaMetadata(
            options=options,
            computer_label=computer.label,
        )

    @staticmethod
    def _add_sirocco_time_prepend_text(options: AiidaMetadataOptions, task: core.Task) -> AiidaMetadataOptions:
        """Append chunk start/stop exports to prepend_text when date cycling is available."""
        start_date = task.cycle_point.chunk_start_date.isoformat()  # type: ignore[attr-defined]
        stop_date = task.cycle_point.chunk_stop_date.isoformat()  # type: ignore[attr-defined]

        exports = f"export SIROCCO_START_DATE={start_date}\nexport SIROCCO_STOP_DATE={stop_date}\n"

        current_prepend = options.prepend_text or ""
        new_prepend = f"{current_prepend}\n{exports}" if current_prepend else exports

        return options.model_copy(update={"prepend_text": new_prepend})

    @staticmethod
    def translate_mpi_placeholder(placeholder: core.MpiCmdPlaceholder) -> str:
        """Translate core MPI command to AiiDA format."""
        match placeholder:
            case core.MpiCmdPlaceholder.MPI_TOTAL_PROCS:
                return "tot_num_mpiprocs"
            case _:
                assert_never(placeholder)

    @staticmethod
    def parse_mpi_cmd(mpi_cmd: str) -> str:
        """Parse MPI command and translate placeholders to AiiDA format."""
        for placeholder in core.MpiCmdPlaceholder:
            mpi_cmd = mpi_cmd.replace(
                f"{{{placeholder.value}}}",
                f"{{{AiidaAdapter.translate_mpi_placeholder(placeholder)}}}",
            )
        return mpi_cmd

    @staticmethod
    def substitute_argument_placeholders(arguments_template: str | None, placeholder_to_node_key: dict) -> list[str]:
        """Process argument template and replace placeholders with actual node keys.

        Handles both standalone placeholders like {data_label} and embedded placeholders
        like --pool=data_label where data_label should be replaced with a node key.

        Args:
            arguments_template: Template string with {placeholder} or bare placeholder syntax
            placeholder_to_node_key: Dict mapping placeholder names to node keys

        Returns:
            List of processed arguments with placeholders replaced
        """
        if not arguments_template:
            return []

        arguments_list = arguments_template.split()
        processed_arguments = []

        for arg in arguments_list:
            # Check if this argument is a standalone placeholder like {data_label}
            if arg.startswith("{") and arg.endswith("}"):
                placeholder_name = arg[1:-1]  # Remove the braces
                # Map to the actual node key if we have a mapping
                if placeholder_name in placeholder_to_node_key:
                    actual_node_key = placeholder_to_node_key[placeholder_name]
                    processed_arguments.append(f"{{{actual_node_key}}}")
                else:
                    # Keep original if no mapping found
                    processed_arguments.append(arg)
            else:
                # Check if any data label from placeholder_to_node_key appears in this arg
                # This handles cases like --pool=tmp_data_pool where tmp_data_pool needs replacement
                processed_arg = arg
                for data_label, node_key in placeholder_to_node_key.items():
                    if data_label in arg:
                        processed_arg = arg.replace(data_label, f"{{{node_key}}}")
                        break
                processed_arguments.append(processed_arg)

        return processed_arguments

    @staticmethod
    def get_default_wrapper_script() -> aiida.orm.SinglefileData:
        """Get default wrapper script for ICON tasks.

        Returns:
            AiiDA SinglefileData node containing the default wrapper script
        """
        from aiida_icon.site_support.cscs.alps import SCRIPT_DIR

        # TODO: Using string constants is poor design - should use specific wrapper script classes
        #       with isinstance checks for type-safe selection (e.g., TodiCPU, SantisCPU, GPUWrapper)
        DEFAULT_WRAPPER_SCRIPT = "todi_cpu.sh"
        default_script_path = SCRIPT_DIR / DEFAULT_WRAPPER_SCRIPT
        return aiida.orm.SinglefileData(file=default_script_path)

    @staticmethod
    def get_wrapper_script_data(task) -> aiida.orm.SinglefileData:
        """Get AiiDA SinglefileData for wrapper script.

        Args:
            task: Task with optional wrapper_script attribute

        Returns:
            AiiDA SinglefileData node for the wrapper script
        """
        if task.wrapper_script is not None:
            return aiida.orm.SinglefileData(str(task.wrapper_script))
        return AiidaAdapter.get_default_wrapper_script()
