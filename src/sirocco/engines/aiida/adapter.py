"""AiiDA adapter - translates core domain to AiiDA representations."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, assert_never

import aiida.common
import aiida.orm
import aiida.transports
from aiida.common.exceptions import NotExistent

from sirocco import core
from sirocco.engines.aiida.utils import (
    replace_invalid_chars_in_label,
    serialize_coordinates,
    split_cmd_arg,
)
from sirocco.parsing._utils import TimeUtils
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    type AiiDAFileNode = aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData


class AiiDAAdapter:
    """Adapts Sirocco core domain objects to AiiDA representations.

    This class isolates all AiiDA-specific transformations, keeping the
    core domain AiiDA-agnostic.
    """

    def __init__(self, core_workflow: core.Workflow):
        self.core_workflow = core_workflow

    @staticmethod
    def get_graph_item_label(graph_item: core.GraphItem) -> str:
        """Returns a unique AiiDA label for the given graph item.

        The graph item object is uniquely determined by its name and its coordinates.
        """
        return replace_invalid_chars_in_label(
            f"{graph_item.name}" + "__".join(f"_{key}_{value}" for key, value in graph_item.coordinates.items())
        )

    def create_input_data_node(self, core_data: core.AvailableData) -> AiiDAFileNode:
        """Create an AiiDA data node from AvailableData.

        Args:
            data: The AvailableData to create a node for

        Returns:
            AiiDA data node (RemoteData, SinglefileData, or FolderData)
        """
        label = self.get_graph_item_label(core_data)

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

        # Check if this data will be used by ICON tasks
        used_by_icon_task = any(
            isinstance(task, core.IconTask) and core_data in task.input_data_nodes() for task in self.core_workflow.tasks
        )

        if used_by_icon_task:
            # ICON tasks always require RemoteData, at least that's our assumption
            return aiida.orm.RemoteData(remote_path=str(core_data.path), label=label, computer=computer)
        if computer.get_transport_class() is aiida.transports.plugins.local.LocalTransport:
            if core_data.path.is_file():
                return aiida.orm.SinglefileData(file=str(core_data.path), label=label)
            return aiida.orm.FolderData(tree=str(core_data.path), label=label)
        return aiida.orm.RemoteData(remote_path=str(core_data.path), label=label, computer=computer)

    # TODO: This needs to be generalized to cover all use cases
    @staticmethod
    def create_shell_code(task: core.ShellTask, computer: aiida.orm.Computer) -> aiida.orm.Code:
        """Create or load an AiiDA Code for a shell task.

        Determines whether to create PortableCode or InstalledCode based on where the
        executable/script actually exists:
        - If file exists locally (absolute or relative path) -> PortableCode (upload)
        - If file exists remotely (absolute path only) -> InstalledCode (reference)
        - If just executable name (no path separators) -> InstalledCode (assume in PATH)

        Args:
            task: The ShellTask to create code for
            computer: The AiiDA computer

        Returns:
            The AiiDA Code object
        """
        from aiida_shell import ShellCode

        # Determine the executable path to use
        if task.path is not None:
            executable_path = str(task.path)
        else:
            executable_path, _ = split_cmd_arg(task.command)

        path_obj = Path(executable_path)

        # Check if this is a path (contains separators) or just an executable name
        is_path = "/" in executable_path or executable_path.startswith("./")

        if not is_path:
            # Just an executable name (e.g., "python", "bash") -> InstalledCode
            code_label = executable_path

            try:
                code = aiida.orm.load_code(f"{code_label}@{computer.label}")
            except NotExistent:
                code = ShellCode(  # type: ignore[assignment]
                    label=code_label,
                    computer=computer,
                    filepath_executable=executable_path,
                    default_calc_job_plugin="core.shell",
                    use_double_quotes=True,
                )
                _ = code.store()

            return code

        # It's a path - check if it exists locally
        if not path_obj.is_absolute():
            path_obj = path_obj.resolve()

        exists_locally = path_obj.exists() and path_obj.is_file()

        if exists_locally:
            # File exists locally -> PortableCode
            script_name = path_obj.name
            script_dir = path_obj.parent

            base_label = script_name
            # FIXME: hardcoded for bash and python currently
            if base_label.endswith((".sh", ".py")):
                base_label = base_label[:-3]

            path_hash = hashlib.sha256(str(path_obj).encode()).hexdigest()[:8]

            with open(path_obj, "rb") as f:
                content_hash = hashlib.sha256(f.read()).hexdigest()[:8]

            code_label = f"{base_label}-{path_hash}-{content_hash}"

            try:
                code = aiida.orm.load_code(f"{code_label}@{computer.label}")
            except NotExistent:
                code = aiida.orm.PortableCode(
                    label=code_label,
                    description=f"Shell script: {path_obj}",
                    computer=computer,
                    filepath_executable=script_name,
                    filepath_files=str(script_dir),
                    default_calc_job_plugin="core.shell",
                )
                _ = code.store()

            return code

        # File doesn't exist locally - check remotely
        if not Path(executable_path).is_absolute():
            msg = (
                f"File not found locally at {path_obj}, and relative paths are not "
                f"supported for remote files. Use an absolute path for remote files."
            )
            raise FileNotFoundError(msg)

        # Check remote file existence
        user = aiida.orm.User.collection.get_default()
        if user is None:
            msg = "No default AiiDA user available."
            raise RuntimeError(msg)

        authinfo = computer.get_authinfo(user)
        with authinfo.get_transport() as transport:
            if not transport.isfile(executable_path):
                msg = (
                    f"File not found locally or remotely: {executable_path}\n"
                    f"Local path checked: {path_obj}\n"
                    f"Remote path checked: {executable_path} on {computer.label}"
                )
                raise FileNotFoundError(msg)

        # File exists remotely -> InstalledCode
        script_name = Path(executable_path).name
        base_label = script_name
        # FIXME: hardcoded for bash and python currently
        if base_label.endswith((".sh", ".py")):
            base_label = base_label[:-3]

        path_hash = hashlib.sha256(executable_path.encode()).hexdigest()[:8]
        code_label = f"{base_label}-{path_hash}"

        try:
            code = aiida.orm.load_code(f"{code_label}@{computer.label}")
        except NotExistent:
            code = ShellCode(  # type: ignore[assignment]
                label=code_label,
                description=f"Shell script: {executable_path}",
                computer=computer,
                filepath_executable=executable_path,
                default_calc_job_plugin="core.shell",
                use_double_quotes=True,
            )
            _ = code.store()

        return code

    @staticmethod
    def get_scheduler_options(task: core.Task) -> dict[str, Any]:
        """Extract HPC scheduler options from task.

        Args:
            task: The task to extract options from

        Returns:
            Dict of scheduler options
        """
        options: dict[str, Any] = {}
        if task.walltime is not None:
            options["max_wallclock_seconds"] = TimeUtils.walltime_to_seconds(task.walltime)
        if task.mem is not None:
            options["max_memory_kb"] = task.mem * 1024
        if task.partition is not None:
            options["queue_name"] = task.partition

        # custom_scheduler_commands - initialize if not already set
        if "custom_scheduler_commands" not in options:
            options["custom_scheduler_commands"] = ""

        # Support uenv and view for both IconTask and ShellTask
        if isinstance(task, (core.IconTask, core.ShellTask)):
            if task.uenv is not None:
                if options["custom_scheduler_commands"]:
                    options["custom_scheduler_commands"] += "\n"
                options["custom_scheduler_commands"] += f"#SBATCH --uenv={task.uenv}"
            if task.view is not None:
                if options["custom_scheduler_commands"]:
                    options["custom_scheduler_commands"] += "\n"
                options["custom_scheduler_commands"] += f"#SBATCH --view={task.view}"

        if task.nodes is not None or task.ntasks_per_node is not None or task.cpus_per_task is not None:
            resources = {}
            if task.nodes is not None:
                resources["num_machines"] = task.nodes
            if task.ntasks_per_node is not None:
                resources["num_mpiprocs_per_machine"] = task.ntasks_per_node
            if task.cpus_per_task is not None:
                resources["num_cores_per_mpiproc"] = task.cpus_per_task
            options["resources"] = resources
        return options

    def build_metadata(self, task: core.Task) -> dict:
        """Build base metadata dict with scheduler options.

        Args:
            task: The task to build metadata for

        Returns:
            Metadata dict with computer_label and options
        """
        metadata: dict[str, Any] = {}
        metadata["options"] = {}
        metadata["options"]["account"] = task.account
        # FIXME: Should this also be added for IconCalculation? Only for ShellJob, no?
        # TODO: This can most likely be fully removed
        metadata["options"]["additional_retrieve_list"] = [
            "_scheduler-stdout.txt",
            "_scheduler-stderr.txt",
        ]
        metadata["options"].update(self.get_scheduler_options(task))
        self._add_sirocco_time_prepend_text(metadata, task)

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
            metadata["computer_label"] = computer.label
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        return metadata

    @staticmethod
    def _add_sirocco_time_prepend_text(metadata: dict, task: core.Task) -> None:
        """Append chunk start/stop exports to prepend_text when date cycling is available."""
        if not isinstance(task.cycle_point, DateCyclePoint):
            return

        start_date = task.cycle_point.chunk_start_date.isoformat()
        stop_date = task.cycle_point.chunk_stop_date.isoformat()

        exports = f"export SIROCCO_START_DATE={start_date}\nexport SIROCCO_STOP_DATE={stop_date}\n"

        current_prepend = metadata["options"].get("prepend_text", "")
        if current_prepend:
            metadata["options"]["prepend_text"] = f"{current_prepend}\n{exports}"
        else:
            metadata["options"]["prepend_text"] = exports

    @staticmethod
    def translate_mpi_placeholder_static(placeholder: core.MpiCmdPlaceholder) -> str:
        """Translate core MPI command to AiiDA format."""
        match placeholder:
            case core.MpiCmdPlaceholder.MPI_TOTAL_PROCS:
                return "tot_num_mpiprocs"
            case _:
                assert_never(placeholder)
