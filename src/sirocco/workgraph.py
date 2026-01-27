from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Self, assert_never
from zoneinfo import ZoneInfo

import aiida.common
import aiida.orm
import aiida.transports
import yaml
from aiida.common.exceptions import NotExistent
from aiida.orm.utils.serialize import AiiDALoader
from aiida_icon.calculations import IconCalculation
from aiida_icon.iconutils.namelists import create_namelist_singlefiledata_from_content
from aiida_shell.parsers.shell import ShellParser
from aiida_workgraph import WorkGraph, dynamic, get_current_graph, namespace, task

from sirocco import core
from sirocco.parsing._utils import TimeUtils
from sirocco.parsing.cycling import DateCyclePoint

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    type WorkgraphDataNode = (
        aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData
    )


# =============================================================================
# Data Structures
# =============================================================================


@dataclass(frozen=True)
class DependencyInfo:
    """Information about a task dependency.

    Attributes:
        dep_label: Label of the task that produces this dependency
        filename: Optional filename within the remote folder (None = use whole folder)
        data_label: Label of the data item being consumed
    """

    dep_label: str
    filename: str | None
    data_label: str

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "dep_label": self.dep_label,
            "filename": self.filename,
            "data_label": self.data_label,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        """Create from dict."""
        return cls(
            dep_label=data["dep_label"],
            filename=data["filename"],
            data_label=data["data_label"],
        )


# TODO: See if those are actually required, or I can just add serialization to the core classes
# Considering that core objects are more complex, I'd say we keep the classes here as DTOs
@dataclass(frozen=True)
class BaseDataInfo:
    name: str
    coordinates: str
    label: str
    path: str


@dataclass(frozen=True)
class InputDataInfo(BaseDataInfo):
    port: str
    is_available: bool

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "port": self.port,
            "name": self.name,
            "coordinates": self.coordinates,
            "label": self.label,
            "is_available": self.is_available,
            "path": self.path,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        """Create from dict."""
        return cls(
            port=data["port"],
            name=data["name"],
            coordinates=data["coordinates"],
            label=data["label"],
            is_available=data["is_available"],
            path=data["path"],
        )


@dataclass(frozen=True)
class OutputDataInfo(BaseDataInfo):
    port: str | None  # FIXME: Make ports required also for outputs
    # is_generated: bool  # should always be true

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "name": self.name,
            "coordinates": self.coordinates,
            "label": self.label,
            "path": self.path,
            "port": self.port,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        """Create from dict."""
        return cls(
            name=data["name"],
            coordinates=data["coordinates"],
            label=data["label"],
            path=data["path"],
            port=data["port"],
        )


# Type Aliases for Complex Mappings
type PortToDependencies = dict[str, list[DependencyInfo]]
type ParentFolders = dict[str, Any]  # {dep_label: TaggedValue with .value = int PK}
type JobIds = dict[str, Any]  # {dep_label: TaggedValue with .value = int job_id}
type TaskDepInfo = dict[
    str, Any
]  # {task_label: namespace with .remote_folder, .job_id}
type LauncherDependencies = dict[
    str, list[str]
]  # {launcher_name: [parent_launcher_names]}


# =============================================================================
# Helper Functions - Dataclass Serialization
# =============================================================================


def _port_to_dependencies_to_dict(
    port_to_dep: PortToDependencies,
) -> dict[str, list[dict]]:
    """Convert PortToDependencies to JSON-serializable dict.

    Args:
        port_to_dep: PortToDependencies mapping

    Returns:
        Dict with list of dict values
    """
    return {port: [dep.to_dict() for dep in deps] for port, deps in port_to_dep.items()}


def _port_to_dependencies_from_dict(data: dict[str, list[dict]]) -> PortToDependencies:
    """Convert dict to PortToDependencies.

    Args:
        data: Dict representation

    Returns:
        PortToDependencies mapping
    """
    return {
        port: [DependencyInfo.from_dict(dep) for dep in deps]
        for port, deps in data.items()
    }


# =============================================================================
# Helper Functions - Mapping Utilities
# =============================================================================


def _map_list_append(mapping: dict[str, list], key: str, value: Any) -> None:
    """Append value to list at key, creating list if needed.

    Args:
        mapping: Dictionary to update
        key: Key to append to
        value: Value to append to the list
    """
    mapping.setdefault(key, []).append(value)


def _map_unique_set(mapping: dict[str, Any], key: str, value: Any) -> bool:
    """Set value for key only if not already present.

    Args:
        mapping: Dictionary to update
        key: Key to set
        value: Value to set

    Returns:
        True if value was set, False if key already existed
    """
    if key not in mapping:
        mapping[key] = value
        return True
    return False


# =============================================================================
# Utility Functions
# =============================================================================


def replace_invalid_chars_in_label(label: str) -> str:
    """Replaces chars in the label that are invalid for AiiDA.

    The invalid chars ["-", " ", ":", "."] are replaced with underscores.
    """
    invalid_chars = ["-", " ", ":", "."]
    for invalid_char in invalid_chars:
        label = label.replace(invalid_char, "_")
    return label


def validate_workflow_aiida_link_labels(core_workflow: core.Workflow):
    """Checks if the core workflow uses valid AiiDA names for its tasks and data."""
    for core_task in core_workflow.tasks:
        try:
            aiida.common.validate_link_label(core_task.name)
        except ValueError as exception:
            msg = f"Raised error when validating task name '{core_task.name}': {exception.args[0]}"
            raise ValueError(msg) from exception
        for input_ in core_task.input_data_nodes():
            try:
                aiida.common.validate_link_label(input_.name)
            except ValueError as exception:
                msg = f"Raised error when validating input name '{input_.name}': {exception.args[0]}"
                raise ValueError(msg) from exception
        for output in core_task.output_data_nodes():
            try:
                aiida.common.validate_link_label(output.name)
            except ValueError as exception:
                msg = f"Raised error when validating output name '{output.name}': {exception.args[0]}"
                raise ValueError(msg) from exception


# TODO(MERGE): Align all different possible patterns here
def split_cmd_arg(command_line: str, script_name: str | None = None) -> tuple[str, str]:
    """Split command line into command and arguments.

    If script_name is provided, finds the script in the command line and
    returns everything after it as arguments. This handles various patterns:
    - "script.sh arg1 arg2" → args: "arg1 arg2"
    - "bash script.sh arg1 arg2" → args: "arg1 arg2"
    - "uenv run /path/to/env -- script.sh arg1" → args: "arg1"

    Args:
        command_line: Full command line string
        script_name: Script name to find and split on

    Returns:
        Tuple of (command_prefix, arguments)
    """
    if script_name:
        # Get just the basename for matching
        script_basename = Path(script_name).name

        parts = command_line.split()
        for i, part in enumerate(parts):
            # Check if this part ends with or equals the script basename
            part_basename = Path(part).name
            if part_basename == script_basename:
                # Everything after the script name is arguments
                args = " ".join(parts[i + 1 :])
                cmd = " ".join(parts[: i + 1])
                return cmd, args

    # Fallback: simple split on first space
    split = command_line.split(sep=" ", maxsplit=1)
    if len(split) == 1:
        return command_line, ""
    return split[0], split[1]


def translate_mpi_cmd_placeholder(placeholder: core.MpiCmdPlaceholder) -> str:
    """Translate MPI command placeholder to AiiDA format."""
    match placeholder:
        case core.MpiCmdPlaceholder.MPI_TOTAL_PROCS:
            return "tot_num_mpiprocs"
        case _:
            assert_never(placeholder)


def get_aiida_label_from_graph_item(obj: core.GraphItem) -> str:
    """Returns a unique AiiDA label for the given graph item.

    The graph item object is uniquely determined by its name and its coordinates. There is the possibility that
    through the replacement of invalid chars in the coordinates duplication can happen but it is unlikely.
    """
    return replace_invalid_chars_in_label(
        f"{obj.name}"
        + "__".join(f"_{key}_{value}" for key, value in obj.coordinates.items())
    )


def label_placeholder(data: core.Data) -> str:
    """Create a placeholder string for data."""
    return f"{{{get_aiida_label_from_graph_item(data)}}}"


# TODO(MERGE): Align between standalone and aiida-icon
def get_default_wrapper_script() -> aiida.orm.SinglefileData | None:
    """Get default wrapper script based on task type"""
    # Import the script directory from aiida-icon
    from aiida_icon.site_support.cscs.alps import SCRIPT_DIR

    # TODO: There's also `santis_cpu.sh`. Also gpu available.
    # This should be configurable by the users
    default_script_path = SCRIPT_DIR / "todi_cpu.sh"
    return aiida.orm.SinglefileData(file=default_script_path)


def get_wrapper_script_aiida_data(task) -> aiida.orm.SinglefileData | None:
    """Get AiiDA SinglefileData for wrapper script if configured"""
    if task.wrapper_script is not None:
        return aiida.orm.SinglefileData(str(task.wrapper_script))
    return get_default_wrapper_script()


def parse_mpi_cmd_to_aiida(mpi_cmd: str) -> str:
    """Parse MPI command and translate placeholders to AiiDA format."""
    for placeholder in core.MpiCmdPlaceholder:
        mpi_cmd = mpi_cmd.replace(
            f"{{{placeholder.value}}}",
            f"{{{translate_mpi_cmd_placeholder(placeholder)}}}",
        )
    return mpi_cmd


def serialize_coordinates(coordinates: dict) -> dict:
    """Convert coordinates dict to JSON-serializable format.

    Converts datetime objects to ISO format strings.
    """
    serialized = {}
    for key, value in coordinates.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized


# =============================================================================
# ICON Task Helper Functions
# =============================================================================


# TODO: Adopt this to work with multiple restart files / multi-model setups
def resolve_icon_restart_file(
    workdir_path: str,
    model_namelist_node: aiida.orm.SinglefileData,
    workdir_remote_data: aiida.orm.RemoteData,
) -> aiida.orm.RemoteData:
    """Resolve ICON restart file path using aiida-icon utilities.

    Args:
        workdir_path: Path to remote working directory
        model_namelist_node: AiiDA node containing the model namelist
        workdir_remote_data: RemoteData for the workdir (fallback)

    Returns:
        RemoteData pointing to the restart file (or workdir if resolution fails)
    """
    import f90nml
    from aiida_icon.iconutils.modelnml import read_latest_restart_file_link_name

    try:
        # Read and parse the namelist content
        with model_namelist_node.open(mode="r") as f:
            nml_content = f.read()

        nml = f90nml.reads(nml_content)

        # Use aiida-icon function to get the restart file link name
        restart_link_name = read_latest_restart_file_link_name(nml)
        specific_file_path = f"{workdir_path}/{restart_link_name}"

        file_remote_data = aiida.orm.RemoteData(
            computer=workdir_remote_data.computer,
            remote_path=specific_file_path,
        )
    except Exception as e:  # noqa: BLE001
        LOGGER.warning(e)
        return workdir_remote_data
    else:
        return file_remote_data

# TODO: Check if those can be merged: `resolve_icon_restart_file` and `resolve_icon_restart_file`
def _resolve_icon_dependency(
    dep_info: DependencyInfo,
    workdir_remote: aiida.orm.RemoteData,
    model_namelist_pks: dict,
) -> aiida.orm.RemoteData:
    """Resolve a single ICON dependency to RemoteData.

    Args:
        dep_info: Dependency information
        workdir_remote: RemoteData for the producer's working directory
        model_namelist_pks: Dict of model namelist PKs for restart resolution

    Returns:
        RemoteData pointing to the specific file or workdir
    """
    workdir_path = workdir_remote.get_remote_path()

    # Case 1: filename known → point directly to file
    if dep_info.filename:
        specific_path = f"{workdir_path}/{dep_info.filename}"
        return aiida.orm.RemoteData(
            computer=workdir_remote.computer,
            remote_path=specific_path,
        )

    # Case 2: No filename → try resolve via model namelist
    # TODO: Should this point directly to `atm` / `atmo`?
    # FIXME: Align with Rico's PR on aiida-icon to resolve this from the namelist
    model_pk = model_namelist_pks.get("atm")
    if model_pk:
        model_node: aiida.orm.SinglefileData = aiida.orm.load_node(model_pk)  # type: ignore
        return resolve_icon_restart_file(
            workdir_path,
            model_node,
            workdir_remote,  # type: ignore
        )

    return workdir_remote


def load_icon_dependencies(
    parent_folders: ParentFolders | None,
    port_to_dep_mapping: PortToDependencies,
    model_namelist_pks: dict,
) -> dict[str, aiida.orm.RemoteData]:
    """Load RemoteData dependencies for an ICON task and map to input ports."""

    input_nodes: dict[str, aiida.orm.RemoteData] = {}
    if not parent_folders:
        return input_nodes

    # ------------------------------------------------------------------
    # Load RemoteData for each parent folder (remote_folder pk)
    # ------------------------------------------------------------------
    parent_folders_loaded: dict[str, aiida.orm.RemoteData] = {}
    for dep_label, tagged_val in parent_folders.items():
        node = aiida.orm.load_node(tagged_val.value)
        if not isinstance(node, aiida.orm.RemoteData):
            msg = f"Expected RemoteData for {dep_label} but got {type(node)}"
            raise TypeError(msg)
        parent_folders_loaded[dep_label] = node

    # ------------------------------------------------------------------
    # Process each port → list of dependencies
    # For ICON, each port has at most 1 dependency
    # ------------------------------------------------------------------
    for port_name, dep_list in port_to_dep_mapping.items():
        if not dep_list:
            continue

        # NOTE: Investigate further
        dep_info = dep_list[0]  # ICON: max 1 dependency per port

        workdir_remote = parent_folders_loaded.get(dep_info.dep_label)
        if not workdir_remote:
            continue

        # Use helper to resolve dependency
        input_nodes[port_name] = _resolve_icon_dependency(
            dep_info,
            workdir_remote,
            model_namelist_pks,
        )

    return input_nodes


def _build_slurm_dependency_directive(job_ids: JobIds) -> str:
    """Build SLURM --dependency directive from job IDs.

    Args:
        job_ids: Dict of {dep_label: job_id_tagged_value}

    Returns:
        SLURM directive string like "#SBATCH --dependency=afterok:123:456"
    """
    dep_str = ":".join(str(jid.value) for jid in job_ids.values())
    # NOTE: `--kill-on-invalid-dep=yes` added so that downstream depndent tasks get killed if a task fails
    return f"#SBATCH --dependency=afterok:{dep_str} --kill-on-invalid-dep=yes"


def _add_custom_scheduler_command(metadata: dict, command: str) -> None:
    """Add a custom scheduler command to metadata options (modifies in place).

    Args:
        metadata: Metadata dict with "options" key
        command: Command string to add
    """
    current_cmds = metadata["options"].get("custom_scheduler_commands", "")
    if current_cmds:
        metadata["options"]["custom_scheduler_commands"] = f"{current_cmds}\n{command}"
    else:
        metadata["options"]["custom_scheduler_commands"] = command


def build_icon_metadata_with_slurm_dependencies(
    base_metadata: dict,
    job_ids: JobIds | None,
    computer: aiida.orm.Computer,
    label: str,
) -> dict:
    """Build ICON metadata dict with SLURM job dependencies.

    Args:
        base_metadata: Base metadata from task spec
        job_ids: Dict of {dep_label: job_id_tagged_value} or None
        computer: AiiDA computer object
        label: Task label for debug output

    Returns:
        Metadata dict for ICON task with computer and SLURM dependencies
    """
    metadata = dict(base_metadata)
    metadata["options"] = dict(metadata["options"])

    if job_ids:
        custom_cmd = _build_slurm_dependency_directive(job_ids)
        _add_custom_scheduler_command(metadata, custom_cmd)

    return {
        "computer": computer,
        "options": metadata["options"],
        "call_link_label": label,
    }


def prepare_icon_task_inputs(
    task_spec: dict,
    input_data_nodes: dict,
    metadata_dict: dict,
) -> dict:
    """Prepare complete inputs dict for ICON task.

    Args:
        task_spec: Task specification containing code, namelists, wrapper script PKs
        input_data_nodes: Dict of input data nodes (both AvailableData and RemoteData)
        metadata_dict: Metadata dict with computer and options

    Returns:
        Complete inputs dict for IconTask
    """
    from aiida.engine.processes.ports import PortNamespace
    from aiida_icon.calculations import IconCalculation

    # Get IconCalculation spec to determine which ports are namespaces
    icon_spec = IconCalculation.spec()

    # Start with code and namelists
    inputs: dict[str, Any] = {
        "code": aiida.orm.load_node(task_spec["code_pk"]),
        "master_namelist": aiida.orm.load_node(task_spec["master_namelist_pk"]),
    }

    # Add model namelists as a dict (namespace input)
    models = {}
    for model_name, model_pk in task_spec["model_namelist_pks"].items():
        models[model_name] = aiida.orm.load_node(model_pk)
    if models:
        inputs["models"] = models  # type: ignore[assignment]

    # Add wrapper script if present
    if task_spec["wrapper_script_pk"] is not None:
        inputs["wrapper_script"] = aiida.orm.load_node(task_spec["wrapper_script_pk"])  # type: ignore[assignment]

    # Add ALL input data nodes (both AvailableData and RemoteData for GeneratedData)
    for port_name, data_node in input_data_nodes.items():
        # Check if this port is a namespace by inspecting the spec
        is_namespace = False
        if port_name in icon_spec.inputs:
            port = icon_spec.inputs[port_name]
            is_namespace = isinstance(port, PortNamespace)

        # Wrap namespace ports in a dict with node label as key
        if is_namespace:
            # Use the node's label or a generic key for the namespace
            node_label = data_node.label if data_node.label else "item"
            inputs[port_name] = {node_label: data_node}
        else:
            inputs[port_name] = data_node

    # Add metadata
    inputs["metadata"] = metadata_dict  # type: ignore[assignment]

    LOGGER.debug(f'ICON {inputs=}')

    return inputs


# =============================================================================
# Task Spec Building
# =============================================================================


def add_aiida_input_data_node(
    data: core.AvailableData, core_workflow: core.Workflow, aiida_data_nodes: dict
) -> None:
    """Create an `aiida.orm.Data` instance from the provided available data.

    Args:
        data: The AvailableData to create a node for
        core_workflow: The workflow (to check if data is used by ICON tasks)
        aiida_data_nodes: Dict to add the created node to
    """
    label = get_aiida_label_from_graph_item(data)

    try:
        computer = aiida.orm.load_computer(data.computer)
    except NotExistent as err:
        msg = f"Could not find computer {data.computer!r} for input {data}."
        raise ValueError(msg) from err

    # `remote_path` must be str not PosixPath to be JSON-serializable
    transport = computer.get_transport()
    with transport:
        if not transport.path_exists(str(data.path)):
            msg = f"Could not find available data {data.name} in path {data.path} on computer {data.computer}."
            raise FileNotFoundError(msg)

    # Check if this data will be used by ICON tasks
    used_by_icon_task = any(
        isinstance(task, core.IconTask) and data in task.input_data_nodes()
        for task in core_workflow.tasks
    )

    if used_by_icon_task:
        # ICON tasks __always__ require RemoteData
        aiida_data_nodes[label] = aiida.orm.RemoteData(
            remote_path=str(data.path), label=label, computer=computer
        )
    elif (
        computer.get_transport_class() is aiida.transports.plugins.local.LocalTransport
    ):
        if data.path.is_file():
            aiida_data_nodes[label] = aiida.orm.SinglefileData(
                file=str(data.path), label=label
            )
        else:
            aiida_data_nodes[label] = aiida.orm.FolderData(
                tree=str(data.path), label=label
            )
    else:
        aiida_data_nodes[label] = aiida.orm.RemoteData(
            remote_path=str(data.path), label=label, computer=computer
        )


def get_scheduler_options_from_task(task: core.Task) -> dict[str, Any]:
    """Extract scheduler options from a task.

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
        # AiiDA uses 'queue_name', which maps to SLURM '--partition'
        options["queue_name"] = task.partition

    # custom_scheduler_commands - initialize if not already set
    if "custom_scheduler_commands" not in options:
        options["custom_scheduler_commands"] = ""

    # Support uenv and view for both IconTask and ShellTask
    # TODO: Possibly drop this and instead provide via `command`
    if isinstance(task, (core.IconTask, core.ShellTask)) and task.uenv is not None:
        if options["custom_scheduler_commands"]:
            options["custom_scheduler_commands"] += "\n"
        options["custom_scheduler_commands"] += f"#SBATCH --uenv={task.uenv}"
    if isinstance(task, (core.IconTask, core.ShellTask)) and task.view is not None:
        if options["custom_scheduler_commands"]:
            options["custom_scheduler_commands"] += "\n"
        options["custom_scheduler_commands"] += f"#SBATCH --view={task.view}"

    if (
        task.nodes is not None
        or task.ntasks_per_node is not None
        or task.cpus_per_task is not None
    ):
        resources = {}
        if task.nodes is not None:
            resources["num_machines"] = task.nodes
        if task.ntasks_per_node is not None:
            resources["num_mpiprocs_per_machine"] = task.ntasks_per_node
        if task.cpus_per_task is not None:
            resources["num_cores_per_mpiproc"] = task.cpus_per_task
        options["resources"] = resources
    return options


# TODO(MERGE): Check this logic again if it supports all use cases
def create_shell_code(
    task: core.ShellTask, computer: aiida.orm.Computer
) -> aiida.orm.Code:
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
        executable_path = str(task.path)  # Convert Path to string
    else:
        executable_path, _ = split_cmd_arg(task.command)

    # breakpoint()

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
            ).store()

        return code

    # It's a path - check if it exists locally
    if not path_obj.is_absolute():
        # Relative path - resolve to absolute for local check
        path_obj = path_obj.resolve()

    exists_locally = path_obj.exists() and path_obj.is_file()

    if exists_locally:
        # File exists locally -> PortableCode
        script_name = path_obj.name
        script_dir = path_obj.parent

        # Create unique code label from script name + hash of absolute path + hash of content
        base_label = script_name
        # TODO: More elegant way to strip file extension
        if base_label.endswith((".sh", ".py")):
            base_label = base_label[:-3]

        # Add hash of absolute path for uniqueness
        path_hash = hashlib.sha256(str(path_obj).encode()).hexdigest()[:8]

        # Add hash of file content to detect changes
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
                filepath_executable=script_name,  # Filename within the directory
                filepath_files=str(script_dir),  # Directory containing the script
                default_calc_job_plugin="core.shell",
            )
            code.store()

        return code

    # File doesn't exist locally - check if it exists remotely (only for absolute paths)
    if not Path(executable_path).is_absolute():
        msg = (
            f"File not found locally at {path_obj}, and relative paths are not "
            f"supported for remote files. Use an absolute path for remote files."
        )
        raise FileNotFoundError(msg)

    # Check remote file existence using transport
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

    # Create unique code label from script name + hash of remote path
    base_label = script_name
    # TODO: More elegant way to strip file extension
    if base_label.endswith((".sh", ".py")):
        base_label = base_label[:-3]

    # Add hash of absolute remote path for uniqueness
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
        ).store()

    return code


def build_base_metadata(task: core.Task) -> dict:
    """Build base metadata dict for any task type (without job dependencies).

    Job dependencies will be added at runtime in the @task.graph functions.

    Args:
        task: The task to build metadata for

    Returns:
        Metadata dict with computer_label and options
    """
    metadata: dict[str, Any] = {}
    metadata["options"] = {}
    metadata["options"]["account"] = task.account
    metadata["options"]["additional_retrieve_list"] = [
        "_scheduler-stdout.txt",
        "_scheduler-stderr.txt",
    ]
    metadata["options"].update(get_scheduler_options_from_task(task))
    _add_chunk_time_prepend_text(metadata, task)

    try:
        computer = aiida.orm.Computer.collection.get(label=task.computer)
        metadata["computer_label"] = computer.label
    except NotExistent as err:
        msg = f"Could not find computer {task.computer!r} in AiiDA database."
        raise ValueError(msg) from err

    return metadata


def _add_chunk_time_prepend_text(metadata: dict, task: core.Task) -> None:
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


def build_shell_task_spec(task: core.ShellTask) -> dict:
    """Build all parameters needed to create a shell task.

    Returns a dict with keys: label, code, nodes, metadata,
    arguments_template, filenames, outputs, input_data_info, output_data_info

    NOTE: Job dependencies are NOT included here - they're added at runtime.

    Args:
        task: The ShellTask to build spec for

    Returns:
        Dict containing all shell task parameters
    """
    label = get_aiida_label_from_graph_item(task)

    # Get computer
    try:
        computer = aiida.orm.Computer.collection.get(label=task.computer)
    except NotExistent as err:
        msg = f"Could not find computer {task.computer!r} in AiiDA database."
        raise ValueError(msg) from err

    # Build base metadata (no job dependencies yet)
    metadata = build_base_metadata(task)

    # Add shell-specific metadata options
    metadata["options"]["use_symlinks"] = True

    # Create or load code
    code = create_shell_code(task, computer)

    # Pre-compute input data information using dataclasses
    input_data_info: list[InputDataInfo] = []
    for port_name, input_ in task.input_data_items():
        input_info = InputDataInfo(
            port=port_name,
            name=input_.name,
            coordinates=serialize_coordinates(input_.coordinates),
            label=get_aiida_label_from_graph_item(input_),
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
    # breakpoint()
    output_data_info: list[OutputDataInfo] = []
    for port_name, output in task.output_data_items():  # type: ignore[assignment]
        output_info = OutputDataInfo(
            name=output.name,
            coordinates=serialize_coordinates(output.coordinates),
            label=get_aiida_label_from_graph_item(output),
            path=str(output.path) if output.path is not None else "",  # type: ignore[attr-defined]
            port=port_name,
        )
        output_data_info.append(output_info)

    # TODO:  fix this
    #     ipdb> input_labels
    # {'data_pool': ['/capstor/scratch/cscs/jgeiger/DYAMOND_input'], 'sst_ice_dir': ['/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_data/R02B06/sst_and_seaice/r0001'], 'ozone_dir': ['/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_data/R02B06/ozone/r0001'], 'aero_kine_dir': ['/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_data/R02B06/aerosol_kinne/r0001'], 'icon_input': []}
    # ipdb> pp input_labels
    # {'aero_kine_dir': ['/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_data/R02B06/aerosol_kinne/r0001'],
    #  'data_pool': ['/capstor/scratch/cscs/jgeiger/DYAMOND_input'],
    #  'icon_input': [],
    #  'ozone_dir': ['/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_data/R02B06/ozone/r0001'],
    #  'sst_ice_dir': ['/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_data/R02B06/sst_and_seaice/r0001']}
    # ipdb> pp output_labels
    # {'icon_input': ['icon_input']}
    output_labels: dict[str, list[str]] = {}
    for output_info in output_data_info:
        port_name = output_info.port  # type: ignore[assignment]
        output_label = output_info.label
        if port_name not in output_labels:
            output_labels[port_name] = []
        # For AvailableData with a path, use the actual path directly in command arguments
        # instead of creating a placeholder, since these are pre-existing files/directories
        if output_info.path:
            output_labels[port_name].append(output_info.path)
        else:
            output_labels[port_name].append(f"{{{output_label}}}")

    # breakpoint()

    # Pre-scan command template to find all referenced ports
    # This ensures optional/missing ports are included with empty lists
    for port_match in task.port_pattern.finditer(task.command):
        port_name = port_match.group(2)
        if port_name and port_name not in input_labels:
            input_labels[port_name] = []

    # Pre-resolve arguments template
    # Get script name from task.path for proper command splitting
    script_name = Path(task.path).name if task.path else None

    # breakpoint()
    # FIXME: This merging works for now, but see how else to properly handle the previously empty `--icon_input`
    input_labels.update(output_labels)
    arguments_with_placeholders = task.resolve_ports(input_labels)  # type: ignore[arg-type]

    _, resolved_arguments_template = split_cmd_arg(
        arguments_with_placeholders, script_name
    )

    # Build filenames mapping
    filenames = {}
    for input_info in input_data_info:
        input_label = input_info.label
        if input_info.is_available:
            filenames[input_info.name] = (
                Path(input_info.path).name if input_info.path else input_info.name
            )  # type: ignore[arg-type]
        else:
            # Count how many inputs have the same name
            same_name_count = sum(
                1 for info in input_data_info if info.name == input_info.name
            )
            if same_name_count > 1:
                filenames[input_label] = input_label
            else:
                filenames[input_label] = (
                    Path(input_info.path).name if input_info.path else input_info.name
                )  # type: ignore[arg-type]

    # Build outputs list - but DON'T retrieve, just verify existence
    # Set retrieve_temporary_list instead of outputs so files stay on remote
    # NOTE: For now, keep outputs empty to avoid retrieval
    outputs = []  # type: ignore[var-annotated]

    # Build output port mapping: data_name -> shell output link_label

    output_port_mapping = {}
    for output_info in output_data_info:
        if output_info.path:
            link_label = ShellParser.format_link_label(output_info.path)  # type: ignore[arg-type]
            output_port_mapping[output_info.name] = link_label

    # breakpoint()

    return {
        "label": label,
        "code_pk": code.pk,
        "node_pks": {},
        "metadata": metadata,
        "arguments_template": resolved_arguments_template,
        "filenames": filenames,
        "outputs": outputs,
        "input_data_info": [info.to_dict() for info in input_data_info],
        "output_data_info": [info.to_dict() for info in output_data_info],
        "output_port_mapping": output_port_mapping,
    }


def build_icon_task_spec(task: core.IconTask) -> dict:
    """Build all parameters needed to create an ICON task.

    Returns a dict with keys: label, builder, output_ports

    Note: Job dependencies are NOT included here - they're added at runtime.

    Args:
        task: The IconTask to build spec for

    Returns:
        Dict containing all ICON task parameters
    """

    task_label = get_aiida_label_from_graph_item(task)

    try:
        computer = aiida.orm.Computer.collection.get(label=task.computer)
    except NotExistent as err:
        msg = f"Could not find computer {task.computer!r} in AiiDA database."
        raise ValueError(msg) from err

    # Create or load ICON code with unique label based on executable path
    # Different ICON executables (CPU/GPU, versions) should have different code objects
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
            with_mpi=True,  # ICON is always an MPI application
            use_double_quotes=True,
        )
        icon_code.store()

    # Build base metadata (no job dependencies yet)
    metadata = build_base_metadata(task)

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
            model_node = create_namelist_singlefiledata_from_content(
                content, model_nml.name, store=True
            )
            model_namelist_pks[model_name] = model_node.pk

    # Wrapper script - store as PK if present
    wrapper_script_pk = None
    wrapper_script_data = get_wrapper_script_aiida_data(task)
    if wrapper_script_data is not None:
        wrapper_script_data.store()
        wrapper_script_pk = wrapper_script_data.pk

    # Pre-compute output port mapping: data_name -> icon_port_name
    # task.outputs is dict[port_name, list[Data]]
    # We need to map each Data.name to its ICON port name
    output_port_mapping = {}
    for port_name, output_list in task.outputs.items():
        # For each data item from this port, map data.name -> port_name
        for data in output_list:
            output_port_mapping[data.name] = port_name

    return {
        "label": task_label,
        "code_pk": icon_code.pk,
        "master_namelist_pk": master_namelist_node.pk,
        "model_namelist_pks": model_namelist_pks,
        "wrapper_script_pk": wrapper_script_pk,
        "metadata": metadata,
        "output_port_mapping": output_port_mapping,
    }


# =============================================================================
# Shell Task Helper Functions
# =============================================================================


def _create_shell_remote_data(
    dep_info: DependencyInfo,
    workdir_remote: aiida.orm.RemoteData,
) -> tuple[str, aiida.orm.RemoteData]:
    """Create RemoteData for a shell dependency.

    Args:
        dep_info: Dependency information
        workdir_remote: RemoteData for the producer's working directory

    Returns:
        Tuple of (unique_key, remote_data_node)
    """
    import os

    workdir_path = workdir_remote.get_remote_path()
    unique_key = f"{dep_info.dep_label}_remote"

    if dep_info.filename:
        # Create RemoteData pointing to the specific file/directory
        # Normalize path to remove trailing slashes
        specific_file_path = os.path.normpath(f"{workdir_path}/{dep_info.filename}")
        remote_data = aiida.orm.RemoteData(
            computer=workdir_remote.computer,
            remote_path=specific_file_path,
        )
    else:
        # No specific filename, use the workdir itself
        remote_data = workdir_remote  # type: ignore[assignment]

    return unique_key, remote_data


def load_and_process_shell_dependencies(
    parent_folders: ParentFolders,
    port_to_dep_mapping: PortToDependencies,
    original_filenames: dict,
) -> tuple[dict, dict, dict]:
    """Load RemoteData dependencies and build node/placeholder/filename mappings.

    Args:
        parent_folders: Dict of {dep_label: remote_folder_pk_tagged_value}
        port_to_dep_mapping: Dict mapping port names to list of DependencyInfo objects
        original_filenames: Dict mapping data labels to filenames

    Returns:
        Tuple of (all_nodes, placeholder_to_node_key, filenames) dicts
    """
    all_nodes: dict[str, aiida.orm.RemoteData] = {}
    placeholder_to_node_key: dict[str, str] = {}
    filenames: dict[str, str] = {}

    # Load RemoteData nodes from their PKs
    parent_folders_loaded: dict[str, Any] = {
        key: aiida.orm.load_node(val.value) for key, val in parent_folders.items()
    }

    # Process ALL dependencies: create nodes, map placeholders, and map filenames
    for dep_info_list in port_to_dep_mapping.values():
        # dep_info_list is a list of DependencyInfo objects
        for dep_info in dep_info_list:
            if dep_info.dep_label not in parent_folders_loaded:
                continue

            workdir_remote_data = parent_folders_loaded[dep_info.dep_label]

            # Use helper to create RemoteData
            unique_key, remote_data = _create_shell_remote_data(
                dep_info, workdir_remote_data
            )
            all_nodes[unique_key] = remote_data

            # Build placeholder mapping for arguments
            placeholder_to_node_key[dep_info.data_label] = unique_key

            # Build filename mapping (from original_filenames via data_label)
            if dep_info.data_label in original_filenames:
                filenames[unique_key] = original_filenames[dep_info.data_label]

    return all_nodes, placeholder_to_node_key, filenames


def build_shell_metadata_with_slurm_dependencies(
    base_metadata: dict, job_ids: JobIds | None, computer: aiida.orm.Computer
) -> dict:
    """Build metadata dict with SLURM job dependencies added.

    Args:
        base_metadata: Base metadata from task spec (should contain 'computer_label')
        job_ids: Dict of {dep_label: job_id_tagged_value} or None
        computer: AiiDA computer object

    Returns:
        Metadata dict with computer and optional SLURM dependencies (computer_label removed)
    """
    metadata = dict(base_metadata)
    metadata["options"] = dict(metadata["options"])

    # Remove computer_label and set computer object
    metadata.pop("computer_label", None)
    metadata["computer"] = computer

    if job_ids:
        custom_cmd = _build_slurm_dependency_directive(job_ids)
        _add_custom_scheduler_command(metadata, custom_cmd)
        base_metadata.get("label", "unknown")

    LOGGER.debug(f"{metadata=}")

    return metadata


def process_shell_argument_placeholders(
    arguments_template: str | None, placeholder_to_node_key: dict
) -> list[str]:
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


# =============================================================================
# WorkGraph Builder Helper Functions
# =============================================================================


def collect_available_data_inputs(
    task: core.Task, aiida_data_nodes: dict, get_label_func
) -> dict:
    """Collect AvailableData input nodes for a task.

    Args:
        task: The task to collect inputs for
        aiida_data_nodes: Dict mapping data labels to AiiDA data nodes
        get_label_func: Function to get label from graph item

    Returns:
        Dict mapping port names to AiiDA data nodes
    """
    input_data_for_task = {}
    for port, input_data in task.input_data_items():
        input_label = get_label_func(input_data)
        if isinstance(input_data, core.AvailableData):
            input_data_for_task[port] = aiida_data_nodes[input_label]

    return input_data_for_task


def build_dependency_mapping(
    task: core.Task,
    core_workflow: core.Workflow,
    task_dep_info: TaskDepInfo,
    get_label_func,
) -> tuple[PortToDependencies, ParentFolders, JobIds]:
    """Build dependency mapping for GeneratedData inputs."""

    port_to_dep: PortToDependencies = {}
    parent_folders: ParentFolders = {}
    job_ids: JobIds = {}

    # ---------------------------------------------------------------------
    # Precompute: data_label → (producer_task_label, out_data)
    # ---------------------------------------------------------------------
    producers: dict[str, tuple[str, core.GeneratedData]] = {}

    for prev_task in core_workflow.tasks:
        prev_label = get_label_func(prev_task)

        for _, out_data in prev_task.output_data_items():
            out_label = get_label_func(out_data)
            producers[out_label] = (prev_label, out_data)

    # ---------------------------------------------------------------------
    # Process inputs for the current task
    # ---------------------------------------------------------------------
    for port, input_data in task.input_data_items():
        if not isinstance(input_data, core.GeneratedData):
            continue

        input_label = get_label_func(input_data)

        # Find the producer (if exists)
        producer_info = producers.get(input_label)
        if not producer_info:
            continue  # No producer found

        prev_label, out_data = producer_info

        # Extract filename/path if GeneratedData
        filename = out_data.path.name if getattr(out_data, "path", None) else None

        # Only record dependencies if this producer has completed metadata
        if prev_label not in task_dep_info:
            continue

        # -----------------------------------------------------------------
        # Add to port dependency mapping
        # -----------------------------------------------------------------
        _map_list_append(
            port_to_dep,
            port,
            DependencyInfo(
                dep_label=prev_label, filename=filename, data_label=input_label
            ),
        )

        # -----------------------------------------------------------------
        # Add parent folder + job_id for producer (only once)
        # -----------------------------------------------------------------
        if _map_unique_set(parent_folders, prev_label, None):
            job_data = task_dep_info[prev_label]
            parent_folders[prev_label] = job_data.remote_folder
            job_ids[prev_label] = job_data.job_id

    return port_to_dep, parent_folders, job_ids


def create_icon_launcher_task(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: dict,
    input_data_for_task: dict,
    parent_folders_for_task: dict,
    job_ids_for_task: dict,
    port_to_dep_mapping: dict,
    task_dep_info: dict,
    prev_dep_tasks: dict,
) -> tuple[dict, dict]:
    """Create ICON launcher and get_job_data tasks.

    Args:
        wg: WorkGraph to add tasks to
        wg_name: Parent WorkGraph name (with timestamp)
        task_label: Label for the task
        task_spec: Task specification dict
        input_data_for_task: Dict of AvailableData inputs
        parent_folders_for_task: Dict of parent folder PKs
        job_ids_for_task: Dict of job IDs
        port_to_dep_mapping: Port to dependency mapping
        task_dep_info: Dict to update with new task outputs
        prev_dep_tasks: Dict to update with new dependency task

    Returns:
        Updated (task_dep_info, prev_dep_tasks) dicts
    """
    launcher_name = f"launch_{wg_name}_{task_label}"

    # Add port_to_dep_mapping to task_spec (convert to dict for JSON serialization)
    task_spec["port_to_dep_mapping"] = _port_to_dependencies_to_dict(
        port_to_dep_mapping
    )

    # Create launcher task
    wg.add_task(
        launch_icon_task_with_dependency,
        name=launcher_name,
        task_spec=task_spec,
        input_data_nodes=input_data_for_task if input_data_for_task else None,
        parent_folders=parent_folders_for_task if parent_folders_for_task else None,
        job_ids=job_ids_for_task if job_ids_for_task else None,
    )

    # Create get_job_data task
    # breakpoint()
    dep_task = wg.add_task(
        get_job_data,
        name=f"get_job_data_{task_label}",
        workgraph_name=launcher_name,
        task_name=task_label,
        timeout=3600,  # Explicitly set timeout to ensure it persists
    )

    # Store the outputs namespace for dependent tasks
    task_dep_info[task_label] = dep_task.outputs

    # Chain with previous dependency tasks using >>
    for dep_label in parent_folders_for_task:
        if dep_label in prev_dep_tasks:
            prev_dep_tasks[dep_label] >> dep_task

    # Store for next iteration
    prev_dep_tasks[task_label] = dep_task

    return task_dep_info, prev_dep_tasks


def create_shell_launcher_task(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: dict,
    input_data_for_task: dict,
    parent_folders_for_task: dict,
    job_ids_for_task: dict,
    port_to_dep_mapping: dict,
    task_dep_info: dict,
    prev_dep_tasks: dict,
) -> tuple[dict, dict]:
    """Create Shell launcher and get_job_data tasks.

    Args:
        wg: WorkGraph to add tasks to
        wg_name: Parent WorkGraph name (with timestamp)
        task_label: Label for the task
        task_spec: Task specification dict
        input_data_for_task: Dict of AvailableData inputs
        parent_folders_for_task: Dict of parent folder PKs
        job_ids_for_task: Dict of job IDs
        port_to_dep_mapping: Port to dependency mapping
        task_dep_info: Dict to update with new task outputs
        prev_dep_tasks: Dict to update with new dependency task

    Returns:
        Updated (task_dep_info, prev_dep_tasks) dicts
    """
    launcher_name = f"launch_{wg_name}_{task_label}"

    # Add port_to_dep_mapping to task_spec (convert to dict for JSON serialization)
    task_spec["port_to_dep_mapping"] = _port_to_dependencies_to_dict(
        port_to_dep_mapping
    )

    # Create launcher task
    wg.add_task(
        launch_shell_task_with_dependency,
        name=launcher_name,
        task_spec=task_spec,
        input_data_nodes=input_data_for_task if input_data_for_task else None,
        parent_folders=parent_folders_for_task if parent_folders_for_task else None,
        job_ids=job_ids_for_task if job_ids_for_task else None,
    )

    # Create get_job_data task
    dep_task = wg.add_task(
        get_job_data,
        name=f"get_job_data_{task_label}",
        workgraph_name=launcher_name,
        task_name=task_label,
        timeout=3600,  # Explicitly set timeout to ensure it persists
    )

    # Store the outputs namespace for dependent tasks
    task_dep_info[task_label] = dep_task.outputs

    # Chain with previous dependency tasks
    for dep_label in parent_folders_for_task:
        if dep_label in prev_dep_tasks:
            prev_dep_tasks[dep_label] >> dep_task

    # Store for next iteration
    prev_dep_tasks[task_label] = dep_task

    return task_dep_info, prev_dep_tasks


# =============================================================================
# Workflow Functions
# =============================================================================


# TODO: Consider using asyncio.timeout instead of timeout parameter
@task(outputs=namespace(job_id=int, remote_folder=int))
async def get_job_data(
    workgraph_name: str,
    task_name: str,
    interval: int = 10,
    timeout: int = 3600,  # noqa: ASYNC109
):
    """Monitor CalcJob and return job_id and remote_folder PK when available."""
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine

    start = time.time()

    while True:
        # Timeout check early
        if time.time() - start > timeout:
            msg = f"Timeout waiting for job_id for task {task_name} after {timeout}s"
            raise TimeoutError(msg)

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


@task.graph
def launch_shell_task_with_dependency(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    parent_folders: Annotated[dict, dynamic(int)] | None = None,
    job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic()]:
    """Launch a shell task with optional SLURM job dependencies."""
    from aiida_workgraph.tasks.shelljob_task import _build_shelljob_TaskSpec

    # Get pre-computed data
    label = task_spec["label"]
    output_data_info = task_spec["output_data_info"]

    # Load the code from PK
    code = aiida.orm.load_node(task_spec["code_pk"])

    # Load nodes from PKs and initialize structures
    all_nodes = {
        key: aiida.orm.load_node(pk) for key, pk in task_spec["node_pks"].items()
    }

    # Add AvailableData nodes passed as parameter, remapping from port names to data labels
    # so they match the placeholders in arguments (which use data labels)
    placeholder_to_node_key: dict[str, str] = {}
    if input_data_nodes:
        # Build mapping from port names to data labels for AvailableData
        port_to_label = {}
        for input_info in task_spec["input_data_info"]:
            if input_info["is_available"]:
                port_to_label[input_info["port"]] = input_info["label"]

        # Add nodes with data labels as keys (not port names)
        # Also build placeholder mapping for AvailableData
        for port_name, node in input_data_nodes.items():
            data_label = port_to_label.get(port_name, port_name)
            all_nodes[data_label] = node
            # Map data label to node key for placeholder replacement
            placeholder_to_node_key[data_label] = data_label

    # Process dependencies if present
    filenames: dict[str, str] = {}
    if parent_folders:
        # Convert port_to_dep_mapping from dict back to PortToDependencies
        port_to_dep_dict = task_spec.get("port_to_dep_mapping", {})
        port_to_dep = _port_to_dependencies_from_dict(port_to_dep_dict)

        dep_nodes, placeholder_to_node_key, filenames = (
            load_and_process_shell_dependencies(
                parent_folders,
                port_to_dep,
                task_spec["filenames"],
            )
        )
        all_nodes.update(dep_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(
        label=task_spec["metadata"]["computer_label"]
    )
    metadata = build_shell_metadata_with_slurm_dependencies(
        task_spec["metadata"], job_ids, computer
    )

    # Process argument placeholders
    arguments = process_shell_argument_placeholders(
        task_spec["arguments_template"], placeholder_to_node_key
    )

    # Use pre-computed outputs
    outputs = task_spec["outputs"]

    # Build the shelljob TaskSpec
    parser_outputs = [
        output_info["name"] for output_info in output_data_info if output_info["path"]
    ]

    spec = _build_shelljob_TaskSpec(
        identifier=f"shelljob_{label}",
        outputs=outputs,
        parser_outputs=parser_outputs,
    )

    # Create the shell task
    wg = get_current_graph()

    shell_task = wg.add_task(
        spec,
        name=label,
        command=code,
        arguments=arguments,
        nodes=all_nodes,
        outputs=outputs,
        filenames=filenames,
        metadata=metadata,
        resolve_command=False,
    )

    # Return outputs directly (WorkGraph will wrap them)
    return shell_task.outputs


IconTask = task(IconCalculation)


@task.graph(outputs=IconTask.outputs)
def launch_icon_task_with_dependency(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    parent_folders: Annotated[dict, dynamic(int)] | None = None,
    job_ids: Annotated[dict, dynamic(int)] | None = None,
):
    """Launch an ICON task with SLURM dependencies from upstream tasks.

    Following Xing's approach exactly: accept parent_folders as PKs and job_ids as ints,
    load RemoteData from PKs inside this function.
    This avoids socket dependencies while preserving provenance.

    Args:
        task_spec: Dict from _build_icon_task_spec() containing task specifications
        input_data_nodes: Dict mapping port names to AvailableData nodes
        parent_folders: Dict of {dep_label: remote_folder_pk} from get_job_data
        job_ids: Dict of {dep_label: job_id} from get_job_data

    Returns:
        IconTask outputs
    """
    label = task_spec["label"]
    computer_label = task_spec["metadata"]["computer_label"]

    # Handle None inputs
    if input_data_nodes is None:
        input_data_nodes = {}

    # Load RemoteData dependencies (restart files, etc.)
    # Convert port_to_dep_mapping from dict back to PortToDependencies
    port_to_dep_dict = task_spec.get("port_to_dep_mapping", {})
    port_to_dep = _port_to_dependencies_from_dict(port_to_dep_dict)

    remote_data_nodes = load_icon_dependencies(
        parent_folders,
        port_to_dep,
        task_spec["model_namelist_pks"],
    )
    input_data_nodes.update(remote_data_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(label=computer_label)
    metadata_dict = build_icon_metadata_with_slurm_dependencies(
        task_spec["metadata"], job_ids, computer, label
    )

    # Prepare complete inputs dict for IconTask
    inputs = prepare_icon_task_inputs(task_spec, input_data_nodes, metadata_dict)

    # Call IconTask directly (NOT wg.add_task!)
    # This returns a TaskNode with proper output sockets
    return IconTask(**inputs)


def get_task_dependencies_from_workgraph(wg: WorkGraph) -> dict[str, list[str]]:
    """Extract dependency graph from WorkGraph."""
    deps: dict[str, list[str]] = {}

    # Precompute: get_job_data_X → corresponding launcher task
    # Launcher names: launch_{wg_name}_{task_label}
    # get_job_data names: get_job_data_{task_label}
    # Find actual launcher tasks and map by matching task_label suffix
    launcher_tasks = {t.name: t for t in wg.tasks if t.name.startswith("launch_")}
    get_job_data_to_launcher = {}
    for t in wg.tasks:
        if t.name.startswith("get_job_data_"):
            task_label = t.name.replace("get_job_data_", "")
            # Find the launcher that ends with this task_label
            for launcher_name in launcher_tasks:
                if launcher_name.endswith(f"_{task_label}"):
                    get_job_data_to_launcher[t.name] = launcher_name
                    break

    # Iterate only over launcher tasks
    # Launcher names start with "launch_"
    for task_ in wg.tasks:
        name = task_.name
        if not name.startswith("launch_"):
            continue

        launcher_deps: list[str] = []
        deps[name] = launcher_deps

        sockets = getattr(task_.inputs, "_sockets", None)
        if not sockets:
            continue

        # Iterate over input links → parent tasks
        for socket in sockets.values():
            for link in getattr(socket, "links", []):
                parent_name = link.from_socket.node.name

                # Only care about get_job_data_* parents
                if not parent_name.startswith("get_job_data_"):
                    continue

                parent_launcher = get_job_data_to_launcher.get(parent_name)
                if parent_launcher and parent_launcher not in launcher_deps:
                    launcher_deps.append(parent_launcher)

    return deps


def compute_topological_levels(task_deps: dict[str, list[str]]) -> dict[str, int]:
    """Compute topological level for each task using BFS.

    Level 0 = no dependencies
    Level k = max(parent levels) + 1

    Args:
        task_deps: Dict mapping task_name -> list of parent task names

    Returns:
        Dict mapping task_name -> topological level
    """
    from collections import deque

    levels = {}
    in_degree = {task_name: len(parents) for task_name, parents in task_deps.items()}

    # Find all tasks with no dependencies (level 0)
    queue = deque([task_name for task_name, degree in in_degree.items() if degree == 0])
    for task_name in queue:
        levels[task_name] = 0

    # Build reverse dependency graph: task -> list of tasks that depend on it
    children: dict[str, list[str]] = {task_name: [] for task_name in task_deps}
    for task_name, parents in task_deps.items():
        for parent in parents:
            if parent not in children:
                children[parent] = []
            children[parent].append(task_name)

    # Process tasks in topological order
    processed = set()
    while queue:
        current = queue.popleft()
        processed.add(current)

        # Update children's levels
        for child in children.get(current, []):
            parents = task_deps[child]
            # Check if all parents have been processed
            if all(p in processed for p in parents):
                # Level is max of all parent levels + 1
                parent_levels = [levels[p] for p in parents]
                levels[child] = max(parent_levels) + 1
                queue.append(child)

    return levels


def build_dynamic_sirocco_workgraph(
    core_workflow: core.Workflow,
    aiida_data_nodes: dict,
    shell_task_specs: dict,
    icon_task_specs: dict,
):
    from aiida_workgraph.manager import set_current_graph

    # Add timestamp to make workgraph name unique per run
    base_name = core_workflow.name or "SIROCCO_WF"
    timestamp = datetime.now(ZoneInfo("Europe/Zurich")).strftime("%Y_%m_%d_%H_%M")
    wg_name = f"{base_name}_{timestamp}"
    wg = WorkGraph(wg_name)
    set_current_graph(wg)

    # Store get_job_data task outputs (namespace with job_id, remote_folder)
    task_dep_info: dict[str, Any] = {}
    prev_dep_tasks: dict[str, Any] = {}

    # Track launcher task dependencies for rolling window
    # Maps launch_task_name -> list of parent launch_task_names
    launcher_dependencies: dict[str, list[str]] = {}

    # Helper to get task label
    def get_label(task):
        return get_aiida_label_from_graph_item(task)

    # Process all tasks in the workflow in cycle order
    for cycle in core_workflow.cycles:
        for core_task in cycle.tasks:
            task_label = get_label(core_task)

            # Collect AvailableData inputs
            input_data_for_task = collect_available_data_inputs(
                core_task, aiida_data_nodes, get_label
            )

            # Build dependency mapping for GeneratedData inputs
            port_to_dep_mapping, parent_folders_for_task, job_ids_for_task = (
                build_dependency_mapping(
                    core_task, core_workflow, task_dep_info, get_label
                )
            )

            # Track dependencies for rolling window
            # parent_folders_for_task keys are the task labels this task depends on
            launcher_name = f"launch_{wg_name}_{task_label}"
            launcher_dependencies[launcher_name] = [
                f"launch_{wg_name}_{dep_label}" for dep_label in parent_folders_for_task
            ]

            # Create launcher task based on task type
            if isinstance(core_task, core.IconTask):
                task_spec = icon_task_specs[task_label]
                task_dep_info, prev_dep_tasks = create_icon_launcher_task(
                    wg,
                    wg_name,
                    task_label,
                    task_spec,
                    input_data_for_task,
                    parent_folders_for_task,
                    job_ids_for_task,
                    port_to_dep_mapping,
                    task_dep_info,
                    prev_dep_tasks,
                )

            elif isinstance(core_task, core.ShellTask):
                task_spec = shell_task_specs[task_label]
                task_dep_info, prev_dep_tasks = create_shell_launcher_task(
                    wg,
                    wg_name,
                    task_label,
                    task_spec,
                    input_data_for_task,
                    parent_folders_for_task,
                    job_ids_for_task,
                    port_to_dep_mapping,
                    task_dep_info,
                    prev_dep_tasks,
                )

            else:
                msg = f"Unknown task type: {type(core_task)}"
                raise TypeError(msg)

    return wg, launcher_dependencies


# =============================================================================
# Public API - Main Entry Point Functions
# =============================================================================


def build_sirocco_workgraph(
    core_workflow: core.Workflow,
    front_depth: int = 1,
) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows functionally.

    Args:
        core_workflow: The core workflow to convert
        front_depth: Number of topological fronts to keep active (default: 1)
                    0 = sequential (wait for level N to finish before submitting N+1)
                    1 = one front ahead (default)
                    high value = streaming submission

    Returns:
        A WorkGraph ready for submission

    Example::

        from sirocco import core
        from sirocco.workgraph import build_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Build the WorkGraph with front_depth=2
        wg = build_sirocco_workgraph(wf, front_depth=2)

        # Submit to AiiDA daemon
        wg.submit()
    """
    # Validate workflow
    validate_workflow_aiida_link_labels(core_workflow)

    # Create available data nodes
    aiida_data_nodes: dict[str, WorkgraphDataNode] = {}
    for data in core_workflow.data:
        if isinstance(data, core.AvailableData):
            add_aiida_input_data_node(data, core_workflow, aiida_data_nodes)

    # Build task specs
    shell_task_specs = {}
    icon_task_specs = {}
    for task_ in core_workflow.tasks:
        label = get_aiida_label_from_graph_item(task_)
        if isinstance(task_, core.ShellTask):
            shell_task_specs[label] = build_shell_task_spec(task_)
        elif isinstance(task_, core.IconTask):
            icon_task_specs[label] = build_icon_task_spec(task_)

    # Build the dynamic workgraph
    wg, launcher_dependencies = build_dynamic_sirocco_workgraph(
        core_workflow=core_workflow,
        aiida_data_nodes=aiida_data_nodes,
        shell_task_specs=shell_task_specs,
        icon_task_specs=icon_task_specs,
    )

    # Store window configuration in WorkGraph extras
    # This is now properly serialized/deserialized by aiida-workgraph
    # (requires the extras serialization changes in workgraph.py)
    # Levels will be computed dynamically at runtime by TaskManager
    window_config = {
        "enabled": front_depth
        >= 0,  # Window must be enabled to restrict submission (0 = sequential, 1+ = lookahead)
        "front_depth": front_depth,
        "task_dependencies": launcher_dependencies,  # Dependency graph for dynamic level computation
    }

    wg.extras = {"window_config": window_config}

    return wg


def submit_sirocco_workgraph(
    core_workflow: core.Workflow,
    *,
    inputs: None | dict[str, Any] = None,
    wait: bool = False,
    timeout: int = 60,
    metadata: None | dict[str, Any] = None,
) -> aiida.orm.Node:
    """Build and submit a Sirocco workflow to the AiiDA daemon.

    Args:
        core_workflow: The core workflow to convert and submit
        inputs: Optional inputs to pass to the workgraph
        wait: Whether to wait for completion
        timeout: Timeout in seconds if wait=True
        metadata: Optional metadata for the workgraph

    Returns:
        The AiiDA process node

    Raises:
        RuntimeError: If submission fails

    Example::

        from sirocco import core
        from sirocco.workgraph import submit_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Submit the workflow
        node = submit_sirocco_workgraph(wf)
        print(f"Submitted as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.submit(inputs=inputs, wait=wait, timeout=timeout, metadata=metadata)

    if (output_node := wg.process) is None:
        msg = "Something went wrong when submitting workgraph. Please contact a developer."
        raise RuntimeError(msg)

    return output_node


def run_sirocco_workgraph(
    core_workflow: core.Workflow,
    inputs: None | dict[str, Any] = None,
    metadata: None | dict[str, Any] = None,
) -> aiida.orm.Node:
    """Build and run a Sirocco workflow in a blocking fashion.

    Args:
        core_workflow: The core workflow to convert and run
        inputs: Optional inputs to pass to the workgraph
        metadata: Optional metadata for the workgraph

    Returns:
        The AiiDA process node

    Raises:
        RuntimeError: If execution fails

    Example::

        from sirocco import core
        from sirocco.workgraph import run_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Run the workflow (blocking)
        node = run_sirocco_workgraph(wf)
        print(f"Completed as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.run(inputs=inputs, metadata=metadata)

    if (output_node := wg.process) is None:
        msg = "Something went wrong when running workgraph. Please contact a developer."
        raise RuntimeError(msg)

    return output_node
