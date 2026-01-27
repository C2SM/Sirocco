"""Dependency resolution utilities."""

from __future__ import annotations

import logging
import os
from typing import Any

import aiida.orm

from sirocco import core
from sirocco.workgraph.task_specs import DependencyInfo, JobIds, ParentFolders, PortToDependencies, TaskDepInfo

LOGGER = logging.getLogger(__name__)


# =============================================================================
# ICON Task Dependency Resolution
# =============================================================================


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

    # Load RemoteData for each parent folder (remote_folder pk)
    parent_folders_loaded: dict[str, aiida.orm.RemoteData] = {}
    for dep_label, tagged_val in parent_folders.items():
        node = aiida.orm.load_node(tagged_val.value)
        if not isinstance(node, aiida.orm.RemoteData):
            msg = f"Expected RemoteData for {dep_label} but got {type(node)}"
            raise TypeError(msg)
        parent_folders_loaded[dep_label] = node

    # Process each port → list of dependencies
    # For ICON, each port has at most 1 dependency
    for port_name, dep_list in port_to_dep_mapping.items():
        if not dep_list:
            continue

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

    LOGGER.debug("ICON inputs=%s", inputs)

    return inputs


# =============================================================================
# Shell Task Dependency Resolution
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
    parent_folders_loaded: dict[str, Any] = {key: aiida.orm.load_node(val.value) for key, val in parent_folders.items()}

    # Process ALL dependencies: create nodes, map placeholders, and map filenames
    for dep_info_list in port_to_dep_mapping.values():
        # dep_info_list is a list of DependencyInfo objects
        for dep_info in dep_info_list:
            if dep_info.dep_label not in parent_folders_loaded:
                continue

            workdir_remote_data = parent_folders_loaded[dep_info.dep_label]

            # Use helper to create RemoteData
            unique_key, remote_data = _create_shell_remote_data(dep_info, workdir_remote_data)
            all_nodes[unique_key] = remote_data

            # Build placeholder mapping for arguments
            placeholder_to_node_key[dep_info.data_label] = unique_key

            # Build filename mapping (from original_filenames via data_label)
            if dep_info.data_label in original_filenames:
                filenames[unique_key] = original_filenames[dep_info.data_label]

    return all_nodes, placeholder_to_node_key, filenames


# =============================================================================
# SLURM Dependency Directives
# =============================================================================


def build_slurm_dependency_directive(job_ids: JobIds) -> str:
    """Build SLURM --dependency directive from job IDs.

    Args:
        job_ids: Dict of {dep_label: job_id_tagged_value}

    Returns:
        SLURM directive string like "#SBATCH --dependency=afterok:123:456"
    """
    dep_str = ":".join(str(jid.value) for jid in job_ids.values())
    return f"#SBATCH --dependency=afterok:{dep_str} --kill-on-invalid-dep=yes"


def add_custom_scheduler_command(metadata: dict, command: str) -> None:
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
        custom_cmd = build_slurm_dependency_directive(job_ids)
        add_custom_scheduler_command(metadata, custom_cmd)

    return {
        "computer": computer,
        "options": metadata["options"],
        "call_link_label": label,
    }


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
        custom_cmd = build_slurm_dependency_directive(job_ids)
        add_custom_scheduler_command(metadata, custom_cmd)

    LOGGER.debug("metadata=%s", metadata)

    return metadata


# =============================================================================
# Dependency Mapping
# =============================================================================


def collect_available_data_inputs(task: core.Task, aiida_data_nodes: dict, get_label_func) -> dict:
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

    # Precompute: data_label → (producer_task_label, out_data)
    producers: dict[str, tuple[str, core.GeneratedData]] = {}

    for prev_task in core_workflow.tasks:
        prev_label = get_label_func(prev_task)

        for _, out_data in prev_task.output_data_items():
            out_label = get_label_func(out_data)
            producers[out_label] = (prev_label, out_data)

    # Process inputs for the current task
    for port, input_data in task.input_data_items():
        if not isinstance(input_data, core.GeneratedData):
            continue

        input_label = get_label_func(input_data)

        # Find the producer (if exists)
        producer_info = producers.get(input_label)
        if not producer_info:
            continue

        prev_label, out_data = producer_info

        # Extract filename/path if GeneratedData
        path = getattr(out_data, "path", None)
        filename = path.name if path else None

        # Only record dependencies if this producer has completed metadata
        if prev_label not in task_dep_info:
            continue

        # Add to port dependency mapping
        port_to_dep.setdefault(port, []).append(
            DependencyInfo(dep_label=prev_label, filename=filename, data_label=input_label)
        )

        # Add parent folder + job_id for producer (only once)
        if prev_label not in parent_folders:
            job_data = task_dep_info[prev_label]
            parent_folders[prev_label] = job_data.remote_folder
            job_ids[prev_label] = job_data.job_id

    return port_to_dep, parent_folders, job_ids
