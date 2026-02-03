"""Dependency resolution utilities."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

import aiida.orm

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.types import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    AiidaMetadataOptions,
    DependencyInfo,
    DependencyMapping,
    JobIds,
    ParentFolders,
    PortDependencyMapping,
    TaskOutputMapping,
)

if TYPE_CHECKING:
    from f90nml import Namelist

    from sirocco.engines.aiida.types import AiidaDataNodeMapping

LOGGER = logging.getLogger(__name__)


def resolve_icon_restart_file(
    workdir_path: str,
    model_name: str,
    model_namelist_pks: dict[str, int],
    workdir_remote_data: aiida.orm.RemoteData,
) -> aiida.orm.RemoteData:
    """Resolve ICON restart file path using aiida-icon utilities.

    Args:
        workdir_path: Path to remote working directory
        model_name: Name of the model for which to resolve the restart file (e.g., "atm", "oce")
        model_namelist_pks: Dict of all model namelist PKs (needed for coupled simulations)
        workdir_remote_data: RemoteData for the workdir (fallback)

    Returns:
        RemoteData pointing to the restart file (or workdir if resolution fails)
    """
    import f90nml
    from aiida_icon.iconutils.modelnml import read_latest_restart_file_link_name

    # Load and combine all model namelists (needed for coupled simulations)
    nml_contents = []
    for model_pk in model_namelist_pks.values():
        model_node: aiida.orm.SinglefileData = aiida.orm.load_node(model_pk)  # type: ignore[assignment]
        with model_node.open(mode="r") as f:
            nml_contents.append(f.read())

    # Combine all model namelists into one (following aiida-icon's collect_model_nml pattern)
    combined_nml: Namelist = f90nml.reads("\n".join(nml_contents))

    # Use aiida-icon function to get the restart file link name
    # Pass combined namelist to support coupled model restart resolution
    restart_link_name = read_latest_restart_file_link_name(model_name, model_nml=combined_nml)
    specific_file_path = f"{workdir_path}/{restart_link_name}"

    return aiida.orm.RemoteData(
        computer=workdir_remote_data.computer,
        remote_path=specific_file_path,
    )


def _resolve_icon_dependency(
    dep_info: DependencyInfo,
    workdir_remote: aiida.orm.RemoteData,
    model_name: str,
    model_namelist_pks: dict,
) -> aiida.orm.RemoteData:
    """Resolve a single ICON dependency to RemoteData.

    Args:
        dep_info: Dependency information
        workdir_remote: RemoteData for the producer's working directory
        model_name: Name of the model (e.g., "atm", "ocean")
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

    # Case 2: No filename → resolve via model namelist
    if model_namelist_pks:
        return resolve_icon_restart_file(
            workdir_path,
            model_name,
            model_namelist_pks,
            workdir_remote,  # type: ignore
        )

    return workdir_remote


def resolve_icon_dependency_mapping(
    parent_folders: ParentFolders | None,
    port_dependency_mapping: PortDependencyMapping,
    master_namelist_pk: int,
    model_namelist_pks: dict,
) -> dict[str, aiida.orm.RemoteData]:
    """Resolve ICON task dependencies to RemoteData nodes mapped to input ports.

    This function handles ICON-specific dependency resolution, including:
    - Loading RemoteData from parent folder PKs
    - Resolving restart files using aiida-icon utilities and model namelists
    - Creating model-specific port mappings (e.g., "restart_file.atm")

    The return type is a single dict because ICON tasks (IconCalculation) only need
    RemoteData nodes mapped to their input ports. Unlike shell tasks, ICON doesn't
    need placeholder substitution or filename remapping - it reads configuration
    from Fortran namelists with standard file naming conventions.

    Args:
        parent_folders: Dict of parent folder RemoteData PKs
        port_dependency_mapping: Mapping of ports to their dependencies
        master_namelist_pk: PK of the master namelist
        model_namelist_pks: Dict of model namelist PKs

    Returns:
        Dict mapping port names (e.g., "restart_file.atm") to RemoteData nodes
    """
    import f90nml
    from aiida_icon.iconutils import masternml

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

    # Load master namelist to get model names
    master_namelist_node: aiida.orm.SinglefileData = aiida.orm.load_node(master_namelist_pk)  # type: ignore
    with master_namelist_node.open(mode="r") as f:
        master_nml_content = f.read()
    master_nml = f90nml.reads(master_nml_content)

    # Process each port → list of dependencies
    # For ICON, each port has at most 1 dependency
    for port_name, dep_list in port_dependency_mapping.items():
        if not dep_list:
            continue

        dep_info = dep_list[0]  # ICON: max 1 dependency per port

        workdir_remote = parent_folders_loaded.get(dep_info.dep_label)
        if not workdir_remote:
            continue

        # For restart files, we need to resolve per model
        # Iterate over all models in master namelist
        for model_name, _ in masternml.iter_model_name_filepath(master_nml):
            if model_name in model_namelist_pks:
                # Use helper to resolve dependency for this model
                resolved_remote = _resolve_icon_dependency(
                    dep_info,
                    workdir_remote,
                    model_name,
                    model_namelist_pks,
                )
                # Use model-specific port name (e.g., "restart_file.atm")
                input_nodes[f"{port_name}.{model_name}"] = resolved_remote

    return input_nodes


def build_icon_calcjob_inputs(
    task_spec: AiidaIconTaskSpec,
    input_data_nodes: dict,
    aiida_metadata: AiidaMetadata,
) -> dict:
    """Build complete inputs dict for IconCalculation.

    Assembles all required inputs for an ICON CalcJob from multiple sources:
    - Loads code and namelists from stored PKs
    - Assembles model namelists into a namespace dict
    - Adds wrapper script if present
    - Adds input data nodes, handling namespace vs regular ports
    - Converts and adds AiiDA metadata

    Args:
        task_spec: AiidaIconTaskSpec model with task specifications
        input_data_nodes: Dict of input data nodes (both AvailableData and RemoteData)
        aiida_metadata: AiidaMetadata model with computer and options

    Returns:
        Complete inputs dict ready for IconCalculation submission
    """
    from aiida.engine.processes.ports import PortNamespace
    from aiida_icon.calculations import IconCalculation

    # Get IconCalculation spec to determine which ports are namespaces
    icon_spec = IconCalculation.spec()

    # Start with code and namelists
    inputs: dict[str, Any] = {
        "code": aiida.orm.load_node(task_spec.code_pk),
        "master_namelist": aiida.orm.load_node(task_spec.master_namelist_pk),
    }

    # Add model namelists as a dict (namespace input)
    models = {}
    for model_name, model_pk in task_spec.model_namelist_pks.items():
        models[model_name] = aiida.orm.load_node(model_pk)
    if models:
        inputs["models"] = models  # type: ignore[assignment]

    # Add wrapper script if present
    if task_spec.wrapper_script_pk is not None:
        inputs["wrapper_script"] = aiida.orm.load_node(task_spec.wrapper_script_pk)  # type: ignore[assignment]

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

    # Add metadata (convert Pydantic model to dict for AiiDA)
    inputs["metadata"] = aiida_metadata.model_dump(mode="python", exclude_none=True)  # type: ignore[assignment]

    LOGGER.debug("ICON inputs=%s", inputs)

    return inputs


# TODO: Re-evaluate this function. Is it really needed?
def _resolve_remote_data_for_dependency(
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
        remote_data.store()
    else:
        # No specific filename, use the workdir itself (already stored)
        remote_data = workdir_remote

    return unique_key, remote_data


def resolve_shell_dependency_mappings(
    parent_folders: ParentFolders,
    port_dependency_mapping: PortDependencyMapping,
    original_filenames: dict,
) -> tuple[dict, dict, dict]:
    """Resolve shell task dependencies and build multiple mappings for aiida-shell.

    This function handles shell-specific dependency resolution, including:
    - Loading RemoteData from parent folder PKs
    - Creating or reusing RemoteData for specific files/directories
    - Building placeholder mappings for command argument substitution
    - Building filename mappings for file staging

    The return type is a tuple of 3 dicts because shell tasks (via aiida-shell) need:
    1. RemoteData nodes for inputs
    2. Placeholder mappings to substitute {data_label} in shell command arguments
    3. Filename mappings to control how files are named when staged/linked

    This is different from ICON tasks which use fixed input ports and read
    configuration from namelists rather than command-line arguments.

    Args:
        parent_folders: Dict of {dep_label: remote_folder_pk_tagged_value}
        port_dependency_mapping: Dict mapping port names to list of DependencyInfo objects
        original_filenames: Dict mapping data labels to filenames

    Returns:
        Tuple of (all_nodes, placeholder_to_node_key, filenames) dicts:
        - all_nodes: Dict mapping unique keys to RemoteData nodes
        - placeholder_to_node_key: Dict mapping data labels to node keys (for argument substitution)
        - filenames: Dict mapping node keys to filenames (for file staging)
    """
    all_nodes: dict[str, aiida.orm.RemoteData] = {}
    placeholder_to_node_key: dict[str, str] = {}
    filenames: dict[str, str] = {}

    # Load RemoteData nodes from their PKs
    parent_folders_loaded: dict[str, aiida.orm.RemoteData] = {}
    for key, val in parent_folders.items():
        node = aiida.orm.load_node(val.value)
        if not isinstance(node, aiida.orm.RemoteData):
            msg = f"Expected RemoteData for {key} but got {type(node)}"
            raise TypeError(msg)
        parent_folders_loaded[key] = node

    # Process ALL dependencies: create nodes, map placeholders, and map filenames
    for dep_info_list in port_dependency_mapping.values():
        # dep_info_list is a list of DependencyInfo objects
        for dep_info in dep_info_list:
            if dep_info.dep_label not in parent_folders_loaded:
                continue

            workdir_remote_data = parent_folders_loaded[dep_info.dep_label]

            # Use helper to create RemoteData
            unique_key, remote_data = _resolve_remote_data_for_dependency(dep_info, workdir_remote_data)
            all_nodes[unique_key] = remote_data

            # Build placeholder mapping for arguments
            placeholder_to_node_key[dep_info.data_label] = unique_key

            # Build filename mapping (from original_filenames via data_label)
            if dep_info.data_label in original_filenames:
                filenames[unique_key] = original_filenames[dep_info.data_label]

    return all_nodes, placeholder_to_node_key, filenames


def build_slurm_dependency_directive(job_ids: JobIds) -> str:
    """Build SLURM --dependency directive from job IDs.

    Args:
        job_ids: Dict of {dep_label: job_id_tagged_value}

    Returns:
        SLURM directive string like "#SBATCH --dependency=afterok:123:456"
    """
    dep_str = ":".join(str(jid.value) for jid in job_ids.values())
    return f"#SBATCH --dependency=afterok:{dep_str} --kill-on-invalid-dep=yes"


def add_slurm_dependencies_to_metadata(
    base_metadata: AiidaMetadata,
    job_ids: JobIds | None,
    computer: aiida.orm.Computer | None,
    label: str | None = None,
) -> AiidaMetadata:
    """Add SLURM job dependencies to metadata.

    Args:
        base_metadata: Base metadata from task spec
        job_ids: Dict of {dep_label: job_id_tagged_value} or None
        computer: AiiDA computer object
        label: Optional task label for call_link_label (ICON tasks only)

    Returns:
        AiidaMetadata with computer and optional SLURM dependencies
    """
    # Get options or create empty if None
    options = base_metadata.options or AiidaMetadataOptions()

    # Add SLURM dependency directive if needed
    if job_ids:
        custom_cmd = build_slurm_dependency_directive(job_ids)
        current_cmds = options.custom_scheduler_commands or ""
        new_cmds = f"{current_cmds}\n{custom_cmd}" if current_cmds else custom_cmd
        options = options.model_copy(update={"custom_scheduler_commands": new_cmds})

    if label is not None:
        return AiidaMetadata(computer=computer, options=options, call_link_label=label)
    return AiidaMetadata(computer=computer, options=options)


def collect_available_data_inputs(task: core.Task, aiida_data_nodes: AiidaDataNodeMapping) -> AiidaDataNodeMapping:
    """Collect AvailableData input nodes for a task.

    Args:
        task: The task to collect inputs for
        aiida_data_nodes: Dict mapping data labels to AiiDA data nodes

    Returns:
        Dict mapping port names to AiiDA data nodes
    """
    input_data_for_task = {}
    for port, input_data in task.input_data_items():
        input_label = AiidaAdapter.build_graph_item_label(input_data)
        if isinstance(input_data, core.AvailableData):
            input_data_for_task[port] = aiida_data_nodes[input_label]

    return input_data_for_task


def build_dependency_mapping(
    task: core.Task,
    core_workflow: core.Workflow,
    task_output_mapping: TaskOutputMapping,
) -> DependencyMapping:
    """Build dependency mapping for GeneratedData inputs.

    Returns:
        DependencyMapping with port_mapping, parent_folders, and job_ids for connecting
        this task to its upstream dependencies.
    """

    port_mapping: PortDependencyMapping = {}
    parent_folders: ParentFolders = {}
    job_ids: JobIds = {}

    # Precompute: data_label → (producer_task_label, out_data)
    producers: dict[str, tuple[str, core.GeneratedData]] = {}

    for prev_task in core_workflow.tasks:
        prev_label = AiidaAdapter.build_graph_item_label(prev_task)

        for _, out_data in prev_task.output_data_items():
            out_label = AiidaAdapter.build_graph_item_label(out_data)
            producers[out_label] = (prev_label, out_data)

    # Process inputs for the current task
    for port, input_data in task.input_data_items():
        if not isinstance(input_data, core.GeneratedData):
            continue

        input_label = AiidaAdapter.build_graph_item_label(input_data)

        # Find the producer (if exists)
        producer_info = producers.get(input_label)
        if not producer_info:
            continue

        prev_label, out_data = producer_info

        # Extract filename/path if GeneratedData
        path = getattr(out_data, "path", None)
        filename = path.name if path else None

        # Only record dependencies if this producer has completed metadata
        if prev_label not in task_output_mapping:
            continue

        # Add to port dependency mapping
        port_mapping.setdefault(port, []).append(
            DependencyInfo(dep_label=prev_label, filename=filename, data_label=input_label)
        )

        # Add parent folder + job_id for producer (only once)
        if prev_label not in parent_folders:
            job_data = task_output_mapping[prev_label]
            parent_folders[prev_label] = job_data.remote_folder
            job_ids[prev_label] = job_data.job_id

    return DependencyMapping(
        port_mapping=port_mapping,
        parent_folders=parent_folders,
        job_ids=job_ids,
    )
