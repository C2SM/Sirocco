"""Dependency resolution utilities.

This module handles the resolution of task dependencies to AiiDA nodes.
Resolvers analyze workflow graphs, map dependencies, and convert them to
concrete RemoteData nodes for use in CalcJobs.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import aiida.orm

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.models import (
    DependencyInfo,
    DependencyMapping,
)

if TYPE_CHECKING:
    from f90nml import Namelist

    from sirocco.engines.aiida.types import (
        PortDataMapping,
        PortDependencyMapping,
        TaskFolderMapping,
        TaskJobIdMapping,
        TaskMonitorOutputsMapping,
    )

LOGGER = logging.getLogger(__name__)


def _resolve_icon_restart_file(
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


def _resolve_icon_output_stream_paths(icon_task: core.IconTask) -> dict[str, str]:
    """Extract output stream directory names from ICON namelist.

    Uses aiida-icon's read_output_stream_infos() to parse output_nml sections.

    Args:
        icon_task: The ICON task with model namelists

    Returns:
        Dict mapping stream data names to their directory names (e.g., {'atm_2d': 'atm_2d'})
    """
    from aiida_icon.iconutils import modelnml

    stream_paths = {}

    # Iterate through all model namelists to find output_nml sections
    for model_namelist in icon_task.model_namelists.values():
        # Get namelist data - handle both SinglefileData nodes and raw dicts
        nml_data = model_namelist.namelist if hasattr(model_namelist, "namelist") else model_namelist

        # Use aiida-icon's parser to extract output stream information
        try:
            stream_infos = modelnml.read_output_stream_infos(nml_data)
        except (KeyError, AttributeError):
            # This model namelist doesn't have output_nml sections
            continue

        # Match stream paths with output data names
        for stream_info in stream_infos:
            # stream_info.path is a pathlib.Path like 'atm_2d' or './atm_2d'
            dir_name = str(stream_info.path).strip("./")

            # Find matching output data by name
            for port, outputs in icon_task.outputs.items():
                if port == "output_streams":
                    for out_data in outputs:
                        # Match by name (e.g., 'atm_2d' matches 'atm_2d')
                        if out_data.name == dir_name or out_data.name in dir_name:
                            stream_paths[out_data.name] = dir_name

    return stream_paths


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
        return _resolve_icon_restart_file(
            workdir_path,
            model_name,
            model_namelist_pks,
            workdir_remote,
        )

    return workdir_remote


def resolve_icon_dependency_mapping(
    parent_folders: TaskFolderMapping | None,
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
    master_namelist_node: aiida.orm.SinglefileData = aiida.orm.load_node(master_namelist_pk)  # type: ignore[assignment]
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


def resolve_shell_dependency_mappings(
    parent_folders: TaskFolderMapping,
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

            # Create unique key and RemoteData for this dependency
            workdir_path = workdir_remote_data.get_remote_path()
            # Include data_label to distinguish multiple outputs from the same producer task
            unique_key = f"{dep_info.dep_label}_{dep_info.data_label}_remote"

            if dep_info.filename:
                # Create RemoteData pointing to the specific file/directory
                # Normalize path to remove trailing slashes
                specific_file_path = os.path.normpath(f"{workdir_path}/{dep_info.filename}")
                remote_data = aiida.orm.RemoteData(
                    computer=workdir_remote_data.computer,
                    remote_path=specific_file_path,
                )
                remote_data.store()
            else:
                # No specific filename, use the workdir itself (already stored)
                remote_data = workdir_remote_data

            all_nodes[unique_key] = remote_data

            # Build placeholder mapping for arguments
            placeholder_to_node_key[dep_info.data_label] = unique_key

            # Build filename mapping (from original_filenames via data_label)
            if dep_info.data_label in original_filenames:
                filenames[unique_key] = original_filenames[dep_info.data_label]

    return all_nodes, placeholder_to_node_key, filenames


def resolve_available_data_inputs(task: core.Task, aiida_data_nodes: PortDataMapping) -> PortDataMapping:
    """Resolve AvailableData input nodes for a task.

    Args:
        task: The task to collect inputs for
        aiida_data_nodes: Dict mapping data labels to AiiDA data nodes

    Returns:
        Dict mapping port names to AiiDA data nodes
    """
    input_data_for_task = {}
    for port, input_data in task.input_data_items():
        input_label = AiidaAdapter.build_label_from_graph_item(input_data)
        if isinstance(input_data, core.AvailableData):
            input_data_for_task[port] = aiida_data_nodes[input_label]

    return input_data_for_task


def build_dependency_mapping(
    task: core.Task,
    core_workflow: core.Workflow,
    task_output_mapping: TaskMonitorOutputsMapping,
) -> DependencyMapping:
    """Build dependency mapping for GeneratedData inputs.

    Analyzes the workflow graph to determine which tasks this task depends on,
    extracts filenames and paths, and collects job metadata.

    Returns:
        DependencyMapping with port_mapping, task_folders, and task_job_ids for connecting
        this task to its upstream dependencies.
    """

    port_mapping: PortDependencyMapping = {}
    task_folders: TaskFolderMapping = {}
    task_job_ids: TaskJobIdMapping = {}

    # Precompute: data_label → (producer_task_label, out_data)
    producers: dict[str, tuple[str, core.GeneratedData]] = {}

    for prev_task in core_workflow.tasks:
        prev_label = AiidaAdapter.build_label_from_graph_item(prev_task)

        for _, out_data in prev_task.output_data_items():
            out_label = AiidaAdapter.build_label_from_graph_item(out_data)
            producers[out_label] = (prev_label, out_data)

    # Process inputs for the current task
    for port, input_data in task.input_data_items():
        if not isinstance(input_data, core.GeneratedData):
            continue

        input_label = AiidaAdapter.build_label_from_graph_item(input_data)

        # Find the producer (if exists)
        producer_info = producers.get(input_label)
        if not producer_info:
            continue

        prev_label, out_data = producer_info

        # Extract filename/path if GeneratedData
        path = getattr(out_data, "path", None)
        filename = path.name if path else None

        # SPECIAL CASE: For ICON output streams, get directory from namelist
        if filename is None:
            # Find the producer task
            producer_task: core.graph_items.Task | None = next(
                (t for t in core_workflow.tasks if AiidaAdapter.build_label_from_graph_item(t) == prev_label), None
            )

            if producer_task is not None and isinstance(producer_task, core.IconTask):
                # Check if this output is on the output_streams port
                for port_, outputs in producer_task.outputs.items():
                    if port_ == "output_streams" and out_data in outputs:
                        # Extract output paths from ICON namelist
                        stream_paths = _resolve_icon_output_stream_paths(producer_task)
                        filename = stream_paths.get(out_data.name)
                        break

        # Only record dependencies if this producer has completed metadata
        if prev_label not in task_output_mapping:
            continue

        # Add to port dependency mapping
        port_mapping.setdefault(port, []).append(
            DependencyInfo(dep_label=prev_label, filename=filename, data_label=input_label)
        )

        # Add parent folder + job_id for producer (only once)
        if prev_label not in task_folders:
            job_data = task_output_mapping[prev_label]
            task_folders[prev_label] = job_data.remote_folder
            task_job_ids[prev_label] = job_data.job_id

    return DependencyMapping(
        port_mapping=port_mapping,
        task_folders=task_folders,
        task_job_ids=task_job_ids,
    )
