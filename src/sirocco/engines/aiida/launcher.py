"""Launcher task creation and monitoring functions."""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated

import aiida.orm
import yaml
from aiida.orm.utils.serialize import AiiDALoader
from aiida_icon.calculations import IconCalculation
from aiida_workgraph import dynamic, get_current_graph, namespace, task

from sirocco.engines.aiida.dependencies import (
    build_icon_metadata_with_slurm_dependencies,
    build_shell_metadata_with_slurm_dependencies,
    load_and_process_shell_dependencies,
    load_icon_dependencies,
    prepare_icon_task_inputs,
)
from sirocco.engines.aiida.task_specs import port_to_dependencies_from_dict, port_to_dependencies_to_dict
from sirocco.engines.aiida.utils import process_shell_argument_placeholders

LOGGER = logging.getLogger(__name__)


# =============================================================================
# Async Job Data Monitor
# =============================================================================


async def _poll_for_job_data(workgraph_name: str, task_name: str, interval: int) -> dict:
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
    interval: int = 10,
    timeout_seconds: int = 3600,
):
    """Monitor CalcJob and return job_id and remote_folder PK when available.

    Args:
        workgraph_name: Name of the WorkGraph to monitor
        task_name: Name of the task within the WorkGraph
        interval: Polling interval in seconds
        timeout_seconds: Timeout in seconds (based on task walltime + buffer)

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


# =============================================================================
# Launcher Task Creation
# =============================================================================


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
    all_nodes = {key: aiida.orm.load_node(pk) for key, pk in task_spec["node_pks"].items()}

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
        port_to_dep = port_to_dependencies_from_dict(port_to_dep_dict)

        dep_nodes, placeholder_to_node_key, filenames = load_and_process_shell_dependencies(
            parent_folders,
            port_to_dep,
            task_spec["filenames"],
        )
        all_nodes.update(dep_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(label=task_spec["metadata"]["computer_label"])
    metadata = build_shell_metadata_with_slurm_dependencies(task_spec["metadata"], job_ids, computer)

    # Process argument placeholders
    arguments = process_shell_argument_placeholders(task_spec["arguments_template"], placeholder_to_node_key)

    # Use pre-computed outputs
    outputs = task_spec["outputs"]

    # Build the shelljob TaskSpec
    parser_outputs = [output_info["name"] for output_info in output_data_info if output_info["path"]]

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
        task_spec: Dict from build_icon_task_spec() containing task specifications
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
    port_to_dep = port_to_dependencies_from_dict(port_to_dep_dict)

    remote_data_nodes = load_icon_dependencies(
        parent_folders,
        port_to_dep,
        task_spec["master_namelist_pk"],
        task_spec["model_namelist_pks"],
    )
    input_data_nodes.update(remote_data_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(label=computer_label)
    metadata_dict = build_icon_metadata_with_slurm_dependencies(task_spec["metadata"], job_ids, computer, label)

    # Prepare complete inputs dict for IconTask
    inputs = prepare_icon_task_inputs(task_spec, input_data_nodes, metadata_dict)

    # Call IconTask directly (NOT wg.add_task!)
    # This returns a TaskNode with proper output sockets
    return IconTask(**inputs)


# =============================================================================
# Helper Functions - Create Launcher Pairs
# =============================================================================


def create_icon_launcher_pair(
    wg,
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
    task_spec["port_to_dep_mapping"] = port_to_dependencies_to_dict(port_to_dep_mapping)

    # Create launcher task
    wg.add_task(
        launch_icon_task_with_dependency,
        name=launcher_name,
        task_spec=task_spec,
        input_data_nodes=input_data_for_task if input_data_for_task else None,
        parent_folders=parent_folders_for_task if parent_folders_for_task else None,
        job_ids=job_ids_for_task if job_ids_for_task else None,
    )

    # Calculate timeout based on walltime + buffer for queuing
    # Default to 1 hour if walltime not specified
    walltime_seconds = task_spec.get("metadata", {}).get("options", {}).get("max_wallclock_seconds", 3600)
    # Add 5 minute buffer for job submission and queuing
    timeout = walltime_seconds + 300

    # Create get_job_data task
    dep_task = wg.add_task(
        get_job_data,
        name=f"get_job_data_{task_label}",
        workgraph_name=launcher_name,
        task_name=task_label,
        timeout_seconds=timeout,
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


def create_shell_launcher_pair(
    wg,
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
    task_spec["port_to_dep_mapping"] = port_to_dependencies_to_dict(port_to_dep_mapping)

    # Create launcher task
    wg.add_task(
        launch_shell_task_with_dependency,
        name=launcher_name,
        task_spec=task_spec,
        input_data_nodes=input_data_for_task if input_data_for_task else None,
        parent_folders=parent_folders_for_task if parent_folders_for_task else None,
        job_ids=job_ids_for_task if job_ids_for_task else None,
    )

    # Calculate timeout based on walltime + buffer for queuing
    # Default to 1 hour if walltime not specified
    walltime_seconds = task_spec.get("metadata", {}).get("options", {}).get("max_wallclock_seconds", 3600)
    # Add 5 minute buffer for job submission and queuing
    timeout = walltime_seconds + 300

    # Create get_job_data task
    dep_task = wg.add_task(
        get_job_data,
        name=f"get_job_data_{task_label}",
        workgraph_name=launcher_name,
        task_name=task_label,
        timeout_seconds=timeout,
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
