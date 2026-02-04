"""Launcher task creation and monitoring functions."""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
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
from sirocco.engines.aiida.dependencies import (
    add_slurm_dependencies_to_metadata,
    build_icon_calcjob_inputs,
    resolve_icon_dependency_mapping,
    resolve_shell_dependency_mappings,
)
from sirocco.engines.aiida.types import (
    AiidaIconTaskSpec,
    AiidaShellTaskSpec,
    DependencyInfo,
    DependencyMapping,
    InputDataInfo,
    OutputDataInfo,
    WgTaskProtocol,
)
from sirocco.engines.aiida.utils import serialize_coordinates, split_cmd_arg

if TYPE_CHECKING:
    from aiida_workgraph import WorkGraph

    from sirocco.engines.aiida.types import (
        AiidaDataNodeMapping,
        DependencyTasks,
        TaskOutputMapping,
    )

LOGGER = logging.getLogger(__name__)

# Task name prefixes for WorkGraph tasks
# TODO: Check if WG has a specific `Monitor` task type to avoid hard-coding
LAUNCHER_PREFIX = "launch_"
MONITOR_PREFIX = "get_job_data_"


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

    Returns a dict with keys: label, code, nodes, metadata,
    arguments_template, filenames, outputs, input_data_info, output_data_info

    NOTE: Job dependencies are NOT included here - they're added at runtime.

    Args:
        task: The ShellTask to build spec for

    Returns:
        Dict containing all shell task parameters
    """
    label = AiidaAdapter.build_graph_item_label(task)

    # Get computer
    try:
        computer = aiida.orm.Computer.collection.get(label=task.computer)
    except NotExistent as err:
        msg = f"Could not find computer {task.computer!r} in AiiDA database."
        raise ValueError(msg) from err

    # Build base metadata (no job dependencies yet)
    metadata = AiidaAdapter.build_metadata(task)

    # Add shell-specific metadata options
    if metadata.options:
        metadata = metadata.model_copy(update={"options": metadata.options.model_copy(update={"use_symlinks": True})})

    # Create or load code
    code = AiidaAdapter.create_shell_code(task, computer)

    # Pre-compute input data information using dataclasses
    input_data_info: list[InputDataInfo] = []
    for port_name, input_ in task.input_data_items():
        input_info = InputDataInfo(
            port=port_name,
            name=input_.name,
            coordinates=serialize_coordinates(input_.coordinates),
            label=AiidaAdapter.build_graph_item_label(input_),
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
    output_data_info: list[OutputDataInfo] = []
    for port_name, output in task.output_data_items():  # type: ignore[assignment]
        output_info = OutputDataInfo(
            name=output.name,
            coordinates=serialize_coordinates(output.coordinates),
            label=AiidaAdapter.build_graph_item_label(output),
            path=str(output.path) if output.path is not None else "",  # type: ignore[attr-defined]
            port=port_name,
        )
        output_data_info.append(output_info)

    # Build output labels
    output_labels: dict[str, list[str]] = {}
    for output_info in output_data_info:
        if output_info.port is None:
            continue  # Skip outputs without a port
        port_name = output_info.port
        output_label = output_info.label
        if port_name not in output_labels:
            output_labels[port_name] = []
        # For AvailableData with a path, use the actual path directly in command arguments
        if output_info.path:
            output_labels[port_name].append(output_info.path)
        else:
            output_labels[port_name].append(f"{{{output_label}}}")

    # Pre-scan command template to find all referenced ports
    # This ensures optional/missing ports are included with empty lists
    for port_match in task.port_pattern.finditer(task.command):
        port_name = port_match.group(2)
        if port_name and port_name not in input_labels:
            input_labels[port_name] = []

    # Pre-resolve arguments template
    # Get script name from task.path for proper command splitting
    script_name = Path(task.path).name if task.path else None

    # Merge output labels into input labels for resolution
    input_labels.update(output_labels)
    arguments_with_placeholders = task.resolve_ports(input_labels)  # type: ignore[arg-type]

    _, resolved_arguments_template = split_cmd_arg(arguments_with_placeholders, script_name)

    # Build filenames mapping
    filenames = {}
    for input_info in input_data_info:
        input_label = input_info.label
        if input_info.is_available:
            filenames[input_info.name] = Path(input_info.path).name if input_info.path else input_info.name  # type: ignore[arg-type]
        else:
            # Count how many inputs have the same name
            same_name_count = sum(1 for info in input_data_info if info.name == input_info.name)
            if same_name_count > 1:
                filenames[input_label] = input_label
            else:
                filenames[input_label] = Path(input_info.path).name if input_info.path else input_info.name  # type: ignore[arg-type]

    # Build outputs list - but DON'T retrieve, just verify existence
    outputs = []  # type: ignore[var-annotated]

    # Build output port mapping: data_name -> shell output link_label
    output_port_mapping = {}
    for output_info in output_data_info:
        if output_info.path:
            link_label = ShellParser.format_link_label(output_info.path)  # type: ignore[arg-type]
            output_port_mapping[output_info.name] = link_label

    if code.pk is None:
        msg = f"Code for task {label} must be stored before creating task spec"
        raise RuntimeError(msg)

    return AiidaShellTaskSpec(
        label=label,
        code_pk=code.pk,
        node_pks={},
        metadata=metadata,
        arguments_template=resolved_arguments_template,
        filenames=filenames,
        outputs=outputs,
        input_data_info=[info.model_dump() for info in input_data_info],
        output_data_info=[info.model_dump() for info in output_data_info],
        output_port_mapping=output_port_mapping,
    )


def build_icon_task_spec(task: core.IconTask) -> AiidaIconTaskSpec:
    """Build all parameters needed to create an ICON task.

    Returns a dict with keys: label, builder, output_ports

    Note: Job dependencies are NOT included here - they're added at runtime.

    Args:
        task: The IconTask to build spec for

    Returns:
        Dict containing all ICON task parameters
    """
    task_label = AiidaAdapter.build_graph_item_label(task)

    try:
        computer = aiida.orm.Computer.collection.get(label=task.computer)
    except NotExistent as err:
        msg = f"Could not find computer {task.computer!r} in AiiDA database."
        raise ValueError(msg) from err

    # Create or load ICON code with unique label based on executable path
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
            with_mpi=True,
            use_double_quotes=True,
        )
        icon_code.store()

    # Build base metadata (no job dependencies yet)
    metadata = AiidaAdapter.build_metadata(task)

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
            model_node = create_namelist_singlefiledata_from_content(content, model_nml.name, store=True)
            model_namelist_pks[model_name] = model_node.pk

    # Wrapper script - store as PK if present
    wrapper_script_pk = None
    wrapper_script_data = AiidaAdapter.get_wrapper_script_data(task)
    if wrapper_script_data is not None:
        wrapper_script_data.store()
        wrapper_script_pk = wrapper_script_data.pk

    # Pre-compute output port mapping: data_name -> icon_port_name
    output_port_mapping: dict[str, str] = {}
    for port_name, output_list in task.outputs.items():
        if port_name is not None:
            for data in output_list:
                output_port_mapping[data.name] = port_name

    # Pydantic validation will check that PKs are not None
    return AiidaIconTaskSpec(
        label=task_label,
        code_pk=icon_code.pk,  # type: ignore[arg-type]
        master_namelist_pk=master_namelist_node.pk,  # type: ignore[arg-type]
        model_namelist_pks=model_namelist_pks,  # type: ignore[arg-type]
        wrapper_script_pk=wrapper_script_pk,
        metadata=metadata,
        output_port_mapping=output_port_mapping,
    )


@task.graph()
def build_shell_task_with_dependencies(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    parent_folders: Annotated[dict, dynamic(int)] | None = None,
    job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic()]:
    """Launch a shell task with optional SLURM job dependencies."""
    from aiida_workgraph.tasks.shelljob_task import _build_shelljob_TaskSpec

    # Reconstruct Pydantic model from dict
    task_spec_model = AiidaShellTaskSpec(**task_spec)

    # Get pre-computed data
    label = task_spec_model.label
    output_data_info = task_spec_model.output_data_info

    # Load the code from PK
    code = aiida.orm.load_node(task_spec_model.code_pk)

    # Load nodes from PKs and initialize structures
    all_nodes = {key: aiida.orm.load_node(pk) for key, pk in task_spec_model.node_pks.items()}

    # Add AvailableData nodes passed as parameter, remapping from port names to data labels
    # so they match the placeholders in arguments (which use data labels)
    placeholder_to_node_key: dict[str, str] = {}
    if input_data_nodes:
        # Build mapping from port names to data labels for AvailableData
        port_to_label = {}
        for input_info in task_spec_model.input_data_info:
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
        # Convert port_dependency_mapping from dict back to PortDependencyMapping
        port_dep_dict = task_spec_model.port_dependency_mapping or {}
        port_dependencies = {port: [DependencyInfo(**dep) for dep in deps] for port, deps in port_dep_dict.items()}

        dep_nodes, placeholder_to_node_key, filenames = resolve_shell_dependency_mappings(
            parent_folders,
            port_dependencies,
            task_spec_model.filenames,
        )
        all_nodes.update(dep_nodes)

    # Build metadata with SLURM job dependencies
    base_metadata = task_spec_model.metadata
    computer = (
        aiida.orm.Computer.collection.get(label=base_metadata.computer_label) if base_metadata.computer_label else None
    )
    metadata = add_slurm_dependencies_to_metadata(base_metadata, job_ids, computer)  # type: ignore[arg-type]

    # Process argument placeholders
    arguments = AiidaAdapter.substitute_argument_placeholders(
        task_spec_model.arguments_template, placeholder_to_node_key
    )

    # Use pre-computed outputs
    outputs = task_spec_model.outputs

    # Build the ShellJob TaskSpec
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
        metadata=metadata.model_dump(mode="python", exclude_none=True),
        resolve_command=False,
    )

    # Return outputs directly (WorkGraph will wrap them)
    return shell_task.outputs  # type: ignore[return-value]


@task.graph(outputs=task(IconCalculation).outputs)  # type: ignore[attr-defined]
def build_icon_task_with_dependencies(
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
        task_spec: Dict from WorkGraph (serialized IconTaskSpec)
        input_data_nodes: Dict mapping port names to AvailableData nodes
        parent_folders: Dict of {dep_label: remote_folder_pk} from get_job_data
        job_ids: Dict of {dep_label: job_id} from get_job_data

    Returns:
        IconTask outputs
    """
    # Reconstruct Pydantic model from dict
    task_spec_model = AiidaIconTaskSpec(**task_spec)

    label = task_spec_model.label
    computer_label = task_spec_model.metadata.computer_label if task_spec_model.metadata else None

    # Handle None inputs
    if input_data_nodes is None:
        input_data_nodes = {}

    # Load RemoteData dependencies (restart files, etc.)
    # Convert port_dependency_mapping from dict back to PortDependencyMapping
    port_dep_dict = task_spec_model.port_dependency_mapping or {}
    port_dependencies = {port: [DependencyInfo(**dep) for dep in deps] for port, deps in port_dep_dict.items()}

    remote_data_nodes = resolve_icon_dependency_mapping(
        parent_folders,
        port_dependencies,
        task_spec_model.master_namelist_pk,
        task_spec_model.model_namelist_pks,
    )
    input_data_nodes.update(remote_data_nodes)

    # Build metadata with SLURM job dependencies
    base_metadata = task_spec_model.metadata
    computer = aiida.orm.Computer.collection.get(label=computer_label) if computer_label else None
    metadata = add_slurm_dependencies_to_metadata(base_metadata, job_ids, computer, label)  # type: ignore[arg-type]

    # Build complete inputs dict for IconCalculation
    inputs = build_icon_calcjob_inputs(task_spec_model, input_data_nodes, metadata)

    # Call IconTask directly (NOT wg.add_task!)
    # This returns a TaskNode with proper output sockets
    return task(IconCalculation)(**inputs)


def _add_task_pair_common(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: AiidaIconTaskSpec | AiidaShellTaskSpec,
    build_task_func,
    input_data_for_task: AiidaDataNodeMapping,
    dependencies: DependencyMapping,
    task_dep_info: TaskOutputMapping,
    prev_dep_tasks: DependencyTasks,
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
        task_dep_info: Dict to update with new task outputs (mutated in place)
        prev_dep_tasks: Dict to update with new dependency task (mutated in place)
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
        parent_folders=dependencies.parent_folders if dependencies.parent_folders else None,
        job_ids=dependencies.job_ids if dependencies.job_ids else None,
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
    task_dep_info[task_label] = dep_task.outputs

    # Chain with previous dependency tasks
    for dep_label in dependencies.parent_folders:
        if dep_label in prev_dep_tasks:
            _ = prev_dep_tasks[dep_label] >> dep_task

    # Store for next iteration
    prev_dep_tasks[task_label] = dep_task


def add_icon_task_pair(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: AiidaIconTaskSpec,
    input_data_for_task: AiidaDataNodeMapping,
    dependencies: DependencyMapping,
    task_dep_info: TaskOutputMapping,
    prev_dep_tasks: DependencyTasks,
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
        task_dep_info: Dict to update with new task outputs (mutated in place)
        prev_dep_tasks: Dict to update with new dependency task (mutated in place)
    """
    _add_task_pair_common(
        wg,
        wg_name,
        task_label,
        task_spec,
        build_icon_task_with_dependencies,
        input_data_for_task,
        dependencies,
        task_dep_info,
        prev_dep_tasks,
    )


def add_shell_task_pair(
    wg: WorkGraph,
    wg_name: str,
    task_label: str,
    task_spec: AiidaShellTaskSpec,
    input_data_for_task: AiidaDataNodeMapping,
    dependencies: DependencyMapping,
    task_dep_info: TaskOutputMapping,
    prev_dep_tasks: DependencyTasks,
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
        task_dep_info: Dict to update with new task outputs (mutated in place)
        prev_dep_tasks: Dict to update with new dependency task (mutated in place)
    """
    _add_task_pair_common(
        wg,
        wg_name,
        task_label,
        task_spec,
        build_shell_task_with_dependencies,
        input_data_for_task,
        dependencies,
        task_dep_info,
        prev_dep_tasks,
    )
