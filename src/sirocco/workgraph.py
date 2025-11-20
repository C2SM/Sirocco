from __future__ import annotations

import asyncio
import io
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, TypeAlias, assert_never

import aiida.common
import aiida.orm
import aiida.transports
import aiida.transports.plugins.local
import yaml
from aiida.common.exceptions import NotExistent
from aiida.orm.utils.serialize import AiiDALoader
from aiida_icon.calculations import IconCalculation
from aiida_icon.iconutils.namelists import create_namelist_singlefiledata_from_content
from aiida_shell.parsers.shell import ShellParser
from aiida_workgraph import WorkGraph, dynamic, get_current_graph, namespace, task

from sirocco import core
from sirocco.parsing._utils import TimeUtils

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    WorkgraphDataNode: TypeAlias = aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData


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


@task(outputs=namespace(job_id=int, remote_folder=int))
async def get_job_data(
    workgraph_name: str,
    task_name: str,
    interval: int = 5,
    timeout: int = 180,
):
    """Monitor CalcJob and return job_id and remote_folder PK when available.

    Generic function for all task types.

    Args:
        workgraph_name: Name of the launcher sub-WorkGraph
        task_name: Name of the CalcJob task inside the sub-WorkGraph
        interval: Polling interval in seconds
        timeout: Maximum time to wait in seconds

    Returns:
        Namespace with job_id (int) and remote_folder (int pk)
    """
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine

    start_time_unix = time.time()

    logger.debug("Starting job_data polling for %s in %s", task_name, workgraph_name)

    while True:
        # Query for WorkGraphs created AFTER this task started polling
        builder = orm.QueryBuilder()
        builder.append(
            WorkGraphEngine,
            filters={
                "attributes.process_label": {"==": f"WorkGraph<{workgraph_name}>"},
                "ctime": {">": datetime.fromtimestamp(start_time_unix - 10, tz=UTC)},
            },
            tag="process",
        )

        if builder.count() > 0:
            workgraph_node = builder.all()[-1][0]
            node_data = workgraph_node.task_processes.get(task_name, "")
            if node_data:
                node = yaml.load(node_data, Loader=AiiDALoader)  # noqa: S506
                if node:
                    job_id = node.get_job_id()
                    if job_id is not None:
                        # When job_id is ready, remote_folder is also available
                        logger.debug(
                            "Retrieved job_id=%s, remote_folder_pk=%s for %s",
                            job_id,
                            node.outputs.remote_folder.pk,
                            task_name,
                        )
                        return {
                            "job_id": int(job_id),
                            "remote_folder": node.outputs.remote_folder.pk,
                        }

        # Check timeout
        elapsed = time.time() - start_time_unix
        if elapsed > timeout:
            msg = f"Timeout waiting for job_id for task {task_name} after {timeout}s"
            raise TimeoutError(msg)

        await asyncio.sleep(interval)


@task.graph
def launch_shell_task_with_dependency(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    parent_folders: Annotated[dict, dynamic(int)] | None = None,
    job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic()]:
    # ) -> Annotated[dict, dynamic(aiida.orm.Data)]:
    """Launch a shell task with optional SLURM job dependencies."""
    from aiida_workgraph.tasks.shelljob_task import _build_shelljob_nodespec

    # Get pre-computed data

    label = task_spec["label"]
    output_data_info = task_spec["output_data_info"]

    # Load the code from PK
    code = aiida.orm.load_node(task_spec["code_pk"])

    # Load nodes from PKs and initialize structures
    all_nodes = {key: aiida.orm.load_node(pk) for key, pk in task_spec["node_pks"].items()}

    # Add AvailableData nodes passed as parameter, remapping from port names to data labels
    # so they match the placeholders in arguments (which use data labels)
    if input_data_nodes:
        # Build mapping from port names to data labels for AvailableData
        port_to_label = {}
        for input_info in task_spec["input_data_info"]:
            if input_info["is_available"]:
                port_to_label[input_info["port"]] = input_info["label"]

        # Add nodes with data labels as keys (not port names)
        for port_name, node in input_data_nodes.items():
            data_label = port_to_label.get(port_name, port_name)
            all_nodes[data_label] = node
            logger.debug("  Mapped AvailableData port '%s' -> key '%s'", port_name, data_label)

    # Process dependencies if present
    placeholder_to_node_key: dict[str, str] = {}
    filenames: dict[str, str] = {}
    if parent_folders:
        dep_nodes, placeholder_to_node_key, filenames = load_and_process_shell_dependencies(
            parent_folders,
            task_spec.get("port_to_dep_mapping", {}),
            task_spec["filenames"],
            label,
        )
        all_nodes.update(dep_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(label=task_spec["metadata"]["computer_label"])
    metadata = build_shell_metadata_with_slurm_dependencies(task_spec["metadata"], job_ids, computer)

    # Process argument placeholders
    arguments = process_shell_argument_placeholders(task_spec["arguments_template"], placeholder_to_node_key)

    # Use pre-computed outputs
    outputs = task_spec["outputs"]

    logger.debug("Shell '%s' final configuration:", label)
    logger.debug("  arguments = %s", arguments)
    logger.debug("  all_nodes keys = %s", list(all_nodes.keys()))
    logger.debug("  outputs = %s", outputs)
    logger.debug("  filenames = %s", filenames)

    # Build the shelljob NodeSpec
    parser_outputs = [output_info["name"] for output_info in output_data_info if output_info["path"]]

    spec = _build_shelljob_nodespec(
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

    logger.debug("Launcher for '%s' starting...", label)

    # Load RemoteData dependencies (restart files, etc.)
    remote_data_nodes = load_icon_dependencies(
        parent_folders,
        task_spec.get("port_to_dep_mapping", {}),
        task_spec["model_namelist_pks"],
        label,
    )
    input_data_nodes.update(remote_data_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(label=computer_label)
    metadata_dict = build_icon_metadata_with_slurm_dependencies(task_spec["metadata"], job_ids, computer, label)

    # Prepare complete inputs dict for IconTask
    inputs = prepare_icon_task_inputs(task_spec, input_data_nodes, metadata_dict, label)

    # Call IconTask directly (NOT wg.add_task!)
    # This returns a TaskNode with proper output sockets
    return IconTask(**inputs)


def get_task_dependencies_from_workgraph(wg: WorkGraph) -> dict[str, list[str]]:
    """Extract dependency graph from WorkGraph.

    The key insight: launcher tasks are intentionally disconnected to enable pre-submission.
    Dependencies flow through get_job_data tasks. We need to:
    1. Find which get_job_data tasks each launcher depends on
    2. Map those back to the corresponding launcher tasks

    For example:
    - launch_icon_step1 -> get_job_data_step1 (provides job_id)
    - launch_icon_step2 depends on get_job_data_step1.job_id
    - Therefore: launch_icon_step2 depends on launch_icon_step1

    Args:
        wg: The WorkGraph to extract dependencies from

    Returns:
        Dict mapping launcher task_name -> list of parent launcher task names
    """
    deps: dict[str, list[str]] = {}

    # Build mapping: get_job_data_X -> launch_X
    get_job_data_to_launcher: dict[str, str] = {}
    for wg_task in wg.tasks:
        if wg_task.name.startswith("get_job_data_"):
            # Extract the base name (e.g., get_job_data_icon_step1 -> icon_step1)
            base_name = wg_task.name.replace("get_job_data_", "")
            launcher_name = f"launch_{base_name}"
            get_job_data_to_launcher[wg_task.name] = launcher_name

    # For each launcher task, find its dependencies
    for wg_task in wg.tasks:
        task_name = wg_task.name
        if not task_name.startswith("launch_"):
            continue

        deps[task_name] = []

        # Check which get_job_data tasks this launcher depends on
        if hasattr(wg_task.inputs, "_sockets"):
            for socket in wg_task.inputs._sockets.values():  # noqa: SLF001
                if hasattr(socket, "links"):
                    for link in socket.links:
                        parent_task_name = link.from_socket.node.name

                        # If parent is a get_job_data task, map it back to its launcher
                        if parent_task_name.startswith("get_job_data_"):
                            corresponding_launcher = get_job_data_to_launcher.get(parent_task_name)
                            if corresponding_launcher and corresponding_launcher not in deps[task_name]:
                                deps[task_name].append(corresponding_launcher)

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

    wg_name = core_workflow.name or "SIROCCO-WF"
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

            logger.debug("Building dependencies for task '%s'", task_label)

            # Collect AvailableData inputs
            input_data_for_task = collect_available_data_inputs(core_task, aiida_data_nodes, get_label)

            # Build dependency mapping for GeneratedData inputs
            port_to_dep_mapping, parent_folders_for_task, job_ids_for_task = build_dependency_mapping(
                core_task, core_workflow, task_dep_info, get_label
            )

            # Track dependencies for rolling window
            # parent_folders_for_task keys are the task labels this task depends on
            launcher_name = f"launch_{task_label}"
            launcher_dependencies[launcher_name] = [f"launch_{dep_label}" for dep_label in parent_folders_for_task]

            # Create launcher task based on task type
            if isinstance(core_task, core.IconTask):
                task_spec = icon_task_specs[task_label]
                task_dep_info, prev_dep_tasks = create_icon_launcher_task(
                    wg,
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

    logger.debug("WorkGraph build complete:")
    logger.debug(
        "  Total tasks in workflow: %s",
        sum(len(cycle.tasks) for cycle in core_workflow.cycles),
    )
    logger.debug(
        "  Total launcher tasks created: %s",
        len([t for t in wg.tasks if "launch_" in t.name]),
    )
    logger.debug(
        "  Total get_job_data tasks created: %s",
        len([t for t in wg.tasks if "get_job_data_" in t.name]),
    )

    return wg, launcher_dependencies


# =============================================================================
# WorkGraph Builder Helper Functions
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
            logger.debug("  AvailableData port '%s' -> data label '%s'", port, input_label)
    return input_data_for_task


def build_dependency_mapping(
    task: core.Task, core_workflow: core.Workflow, task_dep_info: dict, get_label_func
) -> tuple[dict, dict, dict]:
    """Build dependency mapping for GeneratedData inputs.

    Args:
        task: The task to build dependencies for
        core_workflow: The complete workflow (to find producers)
        task_dep_info: Dict of {task_label: get_job_data outputs} for completed tasks
        get_label_func: Function to get label from graph item

    Returns:
        Tuple of (port_to_dep_mapping, parent_folders_for_task, job_ids_for_task)
    """
    port_to_dep_mapping: dict[str, list[tuple[str, str | None, str]]] = {}
    parent_folders_for_task: dict[str, Any] = {}
    job_ids_for_task: dict[str, Any] = {}

    for port, input_data in task.input_data_items():
        if isinstance(input_data, core.GeneratedData):
            input_data_label = get_label_func(input_data)
            logger.debug(
                "Processing GeneratedData '%s' (input has no path, will get from output)",
                input_data.name,
            )

            # Find producer task - get filename from the OUTPUT specification
            for prev_task in core_workflow.tasks:
                for _, out_data in prev_task.output_data_items():
                    if get_label_func(out_data) == input_data_label:
                        prev_task_label = get_label_func(prev_task)

                        # Get filename from the OUTPUT data specification
                        # Only GeneratedData has a path attribute
                        filename = None
                        out_data_path = None
                        if isinstance(out_data, core.GeneratedData):
                            out_data_path = out_data.path
                            filename = out_data.path.name if out_data.path else None

                        logger.debug(
                            "  Found producer '%s', output path: %s, filename: %s",
                            prev_task_label,
                            out_data_path,
                            filename,
                        )

                        if prev_task_label in task_dep_info:
                            # Add to port mapping (supports multiple dependencies per port)
                            # Store as tuple: (dep_task_label, filename, data_label)
                            if port not in port_to_dep_mapping:
                                port_to_dep_mapping[port] = []
                            port_to_dep_mapping[port].append((prev_task_label, filename, input_data_label))

                            # Add to parent_folders and job_ids (avoid duplicates)
                            if prev_task_label not in parent_folders_for_task:
                                job_data = task_dep_info[prev_task_label]
                                parent_folders_for_task[prev_task_label] = job_data.remote_folder
                                job_ids_for_task[prev_task_label] = job_data.job_id

                            logger.debug(
                                "  GeneratedData port '%s' <- dep '%s' (data: %s, filename: %s)",
                                port,
                                prev_task_label,
                                input_data.name,
                                filename,
                            )
                        break

    return port_to_dep_mapping, parent_folders_for_task, job_ids_for_task


def create_icon_launcher_task(
    wg: WorkGraph,
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
    launcher_name = f"launch_{task_label}"

    # Add port_to_dep_mapping to task_spec
    task_spec["port_to_dep_mapping"] = port_to_dep_mapping

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
    )

    # Store the outputs namespace for dependent tasks
    task_dep_info[task_label] = dep_task.outputs

    # Chain with previous dependency tasks using >>
    for dep_label in parent_folders_for_task:
        if dep_label in prev_dep_tasks:
            prev_dep_tasks[dep_label] >> dep_task
            logger.debug("Chaining %s >> %s", dep_label, task_label)

    # Store for next iteration
    prev_dep_tasks[task_label] = dep_task

    logger.debug(
        "Created ICON launcher '%s' with %s parent_folders, %s job_ids",
        launcher_name,
        len(parent_folders_for_task),
        len(job_ids_for_task),
    )

    return task_dep_info, prev_dep_tasks


def create_shell_launcher_task(
    wg: WorkGraph,
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
    launcher_name = f"launch_{task_label}"

    # Add port_to_dep_mapping to task_spec
    task_spec["port_to_dep_mapping"] = port_to_dep_mapping

    # Create launcher task
    wg.add_task(
        launch_shell_task_with_dependency,
        name=launcher_name,
        task_spec=task_spec,
        input_data_nodes=input_data_for_task if input_data_for_task else None,
        parent_folders=parent_folders_for_task if parent_folders_for_task else None,
        job_ids=job_ids_for_task if job_ids_for_task else None,
    )

    logger.debug(
        "Created shell launcher '%s' with %s parent_folders",
        launcher_name,
        len(parent_folders_for_task),
    )

    # Create get_job_data task
    # breakpoint()
    dep_task = wg.add_task(
        get_job_data,
        name=f"get_job_data_{task_label}",
        workgraph_name=launcher_name,
        task_name=task_label,
    )

    # Store the outputs namespace for dependent tasks
    task_dep_info[task_label] = dep_task.outputs

    # Chain with previous dependency tasks
    for dep_label in parent_folders_for_task:
        if dep_label in prev_dep_tasks:
            prev_dep_tasks[dep_label] >> dep_task

    # Store for next iteration
    prev_dep_tasks[task_label] = dep_task

    logger.debug(
        "Created Shell launcher '%s' with %s parent_folders, %s job_ids",
        launcher_name,
        len(parent_folders_for_task),
        len(job_ids_for_task),
    )

    return task_dep_info, prev_dep_tasks


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


def split_cmd_arg(command_line: str) -> tuple[str, str]:
    """Split command line into command and arguments."""
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
        f"{obj.name}" + "__".join(f"_{key}_{value}" for key, value in obj.coordinates.items())
    )


def label_placeholder(data: core.Data) -> str:
    """Create a placeholder string for data."""
    return f"{{{get_aiida_label_from_graph_item(data)}}}"


def get_default_wrapper_script() -> aiida.orm.SinglefileData | None:
    """Get default wrapper script based on task type"""
    # Import the script directory from aiida-icon
    from aiida_icon.site_support.cscs.alps import SCRIPT_DIR

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


# =============================================================================
# ICON Task Helper Functions
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
        logger.debug("  Using aiida-icon determined restart file: %s", specific_file_path)
    except Exception as e:  # noqa: BLE001
        logger.debug("  Failed to determine restart filename using aiida-icon: %s", e)
        # Fallback: use the workdir itself
        logger.debug("  Falling back to workdir RemoteData (no specific filename)")
        return workdir_remote_data
    else:
        return file_remote_data


def load_icon_dependencies(
    parent_folders: dict | None,
    port_to_dep_mapping: dict,
    model_namelist_pks: dict,
    label: str,
) -> dict:
    """Load RemoteData dependencies for ICON task and map to input ports.

    Args:
        parent_folders: Dict of {dep_label: remote_folder_pk_tagged_value}
        port_to_dep_mapping: Dict mapping port names to list of (dep_label, filename, data_label) tuples
        model_namelist_pks: Dict of model names to namelist PKs (for restart file resolution)
        label: Task label for debug output

    Returns:
        Dict mapping port names to RemoteData nodes
    """
    input_data_nodes: dict[str, aiida.orm.RemoteData] = {}

    if not parent_folders:
        return input_data_nodes

    # Load RemoteData nodes from their PKs
    parent_folders_loaded = {key: aiida.orm.load_node(val.value) for key, val in parent_folders.items()}

    logger.debug("'%s' loading RemoteData from PKs...", label)
    logger.debug("  port_to_dep_mapping = %s", port_to_dep_mapping)
    logger.debug("  parent_folders_loaded keys = %s", list(parent_folders_loaded.keys()))

    for port_name, dep_info_list in port_to_dep_mapping.items():
        # dep_info_list is a list of (dep_task_label, filename, data_label) tuples
        # For ICON restart_file, should only be one dependency
        if dep_info_list:
            dep_label, filename, data_label = dep_info_list[0]  # Unpack (data_label unused for ICON)
            if dep_label in parent_folders_loaded:
                workdir_remote_data = parent_folders_loaded[dep_label]
                workdir_path = workdir_remote_data.get_remote_path()

                logger.debug("Processing port '%s' from dep '%s'", port_name, dep_label)
                logger.debug("  Workdir RemoteData path: %s", workdir_path)
                logger.debug("  Filename from config: %s", filename)

                if filename:
                    # Create RemoteData pointing to the specific file inside the workdir
                    specific_file_path = f"{workdir_path}/{filename}"
                    file_remote_data = aiida.orm.RemoteData(
                        computer=workdir_remote_data.computer,
                        remote_path=specific_file_path,
                    )
                    input_data_nodes[port_name] = file_remote_data
                    logger.debug("  Created RemoteData for specific file: %s", specific_file_path)
                else:
                    # No specific filename, use aiida-icon to determine restart filename
                    model_namelist_pk = model_namelist_pks.get("atm")
                    if model_namelist_pk:
                        model_namelist_node: aiida.orm.SinglefileData = aiida.orm.load_node(model_namelist_pk)  # type: ignore[assignment]
                        input_data_nodes[port_name] = resolve_icon_restart_file(  # type: ignore[arg-type]
                            workdir_path,
                            model_namelist_node,
                            workdir_remote_data,  # type: ignore[arg-type]
                        )
                    else:
                        # No model namelist, fall back to workdir
                        input_data_nodes[port_name] = workdir_remote_data  # type: ignore[assignment]
                        logger.debug("  No model namelist for restart resolution, using workdir")

    return input_data_nodes


def build_icon_metadata_with_slurm_dependencies(
    base_metadata: dict, job_ids: dict | None, computer: aiida.orm.Computer, label: str
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
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"
        logger.debug("Task %s - Setting SLURM dependency: %s", label, custom_cmd)

        current_cmds = metadata["options"].get("custom_scheduler_commands", "")
        if current_cmds:
            metadata["options"]["custom_scheduler_commands"] = current_cmds + f"\n{custom_cmd}"
        else:
            metadata["options"]["custom_scheduler_commands"] = custom_cmd

        logger.debug(
            "Task %s - custom_scheduler_commands = %s",
            label,
            metadata["options"]["custom_scheduler_commands"],
        )

    return {
        "computer": computer,
        "options": metadata["options"],
        "call_link_label": label,
    }


def prepare_icon_task_inputs(task_spec: dict, input_data_nodes: dict, metadata_dict: dict, label: str) -> dict:
    """Prepare complete inputs dict for ICON task.

    Args:
        task_spec: Task specification containing code, namelists, wrapper script PKs
        input_data_nodes: Dict of input data nodes (both AvailableData and RemoteData)
        metadata_dict: Metadata dict with computer and options
        label: Task label for debug output

    Returns:
        Complete inputs dict for IconTask
    """
    # Start with code and namelists
    inputs = {
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
    logger.debug("Setting ICON inputs for '%s':", label)
    for port_name, data_node in input_data_nodes.items():
        node_type = type(data_node).__name__
        if hasattr(data_node, "get_remote_path"):
            remote_path = data_node.get_remote_path()
            logger.debug(
                "  %s = %s (path: %s, pk: %s)",
                port_name,
                node_type,
                remote_path,
                data_node.pk,
            )
        else:
            logger.debug("  %s = %s (pk: %s)", port_name, node_type, data_node.pk)
        inputs[port_name] = data_node

    # Add metadata
    inputs["metadata"] = metadata_dict  # type: ignore[assignment]

    return inputs


# =============================================================================
# Shell Task Helper Functions
# =============================================================================


def load_and_process_shell_dependencies(
    parent_folders: dict,
    port_to_dep_mapping: dict,
    original_filenames: dict,
    label: str,
) -> tuple[dict, dict, dict]:
    """Load RemoteData dependencies and build node/placeholder/filename mappings.

    Args:
        parent_folders: Dict of {dep_label: remote_folder_pk_tagged_value}
        port_to_dep_mapping: Dict mapping port names to list of (dep_label, filename, data_label) tuples
        original_filenames: Dict mapping data labels to filenames
        label: Task label for debug output

    Returns:
        Tuple of (all_nodes, placeholder_to_node_key, filenames) dicts
    """
    all_nodes: dict[str, aiida.orm.RemoteData] = {}
    placeholder_to_node_key: dict[str, str] = {}
    filenames: dict[str, str] = {}

    # Load RemoteData nodes from their PKs
    parent_folders_loaded: dict[str, Any] = {key: aiida.orm.load_node(val.value) for key, val in parent_folders.items()}

    logger.debug("Shell '%s' loading RemoteData from PKs...", label)
    logger.debug("  port_to_dep_mapping = %s", port_to_dep_mapping)
    logger.debug("  parent_folders_loaded keys = %s", list(parent_folders_loaded.keys()))

    # Process ALL dependencies: create nodes, map placeholders, and map filenames
    for port_name, dep_info_list in port_to_dep_mapping.items():
        # dep_info_list is a list of (dep_task_label, filename, data_label) tuples
        for dep_label, filename, data_label in dep_info_list:
            if dep_label in parent_folders_loaded:
                workdir_remote_data = parent_folders_loaded[dep_label]
                workdir_path = workdir_remote_data.get_remote_path()

                logger.debug("Processing dep '%s' for port '%s'", dep_label, port_name)
                logger.debug("  Workdir path: %s", workdir_path)
                logger.debug("  Filename from config: %s", filename)

                # Create unique key for this dependency
                unique_key = f"{dep_label}_remote"

                # Add RemoteData node directly to all_nodes
                if filename:
                    # Create RemoteData pointing to the specific file
                    specific_file_path = f"{workdir_path}/{filename}"
                    all_nodes[unique_key] = aiida.orm.RemoteData(
                        computer=workdir_remote_data.computer,
                        remote_path=specific_file_path,
                    )
                    logger.debug(
                        "  Added RemoteData '%s' for specific file: %s",
                        unique_key,
                        specific_file_path,
                    )
                else:
                    # No specific filename, use the workdir itself
                    all_nodes[unique_key] = workdir_remote_data  # type: ignore[assignment]
                    logger.debug(
                        "  Added RemoteData '%s' for workdir: %s",
                        unique_key,
                        workdir_path,
                    )

                # Build placeholder mapping for arguments
                placeholder_to_node_key[data_label] = unique_key
                logger.debug("  Mapped placeholder '%s' -> node key '%s'", data_label, unique_key)

                # Build filename mapping (from original_filenames via data_label)
                if data_label in original_filenames:
                    filenames[unique_key] = original_filenames[data_label]
                    logger.debug(
                        "  Mapped filename '%s' -> '%s': '%s'",
                        data_label,
                        unique_key,
                        original_filenames[data_label],
                    )

    return all_nodes, placeholder_to_node_key, filenames


def build_shell_metadata_with_slurm_dependencies(
    base_metadata: dict, job_ids: dict | None, computer: aiida.orm.Computer
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
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"

        if "custom_scheduler_commands" in metadata["options"]:
            metadata["options"]["custom_scheduler_commands"] += f"\n{custom_cmd}"
        else:
            metadata["options"]["custom_scheduler_commands"] = custom_cmd

        logger.debug(
            "Task %s - Setting SLURM dependency: %s",
            base_metadata.get("label", "unknown"),
            custom_cmd,
        )
        logger.debug(
            "Task %s - custom_scheduler_commands = %s",
            base_metadata.get("label", "unknown"),
            metadata["options"]["custom_scheduler_commands"],
        )

    return metadata


def process_shell_argument_placeholders(arguments_template: str | None, placeholder_to_node_key: dict) -> list[str]:
    """Process argument template and replace placeholders with actual node keys.

    Args:
        arguments_template: Template string with {placeholder} syntax
        placeholder_to_node_key: Dict mapping placeholder names to node keys

    Returns:
        List of processed arguments with placeholders replaced
    """
    if not arguments_template:
        return []

    arguments_list = arguments_template.split()
    processed_arguments = []

    for arg in arguments_list:
        # Check if this argument is a placeholder
        if arg.startswith("{") and arg.endswith("}"):
            placeholder_name = arg[1:-1]  # Remove the braces
            # Map to the actual node key if we have a mapping
            if placeholder_name in placeholder_to_node_key:
                actual_node_key = placeholder_to_node_key[placeholder_name]
                processed_arguments.append(f"{{{actual_node_key}}}")
                logger.debug("  Mapped argument placeholder '%s' -> '{%s}'", arg, actual_node_key)
            else:
                # Keep original if no mapping found
                processed_arguments.append(arg)
                logger.debug("  No mapping found for placeholder '%s', keeping original", arg)
        else:
            processed_arguments.append(arg)

    return processed_arguments


# =============================================================================
# Instance Method Conversions - Task Spec Building
# =============================================================================


def validate_workflow(core_workflow: core.Workflow):
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


def add_aiida_input_data_node(data: core.AvailableData, core_workflow: core.Workflow, aiida_data_nodes: dict) -> None:
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
        isinstance(task, core.IconTask) and data in task.input_data_nodes() for task in core_workflow.tasks
    )

    if used_by_icon_task:
        # ICON tasks require RemoteData
        aiida_data_nodes[label] = aiida.orm.RemoteData(remote_path=str(data.path), label=label, computer=computer)
    elif computer.get_transport_class() is aiida.transports.plugins.local.LocalTransport:
        if data.path.is_file():
            aiida_data_nodes[label] = aiida.orm.SinglefileData(file=str(data.path), label=label)
        else:
            aiida_data_nodes[label] = aiida.orm.FolderData(tree=str(data.path), label=label)
    else:
        aiida_data_nodes[label] = aiida.orm.RemoteData(remote_path=str(data.path), label=label, computer=computer)


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

    # custom_scheduler_commands - initialize if not already set
    if "custom_scheduler_commands" not in options:
        options["custom_scheduler_commands"] = ""

    if isinstance(task, core.IconTask) and task.uenv is not None:
        if options["custom_scheduler_commands"]:
            options["custom_scheduler_commands"] += "\n"
        options["custom_scheduler_commands"] += f"#SBATCH --uenv={task.uenv}"
    if isinstance(task, core.IconTask) and task.view is not None:
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

    try:
        computer = aiida.orm.Computer.collection.get(label=task.computer)
        metadata["computer_label"] = computer.label
    except NotExistent as err:
        msg = f"Could not find computer {task.computer!r} in AiiDA database."
        raise ValueError(msg) from err

    return metadata


def build_shell_task_spec(task: core.ShellTask) -> dict:
    """Build all parameters needed to create a shell task.

    Returns a dict with keys: label, code, nodes, metadata,
    arguments_template, filenames, outputs, input_data_info, output_data_info

    Note: Job dependencies are NOT included here - they're added at runtime.

    Args:
        task: The ShellTask to build spec for

    Returns:
        Dict containing all shell task parameters
    """
    from aiida_shell import ShellCode

    label = get_aiida_label_from_graph_item(task)
    cmd, _ = split_cmd_arg(task.command)

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

    # Build nodes (input files like scripts) - store as PKs
    node_pks = {}
    if task.path is not None:
        script_node = aiida.orm.SinglefileData(str(task.path))
        script_node.store()
        node_pks[f"SCRIPT__{label}"] = script_node.pk

    # Create or load code
    code_label = f"{cmd}"
    try:
        code = aiida.orm.load_code(f"code_label@{computer.label}")
    except NotExistent:
        code = ShellCode(  # type: ignore[assignment]
            label=code_label,
            computer=computer,
            filepath_executable=cmd,
            default_calc_job_plugin="core.shell",
            use_double_quotes=True,
        ).store()

    # Pre-compute input data information (as serializable dicts)
    input_data_info = []
    for port_name, input_ in task.input_data_items():
        input_info = {
            "port": port_name,
            "name": input_.name,
            "coordinates": serialize_coordinates(input_.coordinates),
            "label": get_aiida_label_from_graph_item(input_),
            "is_available": isinstance(input_, core.AvailableData),
            "is_generated": isinstance(input_, core.GeneratedData),
            "path": str(input_.path) if input_.path is not None else "",  # type: ignore[attr-defined]
        }
        input_data_info.append(input_info)

    # Pre-compute output data information
    output_data_info = []
    for output in task.output_data_nodes():
        output_info = {
            "name": output.name,
            "coordinates": serialize_coordinates(output.coordinates),
            "label": get_aiida_label_from_graph_item(output),
            "is_generated": isinstance(output, core.GeneratedData),
            "path": str(output.path) if output.path is not None else "",  # type: ignore[attr-defined]
        }
        output_data_info.append(output_info)

    # Build input labels for argument resolution
    input_labels: dict[str, list[str]] = {}
    for input_info in input_data_info:
        port_name = input_info["port"]  # type: ignore[assignment]
        input_label = input_info["label"]
        if port_name not in input_labels:
            input_labels[port_name] = []
        input_labels[port_name].append(f"{{{input_label}}}")

    # Pre-scan command template to find all referenced ports
    # This ensures optional/missing ports are included with empty lists
    for port_match in task.port_pattern.finditer(task.command):
        port_name = port_match.group(2)
        if port_name and port_name not in input_labels:
            input_labels[port_name] = []

    # Pre-resolve arguments template
    arguments_with_placeholders = task.resolve_ports(input_labels)
    _, resolved_arguments_template = split_cmd_arg(arguments_with_placeholders)

    # Build filenames mapping
    filenames = {}
    for input_info in input_data_info:
        input_label = input_info["label"]
        if input_info["is_available"]:
            from pathlib import Path

            filenames[input_info["name"]] = Path(input_info["path"]).name if input_info["path"] else input_info["name"]  # type: ignore[arg-type]
        elif input_info["is_generated"]:
            # Count how many inputs have the same name
            same_name_count = sum(1 for info in input_data_info if info["name"] == input_info["name"])
            if same_name_count > 1:
                filenames[input_label] = input_label
            else:
                from pathlib import Path

                filenames[input_label] = Path(input_info["path"]).name if input_info["path"] else input_info["name"]  # type: ignore[arg-type]

    # Build outputs list
    outputs = [
        output_info["path"]
        for output_info in output_data_info
        if output_info["is_generated"] and output_info["path"] is not None
    ]

    # Build output port mapping: data_name -> shell output link_label

    output_port_mapping = {}
    for output_info in output_data_info:
        if output_info["path"]:
            link_label = ShellParser.format_link_label(output_info["path"])  # type: ignore[arg-type]
            output_port_mapping[output_info["name"]] = link_label

    return {
        "label": label,
        "code_pk": code.pk,
        "node_pks": node_pks,
        "metadata": metadata,
        "arguments_template": resolved_arguments_template,
        "filenames": filenames,
        "outputs": outputs,
        "input_data_info": input_data_info,
        "output_data_info": output_data_info,
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

    # Create or load ICON code
    icon_code_label = "icon"
    try:
        icon_code = aiida.orm.load_code(f"icon_code_label@{computer.label}")
    except NotExistent:
        icon_code = aiida.orm.InstalledCode(
            label=icon_code_label,
            description="aiida_icon",
            default_calc_job_plugin="icon.icon",
            computer=computer,
            filepath_executable=str(task.bin),
            with_mpi=bool(task.mpi_cmd),
            use_double_quotes=True,
        )
        _ = icon_code.store()

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
            model_node = create_namelist_singlefiledata_from_content(content, model_nml.name, store=True)
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
# Public API - Main Entry Point Functions
# =============================================================================


def build_sirocco_workgraph(
    core_workflow: core.Workflow,
    window_size: int = 1,
    max_queued_jobs: int | None = None,
) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows functionally.

    Args:
        core_workflow: The core workflow to convert
        window_size: Number of topological fronts to keep active (default: 1)
                    0 = sequential (wait for level N to finish before submitting N+1)
                    1 = one front ahead (default)
                    high value = streaming submission
        max_queued_jobs: Maximum number of jobs in CREATED/RUNNING state (optional)

    Returns:
        A WorkGraph ready for submission

    Example::

        from sirocco import core
        from sirocco.workgraph import build_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Build the WorkGraph with window_size=2
        wg = build_sirocco_workgraph(wf, window_size=2)

        # Submit to AiiDA daemon
        wg.submit()
    """
    # Validate workflow
    validate_workflow(core_workflow)

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

    # Compute topological levels for rolling window submission
    # Use the dependencies tracked during workgraph building
    task_levels = compute_topological_levels(launcher_dependencies)

    # Store window configuration in WorkGraph extras
    # This is now properly serialized/deserialized by aiida-workgraph
    # (requires the extras serialization changes in workgraph.py)
    window_config = {
        "enabled": window_size >= 0,  # Enable window for any non-negative window_size
        "window_size": window_size,
        "max_queued_jobs": max_queued_jobs,  # Optional hard limit on concurrent jobs
        "task_levels": task_levels,  # Topological levels for each launcher task
    }

    wg.extras = {"window_config": window_config}

    # Print dependency and level information
    if launcher_dependencies:
        logger.debug("Task dependencies tracked:")
        for task_name, parents in sorted(launcher_dependencies.items()):
            if parents:
                logger.debug("  %s depends on: %s", task_name, ", ".join(parents))
            else:
                logger.debug("  %s (no dependencies)", task_name)

    if task_levels:
        max_level = max(task_levels.values()) if task_levels else 0
        logger.debug("Topological levels computed:")
        logger.debug("  Total levels (fronts): %s", max_level + 1)
        logger.debug("  Window size: %s fronts", window_size)
        max_levels_to_show = 5
        for level in range(min(max_level + 1, max_levels_to_show)):
            tasks_at_level = [name for name, level_num in task_levels.items() if level_num == level]
            logger.debug(
                "  Level %s: %s tasks - %s",
                level,
                len(tasks_at_level),
                ", ".join(tasks_at_level),
            )
        if max_level >= max_levels_to_show:
            logger.debug("  ... (levels %s-%s)", max_levels_to_show, max_level)

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
