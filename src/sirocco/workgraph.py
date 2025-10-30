from __future__ import annotations

import asyncio
import io
import time
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, TypeAlias, assert_never

import aiida.common
import aiida.orm
import aiida.transports
import aiida.transports.plugins.local
import yaml
from aiida.common.exceptions import NotExistent
from aiida.orm.utils.serialize import AiiDALoader
from aiida_icon.calculations import IconCalculation
from aiida_shell.parsers.shell import ShellParser
from aiida_workgraph import WorkGraph, dynamic, task, get_current_graph, namespace

from sirocco import core
from sirocco.parsing._utils import TimeUtils

if TYPE_CHECKING:
    WorkgraphDataNode: TypeAlias = (
        aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData
    )


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

    print(f"DEBUG: Starting job_data polling for {task_name} in {workgraph_name}")

    while True:
        # Query for WorkGraphs created AFTER this task started polling
        builder = orm.QueryBuilder()
        builder.append(
            WorkGraphEngine,
            filters={
                "attributes.process_label": {"==": f"WorkGraph<{workgraph_name}>"},
                "ctime": {">": datetime.fromtimestamp(start_time_unix - 10)},
            },
            tag="process",
        )

        if builder.count() > 0:
            workgraph_node = builder.all()[-1][0]
            node_data = workgraph_node.task_processes.get(task_name, "")
            if node_data:
                node = yaml.load(node_data, Loader=AiiDALoader)
                if node:
                    job_id = node.get_job_id()
                    if job_id is not None:
                        # When job_id is ready, remote_folder is also available
                        print(
                            f"DEBUG: Retrieved job_id={job_id}, remote_folder_pk={node.outputs.remote_folder.pk} for {task_name}"
                        )
                        return {
                            "job_id": int(job_id),
                            "remote_folder": node.outputs.remote_folder.pk,
                        }

        # Check timeout
        elapsed = time.time() - start_time_unix
        if elapsed > timeout:
            raise TimeoutError(
                f"Timeout waiting for job_id for task {task_name} after {timeout}s"
            )

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
    input_data_info = task_spec["input_data_info"]
    output_data_info = task_spec["output_data_info"]

    # Load the code from PK
    code = aiida.orm.load_node(task_spec["code_pk"])

    # Load nodes from PKs and initialize structures
    all_nodes = {
        key: aiida.orm.load_node(pk) for key, pk in task_spec["node_pks"].items()
    }

    # Add AvailableData nodes passed as parameter
    if input_data_nodes:
        all_nodes.update(input_data_nodes)

    # Process dependencies if present
    placeholder_to_node_key = {}
    filenames = {}
    if parent_folders:
        dep_nodes, placeholder_to_node_key, filenames = load_and_process_shell_dependencies(
            parent_folders,
            task_spec.get("port_to_dep_mapping", {}),
            task_spec["filenames"],
            label
        )
        all_nodes.update(dep_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(label=task_spec["metadata"]["computer_label"])
    metadata = build_shell_metadata_with_slurm_dependencies(
        task_spec["metadata"],
        job_ids,
        computer
    )

    # Process argument placeholders
    arguments = process_shell_argument_placeholders(
        task_spec["arguments_template"],
        placeholder_to_node_key
    )

    # Use pre-computed outputs
    outputs = task_spec["outputs"]

    print(f"DEBUG: Shell '{label}' final configuration:")
    print(f"DEBUG:   arguments = {arguments}")
    print(f"DEBUG:   all_nodes keys = {list(all_nodes.keys())}")
    print(f"DEBUG:   outputs = {outputs}")
    print(f"DEBUG:   filenames = {filenames}")

    # Build the shelljob NodeSpec
    parser_outputs = [
        output_info["name"] for output_info in output_data_info if output_info["path"]
    ]

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
    job_ids: Annotated[dict, dynamic(int)] = None,
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

    print(f"DEBUG: Launcher for '{label}' starting...")

    # Load RemoteData dependencies (restart files, etc.)
    remote_data_nodes = load_icon_dependencies(
        parent_folders,
        task_spec.get("port_to_dep_mapping", {}),
        task_spec["model_namelist_pks"],
        label
    )
    input_data_nodes.update(remote_data_nodes)

    # Build metadata with SLURM job dependencies
    computer = aiida.orm.Computer.collection.get(label=computer_label)
    metadata_dict = build_icon_metadata_with_slurm_dependencies(
        task_spec["metadata"],
        job_ids,
        computer,
        label
    )

    # Prepare complete inputs dict for IconTask
    inputs = prepare_icon_task_inputs(
        task_spec,
        input_data_nodes,
        metadata_dict,
        label
    )

    # Call IconTask directly (NOT wg.add_task!)
    # This returns a TaskNode with proper output sockets
    icon_task = IconTask(**inputs)

    # Return the TaskNode (which has .outputs as sockets)
    return icon_task


def build_dynamic_sirocco_workgraph(
    core_workflow: core.Workflow,
    aiida_data_nodes: dict,
    shell_task_specs: dict,
    icon_task_specs: dict,
):
    from aiida_workgraph.manager import set_current_graph

    wg = WorkGraph("FULL-WG")
    set_current_graph(wg)

    # Store get_job_data task outputs (namespace with job_id, remote_folder)
    task_dep_info = {}  # {task_label: get_job_data task outputs}
    prev_dep_tasks = {}  # {task_label: get_job_data task} for chaining with >>

    # Helper to get task label
    def get_label(task):
        return get_aiida_label_from_graph_item(task)

    # Process all tasks in the workflow in cycle order
    for cycle in core_workflow.cycles:
        for task in cycle.tasks:
            task_label = get_label(task)

            print(f"DEBUG: Building dependencies for task '{task_label}'")

            # Collect AvailableData inputs
            input_data_for_task = collect_available_data_inputs(
                task, aiida_data_nodes, get_label
            )

            # Build dependency mapping for GeneratedData inputs
            port_to_dep_mapping, parent_folders_for_task, job_ids_for_task = (
                build_dependency_mapping(task, core_workflow, task_dep_info, get_label)
            )

            # Create launcher task based on task type
            if isinstance(task, core.IconTask):
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

            elif isinstance(task, core.ShellTask):
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
                raise TypeError(f"Unknown task type: {type(task)}")

    print(f"\nDEBUG: WorkGraph build complete:")
    print(
        f"  Total tasks in workflow: {sum(len(cycle.tasks) for cycle in core_workflow.cycles)}"
    )
    print(
        f"  Total launcher tasks created: {len([t for t in wg.tasks if 'launch_' in t.name])}"
    )
    print(
        f"  Total get_job_data tasks created: {len([t for t in wg.tasks if 'get_job_data_' in t.name])}"
    )

    return wg


# =============================================================================
# WorkGraph Builder Helper Functions
# =============================================================================


def collect_available_data_inputs(
    task: core.Task,
    aiida_data_nodes: dict,
    get_label_func
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
            print(f"DEBUG:   AvailableData '{port}' -> {input_label}")
    return input_data_for_task


def build_dependency_mapping(
    task: core.Task,
    core_workflow: core.Workflow,
    task_dep_info: dict,
    get_label_func
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
    port_to_dep_mapping = {}  # {port_name: [(dep_task_label, filename, data_label), ...]}
    parent_folders_for_task = {}  # {dep_label: remote_folder_pk}
    job_ids_for_task = {}  # {dep_label: job_id}

    for port, input_data in task.input_data_items():
        if isinstance(input_data, core.GeneratedData):
            input_data_label = get_label_func(input_data)
            print(
                f"DEBUG:   Processing GeneratedData '{input_data.name}' (input has no path, will get from output)"
            )

            # Find producer task - get filename from the OUTPUT specification
            for prev_task in core_workflow.tasks:
                for out_port, out_data in prev_task.output_data_items():
                    if get_label_func(out_data) == input_data_label:
                        prev_task_label = get_label_func(prev_task)

                        # Get filename from the OUTPUT data specification
                        filename = out_data.path.name if out_data.path else None
                        print(
                            f"DEBUG:   Found producer '{prev_task_label}', output path: {out_data.path}, filename: {filename}"
                        )

                        if prev_task_label in task_dep_info:
                            # Add to port mapping (supports multiple dependencies per port)
                            # Store as tuple: (dep_task_label, filename, data_label)
                            if port not in port_to_dep_mapping:
                                port_to_dep_mapping[port] = []
                            port_to_dep_mapping[port].append(
                                (prev_task_label, filename, input_data_label)
                            )

                            # Add to parent_folders and job_ids (avoid duplicates)
                            if prev_task_label not in parent_folders_for_task:
                                job_data = task_dep_info[prev_task_label]
                                parent_folders_for_task[prev_task_label] = (
                                    job_data.remote_folder
                                )
                                job_ids_for_task[prev_task_label] = (
                                    job_data.job_id
                                )

                            print(
                                f"DEBUG:   GeneratedData port '{port}' <- dep '{prev_task_label}' (data: {input_data.name}, filename: {filename})"
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
    prev_dep_tasks: dict
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
    icon_launcher = wg.add_task(
        launch_icon_task_with_dependency,
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
    )

    # Store the outputs namespace for dependent tasks
    task_dep_info[task_label] = dep_task.outputs

    # Chain with previous dependency tasks using >>
    for dep_label in parent_folders_for_task.keys():
        if dep_label in prev_dep_tasks:
            prev_dep_tasks[dep_label] >> dep_task
            print(f"DEBUG: Chaining {dep_label} >> {task_label}")

    # Store for next iteration
    prev_dep_tasks[task_label] = dep_task

    print(
        f"DEBUG: Created ICON launcher '{launcher_name}' with {len(parent_folders_for_task)} parent_folders, {len(job_ids_for_task)} job_ids"
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
    prev_dep_tasks: dict
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
    shell_launcher = wg.add_task(
        launch_shell_task_with_dependency,
        name=launcher_name,
        task_spec=task_spec,
        input_data_nodes=input_data_for_task if input_data_for_task else None,
        parent_folders=parent_folders_for_task if parent_folders_for_task else None,
        job_ids=job_ids_for_task if job_ids_for_task else None,
    )

    print(
        f"DEBUG: Created shell launcher '{launcher_name}' with {len(parent_folders_for_task)} parent_folders"
    )

    # Create get_job_data task
    dep_task = wg.add_task(
        get_job_data,
        name=f"get_job_data_{task_label}",
        workgraph_name=launcher_name,
        task_name=task_label,
    )

    # Store the outputs namespace for dependent tasks
    task_dep_info[task_label] = dep_task.outputs

    # Chain with previous dependency tasks
    for dep_label in parent_folders_for_task.keys():
        if dep_label in prev_dep_tasks:
            prev_dep_tasks[dep_label] >> dep_task

    # Store for next iteration
    prev_dep_tasks[task_label] = dep_task

    print(
        f"DEBUG: Created Shell launcher '{launcher_name}' with {len(parent_folders_for_task)} parent_folders, {len(job_ids_for_task)} job_ids"
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
        f"{obj.name}"
        + "__".join(f"_{key}_{value}" for key, value in obj.coordinates.items())
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
    workdir_remote_data: aiida.orm.RemoteData
) -> aiida.orm.RemoteData:
    """Resolve ICON restart file path using aiida-icon utilities.

    Args:
        workdir_path: Path to remote working directory
        model_namelist_node: AiiDA node containing the model namelist
        workdir_remote_data: RemoteData for the workdir (fallback)

    Returns:
        RemoteData pointing to the restart file (or workdir if resolution fails)
    """
    from aiida_icon.iconutils.modelnml import read_latest_restart_file_link_name
    import f90nml

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
        print(f"DEBUG:   Using aiida-icon determined restart file: {specific_file_path}")
        return file_remote_data

    except Exception as e:
        print(f"DEBUG:   Failed to determine restart filename using aiida-icon: {e}")
        # Fallback: use the workdir itself
        print(f"DEBUG:   Falling back to workdir RemoteData (no specific filename)")
        return workdir_remote_data


def load_icon_dependencies(
    parent_folders: dict | None,
    port_to_dep_mapping: dict,
    model_namelist_pks: dict,
    label: str
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
    input_data_nodes = {}

    if not parent_folders:
        return input_data_nodes

    # Load RemoteData nodes from their PKs
    parent_folders_loaded = {
        key: aiida.orm.load_node(val.value)
        for key, val in parent_folders.items()
    }

    print(f"DEBUG: '{label}' loading RemoteData from PKs...")
    print(f"DEBUG:   port_to_dep_mapping = {port_to_dep_mapping}")
    print(f"DEBUG:   parent_folders_loaded keys = {list(parent_folders_loaded.keys())}")

    for port_name, dep_info_list in port_to_dep_mapping.items():
        # dep_info_list is a list of (dep_task_label, filename, data_label) tuples
        # For ICON restart_file, should only be one dependency
        if dep_info_list:
            dep_label, filename, data_label = dep_info_list[0]  # Unpack (data_label unused for ICON)
            if dep_label in parent_folders_loaded:
                workdir_remote_data = parent_folders_loaded[dep_label]
                workdir_path = workdir_remote_data.get_remote_path()

                print(f"DEBUG: Processing port '{port_name}' from dep '{dep_label}'")
                print(f"DEBUG:   Workdir RemoteData path: {workdir_path}")
                print(f"DEBUG:   Filename from config: {filename}")

                if filename:
                    # Create RemoteData pointing to the specific file inside the workdir
                    specific_file_path = f"{workdir_path}/{filename}"
                    file_remote_data = aiida.orm.RemoteData(
                        computer=workdir_remote_data.computer,
                        remote_path=specific_file_path,
                    )
                    input_data_nodes[port_name] = file_remote_data
                    print(f"DEBUG:   Created RemoteData for specific file: {specific_file_path}")
                else:
                    # No specific filename, use aiida-icon to determine restart filename
                    model_namelist_pk = model_namelist_pks.get("atm")
                    if model_namelist_pk:
                        model_namelist_node = aiida.orm.load_node(model_namelist_pk)
                        input_data_nodes[port_name] = resolve_icon_restart_file(
                            workdir_path,
                            model_namelist_node,
                            workdir_remote_data
                        )
                    else:
                        # No model namelist, fall back to workdir
                        input_data_nodes[port_name] = workdir_remote_data
                        print(f"DEBUG:   No model namelist for restart resolution, using workdir")

    return input_data_nodes


def build_icon_metadata_with_slurm_dependencies(
    base_metadata: dict,
    job_ids: dict | None,
    computer: aiida.orm.Computer,
    label: str
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
        print(f"DEBUG: Task {label} - Setting SLURM dependency: {custom_cmd}")

        current_cmds = metadata["options"].get("custom_scheduler_commands", "")
        if current_cmds:
            metadata["options"]["custom_scheduler_commands"] = current_cmds + f"\n{custom_cmd}"
        else:
            metadata["options"]["custom_scheduler_commands"] = custom_cmd

        print(f"DEBUG: Task {label} - custom_scheduler_commands = {metadata['options']['custom_scheduler_commands']}")

    return {
        "computer": computer,
        "options": metadata["options"],
        "call_link_label": label,
    }


def prepare_icon_task_inputs(
    task_spec: dict,
    input_data_nodes: dict,
    metadata_dict: dict,
    label: str
) -> dict:
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
        inputs["models"] = models

    # Add wrapper script if present
    if task_spec["wrapper_script_pk"] is not None:
        inputs["wrapper_script"] = aiida.orm.load_node(task_spec["wrapper_script_pk"])

    # Add ALL input data nodes (both AvailableData and RemoteData for GeneratedData)
    print(f"DEBUG: Setting ICON inputs for '{label}':")
    for port_name, data_node in input_data_nodes.items():
        node_type = type(data_node).__name__
        if hasattr(data_node, "get_remote_path"):
            remote_path = data_node.get_remote_path()
            print(f"DEBUG:   {port_name} = {node_type} (path: {remote_path}, pk: {data_node.pk})")
        else:
            print(f"DEBUG:   {port_name} = {node_type} (pk: {data_node.pk})")
        inputs[port_name] = data_node

    # Add metadata
    inputs["metadata"] = metadata_dict

    return inputs


# =============================================================================
# Shell Task Helper Functions
# =============================================================================


def load_and_process_shell_dependencies(
    parent_folders: dict,
    port_to_dep_mapping: dict,
    original_filenames: dict,
    label: str
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
    all_nodes = {}
    placeholder_to_node_key = {}
    filenames = {}

    # Load RemoteData nodes from their PKs
    parent_folders_loaded = {
        key: aiida.orm.load_node(val.value)
        for key, val in parent_folders.items()
    }

    print(f"DEBUG: Shell '{label}' loading RemoteData from PKs...")
    print(f"DEBUG:   port_to_dep_mapping = {port_to_dep_mapping}")
    print(f"DEBUG:   parent_folders_loaded keys = {list(parent_folders_loaded.keys())}")

    # Process ALL dependencies: create nodes, map placeholders, and map filenames
    for port_name, dep_info_list in port_to_dep_mapping.items():
        # dep_info_list is a list of (dep_task_label, filename, data_label) tuples
        for dep_label, filename, data_label in dep_info_list:
            if dep_label in parent_folders_loaded:
                workdir_remote_data = parent_folders_loaded[dep_label]
                workdir_path = workdir_remote_data.get_remote_path()

                print(f"DEBUG: Processing dep '{dep_label}' for port '{port_name}'")
                print(f"DEBUG:   Workdir path: {workdir_path}")
                print(f"DEBUG:   Filename from config: {filename}")

                # Create unique key for this dependency
                unique_key = f"{dep_label}_remote"

                # Add RemoteData node directly to all_nodes
                if filename:
                    # Create RemoteData pointing to the specific file
                    specific_file_path = f"{workdir_path}/{filename}"
                    all_nodes[unique_key] = aiida.orm.RemoteData(
                        computer=workdir_remote_data.computer,
                        remote_path=specific_file_path
                    )
                    print(f"DEBUG:   Added RemoteData '{unique_key}' for specific file: {specific_file_path}")
                else:
                    # No specific filename, use the workdir itself
                    all_nodes[unique_key] = workdir_remote_data
                    print(f"DEBUG:   Added RemoteData '{unique_key}' for workdir: {workdir_path}")

                # Build placeholder mapping for arguments
                placeholder_to_node_key[data_label] = unique_key
                print(f"DEBUG:   Mapped placeholder '{data_label}' -> node key '{unique_key}'")

                # Build filename mapping (from original_filenames via data_label)
                if data_label in original_filenames:
                    filenames[unique_key] = original_filenames[data_label]
                    print(f"DEBUG:   Mapped filename '{data_label}' -> '{unique_key}': '{original_filenames[data_label]}'")

    return all_nodes, placeholder_to_node_key, filenames


def build_shell_metadata_with_slurm_dependencies(
    base_metadata: dict,
    job_ids: dict | None,
    computer: aiida.orm.Computer
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

        print(f"DEBUG: Task {base_metadata.get('label', 'unknown')} - Setting SLURM dependency: {custom_cmd}")
        print(f"DEBUG: Task {base_metadata.get('label', 'unknown')} - custom_scheduler_commands = {metadata['options']['custom_scheduler_commands']}")

    return metadata


def process_shell_argument_placeholders(
    arguments_template: str | None,
    placeholder_to_node_key: dict
) -> list[str]:
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
        if arg.startswith('{') and arg.endswith('}'):
            placeholder_name = arg[1:-1]  # Remove the braces
            # Map to the actual node key if we have a mapping
            if placeholder_name in placeholder_to_node_key:
                actual_node_key = placeholder_to_node_key[placeholder_name]
                processed_arguments.append(f"{{{actual_node_key}}}")
                print(f"DEBUG:   Mapped argument placeholder '{arg}' -> '{{{actual_node_key}}}'")
            else:
                # Keep original if no mapping found
                processed_arguments.append(arg)
                print(f"DEBUG:   No mapping found for placeholder '{arg}', keeping original")
        else:
            processed_arguments.append(arg)

    return processed_arguments


# =============================================================================
# Instance Method Conversions - Task Spec Building
# =============================================================================


def validate_workflow(core_workflow: core.Workflow):
    """Checks if the core workflow uses valid AiiDA names for its tasks and data."""
    for task in core_workflow.tasks:
        try:
            aiida.common.validate_link_label(task.name)
        except ValueError as exception:
            msg = f"Raised error when validating task name '{task.name}': {exception.args[0]}"
            raise ValueError(msg) from exception
        for input_ in task.input_data_nodes():
            try:
                aiida.common.validate_link_label(input_.name)
            except ValueError as exception:
                msg = f"Raised error when validating input name '{input_.name}': {exception.args[0]}"
                raise ValueError(msg) from exception
        for output in task.output_data_nodes():
            try:
                aiida.common.validate_link_label(output.name)
            except ValueError as exception:
                msg = f"Raised error when validating output name '{output.name}': {exception.args[0]}"
                raise ValueError(msg) from exception


def add_aiida_input_data_node(
    data: core.AvailableData,
    core_workflow: core.Workflow,
    aiida_data_nodes: dict
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
        # ICON tasks require RemoteData
        aiida_data_nodes[label] = aiida.orm.RemoteData(
            remote_path=str(data.path), label=label, computer=computer
        )
    elif (
        computer.get_transport_class()
        is aiida.transports.plugins.local.LocalTransport
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
        options["max_wallclock_seconds"] = TimeUtils.walltime_to_seconds(
            task.walltime
        )
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


def build_base_metadata(task: core.Task) -> dict:
    """Build base metadata dict for any task type (without job dependencies).

    Job dependencies will be added at runtime in the @task.graph functions.

    Args:
        task: The task to build metadata for

    Returns:
        Metadata dict with computer_label and options
    """
    metadata = {}
    metadata["options"] = {}
    metadata["options"]["account"] = "cwd01"
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
    code_label = f"{cmd}@{computer.label}"
    try:
        code = aiida.orm.load_code(code_label)
    except NotExistent:
        code = ShellCode(
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
            "path": str(input_.path) if input_.path is not None else None,
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
            "path": str(output.path) if output.path is not None else None,
        }
        output_data_info.append(output_info)

    # Build input labels for argument resolution
    input_labels = {}
    for input_info in input_data_info:
        port_name = input_info["port"]
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

            filenames[input_info["name"]] = (
                Path(input_info["path"]).name
                if input_info["path"]
                else input_info["name"]
            )
        elif input_info["is_generated"]:
            # Count how many inputs have the same name
            same_name_count = sum(
                1 for info in input_data_info if info["name"] == input_info["name"]
            )
            if same_name_count > 1:
                filenames[input_label] = input_label
            else:
                from pathlib import Path

                filenames[input_label] = (
                    Path(input_info["path"]).name
                    if input_info["path"]
                    else input_info["name"]
                )

    # Build outputs list
    outputs = []
    for output_info in output_data_info:
        if output_info["is_generated"] and output_info["path"] is not None:
            outputs.append(output_info["path"])

    # Build output port mapping: data_name -> shell output link_label
    from aiida_shell.parsers.shell import ShellParser

    output_port_mapping = {}
    for output_info in output_data_info:
        if output_info["path"]:
            link_label = ShellParser.format_link_label(output_info["path"])
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
    icon_code_label = f"icon@{computer.label}"
    try:
        icon_code = aiida.orm.load_code(icon_code_label)
    except NotExistent:
        icon_code = aiida.orm.InstalledCode(
            label=icon_code_label,
            description="aiida_icon",
            default_calc_job_plugin="icon.icon",
            computer=computer,
            filepath_executable=str(task.bin),
            with_mpi=bool(task.mpi_cmd),
            use_double_quotes=True,
        ).store()

    # Build base metadata (no job dependencies yet)
    metadata = build_base_metadata(task)

    # Update task namelists
    task.update_icon_namelists_from_workflow()

    # Master namelist - store as PK
    with io.StringIO() as buffer:
        task.master_namelist.namelist.write(buffer)
        buffer.seek(0)
        master_namelist_node = aiida.orm.SinglefileData(
            buffer, task.master_namelist.name
        )
        master_namelist_node.store()

    # Model namelists - store as PKs
    model_namelist_pks = {}
    for model_name, model_nml in task.model_namelists.items():
        with io.StringIO() as buffer:
            model_nml.namelist.write(buffer)
            buffer.seek(0)
            model_node = aiida.orm.SinglefileData(buffer, model_nml.name)
            model_node.store()
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


class AiidaWorkGraph:
    """DEPRECATED: Use the functional API instead.

    This class is maintained for backward compatibility but is no longer the recommended approach.

    Migration Guide:
        Old:
            >>> aiida_wg = AiidaWorkGraph(core_wf)
            >>> wg = aiida_wg.build()
            >>> node = aiida_wg.submit()

        New (Recommended):
            >>> from sirocco.workgraph import build_sirocco_workgraph, submit_sirocco_workgraph
            >>> wg = build_sirocco_workgraph(core_wf)
            >>> # Or directly:
            >>> node = submit_sirocco_workgraph(core_wf)

    The functional API provides:
    - Simpler, more testable code
    - No hidden state
    - Clear data flow
    - Better composability
    """

    def __init__(self, core_workflow: core.Workflow):
        """Initialize with minimal setup - only validate and prepare data.

        DEPRECATED: Use build_sirocco_workgraph() instead.
        """
        self._core_workflow = core_workflow
        validate_workflow(core_workflow)

        # Only create available data nodes
        self._aiida_data_nodes: dict[str, WorkgraphDataNode] = {}
        for data in self._core_workflow.data:
            if isinstance(data, core.AvailableData):
                add_aiida_input_data_node(data, self._core_workflow, self._aiida_data_nodes)

        # Don't create workgraph yet
        self._workgraph = None

    # Thin wrappers for backwards compatibility - delegate to module-level functions
    @staticmethod
    def replace_invalid_chars_in_label(label: str) -> str:
        return replace_invalid_chars_in_label(label)

    @classmethod
    def get_aiida_label_from_graph_item(cls, obj: core.GraphItem) -> str:
        return get_aiida_label_from_graph_item(obj)

    @staticmethod
    def split_cmd_arg(command_line: str) -> tuple[str, str]:
        return split_cmd_arg(command_line)

    @classmethod
    def label_placeholder(cls, data: core.Data) -> str:
        return label_placeholder(data)

    @staticmethod
    def get_wrapper_script_aiida_data(task) -> aiida.orm.SinglefileData | None:
        return get_wrapper_script_aiida_data(task)

    @staticmethod
    def _get_default_wrapper_script() -> aiida.orm.SinglefileData | None:
        return get_default_wrapper_script()

    @staticmethod
    def _parse_mpi_cmd_to_aiida(mpi_cmd: str) -> str:
        return parse_mpi_cmd_to_aiida(mpi_cmd)

    @staticmethod
    def _translate_mpi_cmd_placeholder(placeholder: core.MpiCmdPlaceholder) -> str:
        return translate_mpi_cmd_placeholder(placeholder)


    def build(self) -> WorkGraph:
        """Build the dynamic workgraph with SLURM job dependencies.

        Returns:
            A WorkGraph ready for submission
        """
        # Pre-build all task specs (no job dependencies yet)
        shell_task_specs = {}
        icon_task_specs = {}

        for task in self._core_workflow.tasks:
            label = get_aiida_label_from_graph_item(task)
            if isinstance(task, core.ShellTask):
                shell_task_specs[label] = build_shell_task_spec(task)
            elif isinstance(task, core.IconTask):
                icon_task_specs[label] = build_icon_task_spec(task)

        # Build the dynamic workgraph
        wg = build_dynamic_sirocco_workgraph(
            core_workflow=self._core_workflow,
            aiida_data_nodes=self._aiida_data_nodes,
            shell_task_specs=shell_task_specs,
            icon_task_specs=icon_task_specs,
        )

        self._workgraph = wg
        return wg

    def submit(
        self,
        *,
        inputs: None | dict[str, Any] = None,
        wait: bool = False,
        timeout: int = 60,
        metadata: None | dict[str, Any] = None,
    ) -> aiida.orm.Node:
        """Submit the workflow to the AiiDA daemon.

        Builds the dynamic workgraph if not already built, then submits it.

        Args:
            inputs: Optional inputs to pass to the workgraph
            wait: Whether to wait for completion
            timeout: Timeout in seconds if wait=True
            metadata: Optional metadata for the workgraph

        Returns:
            The AiiDA process node

        Raises:
            RuntimeError: If submission fails
        """
        if self._workgraph is None:
            self.build()

        self._workgraph.submit(
            inputs=inputs, wait=wait, timeout=timeout, metadata=metadata
        )

        if (output_node := self._workgraph.process) is None:
            msg = "Something went wrong when submitting workgraph. Please contact a developer."
            raise RuntimeError(msg)

        return output_node

    def run(
        self,
        inputs: None | dict[str, Any] = None,
        metadata: None | dict[str, Any] = None,
    ) -> aiida.orm.Node:
        """Run the workflow in a blocking fashion.

        Builds the dynamic workgraph if not already built, then runs it.

        Args:
            inputs: Optional inputs to pass to the workgraph
            metadata: Optional metadata for the workgraph

        Returns:
            The AiiDA process node

        Raises:
            RuntimeError: If execution fails
        """
        if self._workgraph is None:
            self.build()

        self._workgraph.run(inputs=inputs, metadata=metadata)

        if (output_node := self._workgraph.process) is None:
            msg = "Something went wrong when running workgraph. Please contact a developer."
            raise RuntimeError(msg)

        return output_node


# =============================================================================
# Public API - Main Entry Point Functions
# =============================================================================


def build_sirocco_workgraph(core_workflow: core.Workflow) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows functionally.

    Args:
        core_workflow: The core workflow to convert

    Returns:
        A WorkGraph ready for submission

    Example:
        >>> from sirocco import core
        >>> from sirocco.workgraph import build_sirocco_workgraph
        >>> wf = core.Workflow(...)
        >>> wg = build_sirocco_workgraph(wf)
        >>> wg.submit()
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
    for task in core_workflow.tasks:
        label = get_aiida_label_from_graph_item(task)
        if isinstance(task, core.ShellTask):
            shell_task_specs[label] = build_shell_task_spec(task)
        elif isinstance(task, core.IconTask):
            icon_task_specs[label] = build_icon_task_spec(task)

    # Build the dynamic workgraph
    wg = build_dynamic_sirocco_workgraph(
        core_workflow=core_workflow,
        aiida_data_nodes=aiida_data_nodes,
        shell_task_specs=shell_task_specs,
        icon_task_specs=icon_task_specs,
    )

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

    Example:
        >>> from sirocco import core
        >>> from sirocco.workgraph import submit_sirocco_workgraph
        >>> wf = core.Workflow(...)
        >>> node = submit_sirocco_workgraph(wf)
        >>> print(f"Submitted as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.submit(
        inputs=inputs, wait=wait, timeout=timeout, metadata=metadata
    )

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

    Example:
        >>> from sirocco import core
        >>> from sirocco.workgraph import run_sirocco_workgraph
        >>> wf = core.Workflow(...)
        >>> node = run_sirocco_workgraph(wf)
        >>> print(f"Completed as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.run(inputs=inputs, metadata=metadata)

    if (output_node := wg.process) is None:
        msg = "Something went wrong when running workgraph. Please contact a developer."
        raise RuntimeError(msg)

    return output_node

