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

    placeholder_to_node_key = {}
    filenames = {}

    # Load RemoteData nodes from their PKs and process dependencies in one pass
    parent_folders_loaded = (
        {key: aiida.orm.load_node(val.value) for key, val in parent_folders.items()}
        if parent_folders
        else None
    )

    if parent_folders_loaded:
        port_to_dep_mapping = task_spec.get("port_to_dep_mapping", {})
        original_filenames = task_spec["filenames"]

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

    # Copy and modify metadata with runtime job_ids
    metadata = dict(task_spec["metadata"])
    metadata["options"] = dict(metadata["options"])

    # Load computer from label
    computer = aiida.orm.Computer.collection.get(label=metadata.pop("computer_label"))
    metadata["computer"] = computer

    # CRITICAL: Make sure we have job dependencies for ALL upstream tasks
    if job_ids:
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"

        if "custom_scheduler_commands" in metadata["options"]:
            metadata["options"]["custom_scheduler_commands"] += f"\n{custom_cmd}"
        else:
            metadata["options"]["custom_scheduler_commands"] = custom_cmd

    # Use pre-resolved arguments template but update placeholders to match actual node keys
    arguments_template = task_spec["arguments_template"]
    
    # Split the arguments into a list where each argument is a separate element
    if arguments_template:
        arguments_list = arguments_template.split()
    else:
        arguments_list = []
    
    # Update placeholders to match the actual node keys we created
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
    
    arguments = processed_arguments

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
        task_spec: Dict from _build_icon_task_spec() containing:
            - label: Task label
            - code_pk: ICON code PK
            - master_namelist_pk: Master namelist PK
            - model_namelist_pks: Dict of model name -> namelist PK
            - wrapper_script_pk: Wrapper script PK (optional)
            - metadata: Base metadata dict
            - output_port_mapping: Dict mapping data names to ICON output port names
            - port_to_dep_mapping: Dict mapping port names to dependency labels
        input_data_nodes: Dict mapping port names to AvailableData nodes
        parent_folders: Dict of {dep_label: remote_folder_pk} from get_job_data
        job_ids: Dict of {dep_label: job_id} from get_job_data

    Returns:
        IconTask outputs
    """
    from aiida_icon.iconutils.modelnml import read_latest_restart_file_link_name
    import f90nml

    label = task_spec["label"]
    output_port_mapping = task_spec["output_port_mapping"]
    computer_label = task_spec["metadata"]["computer_label"]

    # Handle None inputs
    if input_data_nodes is None:
        input_data_nodes = {}

    print(f"DEBUG: Launcher for '{label}' starting...")

    # Load RemoteData nodes from their PKs (following Xing's pattern exactly)
    parent_folders_loaded = (
        {key: aiida.orm.load_node(val.value) for key, val in parent_folders.items()}
        if parent_folders
        else None
    )

    # Map loaded RemoteData to input ports using port_to_dep_mapping
    # port_to_dep_mapping[port_name] = [(dep_label, filename), ...] (list of (dep, filename) tuples)
    if parent_folders_loaded:
        port_to_dep_mapping = task_spec.get("port_to_dep_mapping", {})
        print(f"DEBUG: '{label}' loading RemoteData from PKs...")
        print(f"DEBUG:   port_to_dep_mapping = {port_to_dep_mapping}")
        print(
            f"DEBUG:   parent_folders_loaded keys = {list(parent_folders_loaded.keys())}"
        )

        for port_name, dep_info_list in port_to_dep_mapping.items():
            # dep_info_list is a list of (dep_task_label, filename, data_label) tuples
            # For ICON restart_file, should only be one dependency
            if dep_info_list:
                dep_label, filename, data_label = dep_info_list[0]  # Unpack the tuple (data_label unused for ICON)
                if dep_label in parent_folders_loaded:
                    workdir_remote_data = parent_folders_loaded[dep_label]
                    workdir_path = workdir_remote_data.get_remote_path()

                    print(
                        f"DEBUG: Processing port '{port_name}' from dep '{dep_label}'"
                    )
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
                        print(
                            f"DEBUG:   Created RemoteData for specific file: {specific_file_path}"
                        )
                    else:
                        # No specific filename, use aiida-icon to determine restart filename
                        try:
                            # Get the model namelist for this task (assuming 'atm' model)
                            model_namelist_pk = task_spec["model_namelist_pks"]["atm"]
                            model_namelist_node = aiida.orm.load_node(model_namelist_pk)

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
                            input_data_nodes[port_name] = file_remote_data
                            print(
                                f"DEBUG:   Using aiida-icon determined restart file: {specific_file_path}"
                            )

                        except Exception as e:
                            print(
                                f"DEBUG:   Failed to determine restart filename using aiida-icon: {e}"
                            )
                            # Fallback: use the workdir itself
                            input_data_nodes[port_name] = workdir_remote_data
                            print(
                                f"DEBUG:   Falling back to workdir RemoteData (no specific filename)"
                            )

    # Collect job IDs for SLURM dependencies (following Xing's pattern)
    if job_ids:
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"
        print(f"DEBUG: Task {label} - Setting SLURM dependency: {custom_cmd}")

    # Reconstruct metadata with computer
    metadata = dict(task_spec["metadata"])
    metadata["options"] = dict(metadata["options"])
    computer = aiida.orm.Computer.collection.get(label=computer_label)

    # Add SLURM job dependencies if we have any (following Xing's pattern)
    if job_ids:
        # custom_cmd was already set above
        current_cmds = metadata["options"].get("custom_scheduler_commands", "")
        if current_cmds:
            metadata["options"]["custom_scheduler_commands"] = (
                current_cmds + f"\n{custom_cmd}"
            )
        else:
            metadata["options"]["custom_scheduler_commands"] = custom_cmd

        print(
            f"DEBUG: Task {label} - custom_scheduler_commands = {metadata['options']['custom_scheduler_commands']}"
        )

    # Prepare inputs dict for IconTask
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

    # Add ALL input data nodes (both AvailableData and future RemoteData for GeneratedData)
    print(f"DEBUG: Setting ICON inputs for '{label}':")
    for port_name, data_node in input_data_nodes.items():
        node_type = type(data_node).__name__
        if hasattr(data_node, "get_remote_path"):
            remote_path = data_node.get_remote_path()
            print(
                f"DEBUG:   {port_name} = {node_type} (path: {remote_path}, pk: {data_node.pk})"
            )
        else:
            print(f"DEBUG:   {port_name} = {node_type} (pk: {data_node.pk})")
        inputs[port_name] = data_node

    # Prepare metadata dict
    metadata_dict = {
        "computer": computer,
        "options": metadata["options"],
        "call_link_label": label,
    }
    inputs["metadata"] = metadata_dict

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

            # Collect ONLY AvailableData inputs
            input_data_for_task = {}
            for port, input_data in task.input_data_items():
                input_label = get_label(input_data)
                if isinstance(input_data, core.AvailableData):
                    input_data_for_task[port] = aiida_data_nodes[input_label]
                    print(f"DEBUG:   AvailableData '{port}' -> {input_label}")

            # Build port_to_dep_mapping: map each port to list of (dependency_label, filename, data_label) tuples
            # The filename is extracted from input_data.path and will be used to create specific symlinks
            # The data_label is used for Shell task placeholder resolution
            port_to_dep_mapping = {}  # {port_name: [(dep_task_label, filename, data_label), ...]}
            parent_folders_for_task = {}  # {dep_label: remote_folder_pk}
            job_ids_for_task = {}  # {dep_label: job_id}

            for port, input_data in task.input_data_items():
                if isinstance(input_data, core.GeneratedData):
                    input_data_label = get_label(input_data)
                    print(
                        f"DEBUG:   Processing GeneratedData '{input_data.name}' (input has no path, will get from output)"
                    )

                    # Find producer task - get filename from the OUTPUT specification
                    for prev_task in core_workflow.tasks:
                        for out_port, out_data in prev_task.output_data_items():
                            if get_label(out_data) == input_data_label:
                                prev_task_label = get_label(prev_task)

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

            if isinstance(task, core.IconTask):
                task_spec = icon_task_specs[task_label]
                launcher_name = f"launch_{task_label}"

                # Add port_to_dep_mapping to task_spec
                task_spec["port_to_dep_mapping"] = port_to_dep_mapping

                # Create launcher task (following Xing's pattern)
                icon_launcher = wg.add_task(
                    launch_icon_task_with_dependency,
                    name=launcher_name,
                    task_spec=task_spec,
                    input_data_nodes=input_data_for_task
                    if input_data_for_task
                    else None,
                    parent_folders=parent_folders_for_task
                    if parent_folders_for_task
                    else None,
                    job_ids=job_ids_for_task if job_ids_for_task else None,
                )

                # Create get_job_data task (following Xing's pattern)
                # Returns namespace with job_id (int) and remote_folder (int pk)
                dep_task = wg.add_task(
                    get_job_data,
                    name=f"get_job_data_{task_label}",
                    workgraph_name=launcher_name,
                    task_name=task_label,
                )

                # Store the outputs namespace for dependent tasks
                task_dep_info[task_label] = dep_task.outputs

                # Chain with previous dependency tasks using >>
                # This ensures get_job_data tasks wait for upstream dependencies (optional, like Xing's)
                for dep_label in parent_folders_for_task.keys():
                    if dep_label in prev_dep_tasks:
                        prev_dep_tasks[dep_label] >> dep_task
                        print(f"DEBUG: Chaining {dep_label} >> {task_label}")

                # Store for next iteration
                prev_dep_tasks[task_label] = dep_task

                print(
                    f"DEBUG: Created ICON launcher '{launcher_name}' with {len(parent_folders_for_task)} parent_folders, {len(job_ids_for_task)} job_ids"
                )

            elif isinstance(task, core.ShellTask):
                task_spec = shell_task_specs[task_label]
                launcher_name = f"launch_{task_label}"
                output_port_mapping = task_spec["output_port_mapping"]

                # Add port_to_dep_mapping to task_spec (even though shell tasks don't use it yet)
                task_spec["port_to_dep_mapping"] = port_to_dep_mapping

                # Create launcher task (following Xing's pattern)
                shell_launcher = wg.add_task(
                    launch_shell_task_with_dependency,
                    name=launcher_name,
                    task_spec=task_spec,
                    input_data_nodes=input_data_for_task
                    if input_data_for_task
                    else None,
                    parent_folders=parent_folders_for_task
                    if parent_folders_for_task
                    else None,
                    job_ids=job_ids_for_task if job_ids_for_task else None,
                )

                print(
                    f"DEBUG: Created shell launcher '{launcher_name}' with {len(parent_folders_for_task)} parent_folders"
                )

                # Create get_job_data task (following Xing's pattern)
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


class AiidaWorkGraph:
    def __init__(self, core_workflow: core.Workflow):
        """Initialize with minimal setup - only validate and prepare data."""
        self._core_workflow = core_workflow
        self._validate_workflow()

        # Only create available data nodes
        self._aiida_data_nodes: dict[str, WorkgraphDataNode] = {}
        for data in self._core_workflow.data:
            if isinstance(data, core.AvailableData):
                self._add_aiida_input_data_node(data)

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

    def _validate_workflow(self):
        """Checks if the core workflow uses valid AiiDA names for its tasks and data."""
        for task in self._core_workflow.tasks:
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

    def _add_aiida_input_data_node(self, data: core.AvailableData):
        """
        Create an `aiida.orm.Data` instance from the provided `data` that needs to exist on initialization of workflow.
        """
        label = self.get_aiida_label_from_graph_item(data)

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
            for task in self._core_workflow.tasks
        )

        if used_by_icon_task:
            # ICON tasks require RemoteData
            self._aiida_data_nodes[label] = aiida.orm.RemoteData(
                remote_path=str(data.path), label=label, computer=computer
            )
        elif (
            computer.get_transport_class()
            is aiida.transports.plugins.local.LocalTransport
        ):
            if data.path.is_file():
                self._aiida_data_nodes[label] = aiida.orm.SinglefileData(
                    file=str(data.path), label=label
                )
            else:
                self._aiida_data_nodes[label] = aiida.orm.FolderData(
                    tree=str(data.path), label=label
                )
        else:
            self._aiida_data_nodes[label] = aiida.orm.RemoteData(
                remote_path=str(data.path), label=label, computer=computer
            )

    def _from_task_get_scheduler_options(self, task: core.Task) -> dict[str, Any]:
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

    def _build_base_metadata(self, task: core.Task) -> dict:
        """Build base metadata dict for any task type (without job dependencies).

        Job dependencies will be added at runtime in the @task.graph functions.
        """
        metadata = {}
        metadata["options"] = {}
        metadata["options"]["account"] = "cwd01"
        metadata["options"]["additional_retrieve_list"] = [
            "_scheduler-stdout.txt",
            "_scheduler-stderr.txt",
        ]
        metadata["options"].update(self._from_task_get_scheduler_options(task))

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
            metadata["computer_label"] = computer.label
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        return metadata

    def _build_shell_task_spec(self, task: core.ShellTask) -> dict:
        """Build all parameters needed to create a shell task.

        Returns a dict with keys: label, code, nodes, metadata,
        arguments_template, filenames, outputs, input_data_info, output_data_info

        Note: Job dependencies are NOT included here - they're added at runtime.
        """
        from aiida_shell import ShellCode

        label = self.get_aiida_label_from_graph_item(task)
        cmd, _ = self.split_cmd_arg(task.command)

        # Get computer
        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        # Build base metadata (no job dependencies yet)
        metadata = self._build_base_metadata(task)

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
                "label": self.get_aiida_label_from_graph_item(input_),
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
                "label": self.get_aiida_label_from_graph_item(output),
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
        _, resolved_arguments_template = self.split_cmd_arg(arguments_with_placeholders)

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

    def _build_icon_task_spec(self, task: core.IconTask) -> dict:
        """Build all parameters needed to create an ICON task.

        Returns a dict with keys: label, builder, output_ports

        Note: Job dependencies are NOT included here - they're added at runtime.
        """

        task_label = self.get_aiida_label_from_graph_item(task)

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
        metadata = self._build_base_metadata(task)

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
        wrapper_script_data = self.get_wrapper_script_aiida_data(task)
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

    def build(self) -> WorkGraph:
        """Build the dynamic workgraph with SLURM job dependencies.

        Returns:
            A WorkGraph ready for submission
        """
        # Pre-build all task specs (no job dependencies yet)
        shell_task_specs = {}
        icon_task_specs = {}

        for task in self._core_workflow.tasks:
            label = self.get_aiida_label_from_graph_item(task)
            if isinstance(task, core.ShellTask):
                shell_task_specs[label] = self._build_shell_task_spec(task)
            elif isinstance(task, core.IconTask):
                icon_task_specs[label] = self._build_icon_task_spec(task)

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

