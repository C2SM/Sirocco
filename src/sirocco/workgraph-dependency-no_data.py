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
from aiida_workgraph import WorkGraph, dynamic, task, get_current_graph

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


@task
async def get_job_id(
    workgraph_name: str, task_name: str, interval: int = 5, timeout: int = 180
):
    """Get the job_id of a CalcJob task immediately after submission.

    This polls for the job ID to become available, which should happen
    almost immediately after the CalcJob is submitted to SLURM.
    """
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine

    builder = orm.QueryBuilder()
    builder.append(
        WorkGraphEngine,
        filters={"attributes.process_label": {"==": f"WorkGraph<{workgraph_name}>"}},
        tag="process",
    )
    start_time = time.time()

    print(
        f"DEBUG: Starting job_id polling for {task_name} in workgraph {workgraph_name}"
    )

    while True:
        if builder.count() > 0:
            workgraph_node = builder.all()[-1][0]
            node_data = workgraph_node.task_processes.get(task_name, "")
            if node_data:
                node = yaml.load(node_data, Loader=AiiDALoader)
                if node:
                    job_id = node.get_job_id()
                    if job_id is not None:
                        print(
                            f"DEBUG: Successfully retrieved job_id {job_id} for {task_name}"
                        )
                        return aiida.orm.Int(job_id)
                    else:
                        print(
                            f"DEBUG: Job ID not yet available for {task_name}, continuing to poll..."
                        )

        # If we've been waiting too long, something is wrong
        elapsed = time.time() - start_time
        if elapsed > timeout:
            # Let's debug what's available
            print(f"DEBUG: Timeout debugging for {task_name}:")
            print(f"  - Builder count: {builder.count()}")
            if builder.count() > 0:
                workgraph_node = builder.all()[-1][0]
                print(
                    f"  - Task processes keys: {list(workgraph_node.task_processes.keys())}"
                )
                node_data = workgraph_node.task_processes.get(task_name, "")
                if node_data:
                    node = yaml.load(node_data, Loader=AiiDALoader)
                    print(f"  - Node type: {type(node)}")
                    if node:
                        print(f"  - Node state: {node.process_state}")
                        print(f"  - Job ID: {node.get_job_id()}")

            raise TimeoutError(
                f"Timeout waiting for job_id for task {task_name} after {timeout}s"
            )

        await asyncio.sleep(interval)


@task.graph
def launch_shell_task_with_dependency(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] | None = None,
    job_ids: Annotated[dict, dynamic(int)] | None = None,
) -> Annotated[dict, dynamic(aiida.orm.Data)]:
    """Launch a shell task with optional SLURM job dependencies.

    Args:
        task_spec: Dict from _build_shell_task_spec() containing:
            - label: Task label
            - code_pk: ShellCode PK
            - node_pks: Dict of script file PKs
            - metadata: Base metadata dict
            - arguments_template: Pre-resolved command arguments template
            - filenames: Input filename mappings
            - outputs: List of output file paths
            - input_data_info: List of input data information dicts
            - output_data_info: List of output data information dicts
        input_data_nodes: Dict mapping port names to AiiDA data nodes
        job_ids: Optional dict of {dependency_label: job_id} for SLURM dependencies

    Returns:
        Dict with task outputs
    """
    from aiida_workgraph.tasks.shelljob_task import _build_shelljob_nodespec

    # Get pre-computed data
    label = task_spec["label"]
    input_data_info = task_spec["input_data_info"]
    output_data_info = task_spec["output_data_info"]

    # Handle None input_data_nodes
    if input_data_nodes is None:
        input_data_nodes = {}

    # Load the code from PK
    code = aiida.orm.load_node(task_spec["code_pk"])

    # Load nodes from PKs
    all_nodes = {
        key: aiida.orm.load_node(pk) for key, pk in task_spec["node_pks"].items()
    }

    # Copy and modify metadata with runtime job_ids
    metadata = dict(task_spec["metadata"])
    metadata["options"] = dict(metadata["options"])

    # Load computer from label
    computer = aiida.orm.Computer.collection.get(label=metadata.pop("computer_label"))
    metadata["computer"] = computer

    if job_ids:
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"

        if "custom_scheduler_commands" in metadata["options"]:
            metadata["options"]["custom_scheduler_commands"] += f"\n{custom_cmd}"
        else:
            metadata["options"]["custom_scheduler_commands"] = custom_cmd

    # Use pre-resolved arguments template (no need to resolve again)
    arguments = task_spec["arguments_template"]

    # Merge script nodes with input data nodes
    for port, data_node in input_data_nodes.items():
        # Find the label for this port from pre-computed info
        node_label = next(
            info["label"] for info in input_data_info if info["port"] == port
        )
        all_nodes[node_label] = data_node

    # Use pre-computed outputs
    outputs = task_spec["outputs"]

    # Use pre-computed filenames
    filenames = task_spec["filenames"]

    # Build the shelljob NodeSpec
    # Create parser_outputs list (output names as strings)
    parser_outputs = [
        output_info["name"] for output_info in output_data_info if output_info["path"]
    ]

    spec = _build_shelljob_nodespec(
        identifier=f"shelljob_{label}",
        outputs=outputs,
        parser_outputs=parser_outputs,
    )

    # Create the shell task
    # Note: This returns a namespace with the task's outputs

    wg = get_current_graph()

    shell_task = wg.add_task(
        spec,
        name=label,
        command=code,
        arguments=[arguments],
        nodes=all_nodes,
        outputs=outputs,
        filenames=filenames,
        metadata=metadata,
        resolve_command=False,
    )

    # Return the shell_task outputs directly
    # We'll use output_port_mapping in the connection logic to map data names to link labels
    return shell_task.outputs


@task.graph
def launch_icon_task_with_dependency(
    task_spec: dict,
    input_data_nodes: Annotated[dict, dynamic(aiida.orm.Data)] = None,
    job_ids: Annotated[dict, dynamic(int)] = None,
) -> Annotated[dict, dynamic(aiida.orm.Data)]:
    """Launch an ICON task with optional SLURM job dependencies.

    Args:
        task_spec: Dict from _build_icon_task_spec() containing:
            - label: Task label
            - code_pk: ICON code PK
            - master_namelist_pk: Master namelist PK
            - model_namelist_pks: Dict of model name -> namelist PK
            - wrapper_script_pk: Wrapper script PK (optional)
            - metadata: Base metadata dict
            - output_port_mapping: Dict mapping data names to ICON output port names
        input_data_nodes: Dict mapping port names to AiiDA data nodes
        job_ids: Optional dict of {dependency_label: job_id} for SLURM dependencies

    Returns:
        Builder outputs (output_streams, restart_file, finish_status, etc.)
    """
    label = task_spec["label"]
    output_port_mapping = task_spec["output_port_mapping"]

    # Handle None input_data_nodes
    if input_data_nodes is None:
        input_data_nodes = {}

    # Reconstruct the builder from PKs
    builder = IconCalculation.get_builder()
    builder.code = aiida.orm.load_node(task_spec["code_pk"])
    builder.master_namelist = aiida.orm.load_node(task_spec["master_namelist_pk"])

    # Load model namelists
    for model_name, model_pk in task_spec["model_namelist_pks"].items():
        setattr(builder.models, model_name, aiida.orm.load_node(model_pk))

    # Load wrapper script if present
    if task_spec["wrapper_script_pk"] is not None:
        builder.wrapper_script = aiida.orm.load_node(task_spec["wrapper_script_pk"])

    # Reconstruct metadata with computer
    metadata = dict(task_spec["metadata"])
    metadata["options"] = dict(metadata["options"])
    computer_label = metadata.pop("computer_label")
    computer = aiida.orm.Computer.collection.get(label=computer_label)

    # Add runtime job dependencies to custom_scheduler_commands
    if job_ids:
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"

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

    # Set metadata on builder - set computer and options individually to avoid overwriting
    builder.metadata.computer = computer
    for option_key, option_value in metadata["options"].items():
        setattr(builder.metadata.options, option_key, option_value)

    # Set input data nodes on the builder
    for port_name, data_node in input_data_nodes.items():
        print(
            f"DEBUG: Setting builder.{port_name} = {type(data_node).__name__} (node type)"
        )
        if port_name == "restart_file":
            print(
                f"DEBUG: restart_file type check - expected RemoteData, got {type(data_node)}"
            )
            builder.restart_file = data_node
        elif port_name == "dynamics_grid_file":
            builder.dynamics_grid_file = data_node
        elif port_name == "ecrad_data":
            builder.ecrad_data = data_node
        elif port_name == "cloud_opt_props":
            builder.cloud_opt_props = data_node
        elif port_name == "rrtmg_lw":
            builder.rrtmg_lw = data_node
        elif port_name == "dmin_wetgrowth_lookup":
            builder.dmin_wetgrowth_lookup = data_node
        else:
            # Generic port setting
            setattr(builder, port_name, data_node)

    # Submit the builder via workgraph
    wg = get_current_graph()

    icon_task = wg.add_task(builder, name=label)

    # Return the icon_task outputs directly
    # We'll use output_port_mapping in the connection logic to map data names to port names
    return icon_task.outputs


def build_dynamic_sirocco_workgraph(
    core_workflow: core.Workflow,
    aiida_data_nodes: dict,
    shell_task_specs: dict,
    icon_task_specs: dict,
):
    from aiida_workgraph.manager import set_current_graph

    wg = WorkGraph("FULL-WG")
    set_current_graph(wg)

    job_ids = {}  # Store SLURM job ID futures by label
    previous_job_id_task = None  # Track the previous job_id task

    # Helper to get task label
    def get_label(task):
        return AiidaWorkGraph.get_aiida_label_from_graph_item(task)

    # Process all tasks in the workflow in cycle order
    for cycle in core_workflow.cycles:
        for task in cycle.tasks:
            task_label = get_label(task)

            # Collect input data
            input_data_for_task = {}
            for port, input_data in task.input_data_items():
                input_label = get_label(input_data)
                if isinstance(input_data, core.AvailableData):
                    input_data_for_task[port] = aiida_data_nodes[input_label]

            # Build job_ids dict from data dependencies
            dep_job_ids = {}
            for port, input_data in task.input_data_items():
                if isinstance(input_data, core.GeneratedData):
                    for prev_task in core_workflow.tasks:
                        for out_port, out_data in prev_task.output_data_items():
                            if get_label(out_data) == get_label(input_data):
                                prev_task_label = get_label(prev_task)
                                if prev_task_label in job_ids:
                                    dep_job_ids[prev_task_label] = job_ids[
                                        prev_task_label
                                    ]
                                    print(
                                        f"DEBUG: Task '{task_label}' will depend on job_id from '{prev_task_label}' (data: {input_data.name})"
                                    )
                                break

            if isinstance(task, core.IconTask):
                task_spec = icon_task_specs[task_label]
                launcher_name = f"launch_{task_label}"

                # Create launcher task (starts immediately)
                icon_launcher = wg.add_task(
                    launch_icon_task_with_dependency,
                    name=launcher_name,
                    task_spec=task_spec,
                    input_data_nodes=input_data_for_task
                    if input_data_for_task
                    else None,
                    job_ids=dep_job_ids if dep_job_ids else None,
                )

                # Create job_id fetcher for THIS launcher
                job_id_task = wg.add_task(
                    get_job_id,
                    name=f"get_job_id_{task_label}",
                    workgraph_name=launcher_name,
                    task_name=task_label,
                )

                # CRITICAL: Make this job_id_task wait for the PREVIOUS job_id_task
                # This creates the job ID dependency chain
                if previous_job_id_task is not None:
                    previous_job_id_task >> job_id_task

                # Store the job_id future for dependent tasks
                job_ids[task_label] = job_id_task.outputs["result"]

                # Update previous job_id task for next iteration
                previous_job_id_task = job_id_task

                print(
                    f"DEBUG: Created task '{task_label}' with {len(dep_job_ids)} data dependencies"
                )

            elif isinstance(task, core.ShellTask):
                task_spec = shell_task_specs[task_label]
                launcher_name = f"launch_{task_label}"

                # Create launcher task
                shell_launcher = wg.add_task(
                    launch_shell_task_with_dependency,
                    name=launcher_name,
                    task_spec=task_spec,
                    input_data_nodes=input_data_for_task
                    if input_data_for_task
                    else None,
                    job_ids=dep_job_ids if dep_job_ids else None,
                )

                # Create job_id fetcher for THIS launcher
                job_id_task = wg.add_task(
                    get_job_id,
                    name=f"get_job_id_{task_label}",
                    workgraph_name=launcher_name,
                    task_name=task_label,
                )

                # Make this job_id_task wait for the PREVIOUS job_id_task
                if previous_job_id_task is not None:
                    previous_job_id_task >> job_id_task

                # Store the job_id future for dependent tasks
                job_ids[task_label] = job_id_task.outputs["result"]

                # Update previous job_id task for next iteration
                previous_job_id_task = job_id_task

                print(
                    f"DEBUG: Created task '{task_label}' with {len(dep_job_ids)} data dependencies"
                )

            else:
                raise TypeError(f"Unknown task type: {type(task)}")

            print(f"DEBUG: Created task '{task_label}'")

    return wg


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

    @staticmethod
    def replace_invalid_chars_in_label(label: str) -> str:
        """Replaces chars in the label that are invalid for AiiDA.

        The invalid chars ["-", " ", ":", "."] are replaced with underscores.
        """
        invalid_chars = ["-", " ", ":", "."]
        for invalid_char in invalid_chars:
            label = label.replace(invalid_char, "_")
        return label

    @classmethod
    def get_aiida_label_from_graph_item(cls, obj: core.GraphItem) -> str:
        """Returns a unique AiiDA label for the given graph item.

        The graph item object is uniquely determined by its name and its coordinates. There is the possibility that
        through the replacement of invalid chars in the coordinates duplication can happen but it is unlikely.
        """
        return cls.replace_invalid_chars_in_label(
            f"{obj.name}"
            + "__".join(f"_{key}_{value}" for key, value in obj.coordinates.items())
        )

    @staticmethod
    def split_cmd_arg(command_line: str) -> tuple[str, str]:
        split = command_line.split(sep=" ", maxsplit=1)
        if len(split) == 1:
            return command_line, ""
        return split[0], split[1]

    @classmethod
    def label_placeholder(cls, data: core.Data) -> str:
        return f"{{{cls.get_aiida_label_from_graph_item(data)}}}"

    @staticmethod
    def get_wrapper_script_aiida_data(task) -> aiida.orm.SinglefileData | None:
        """Get AiiDA SinglefileData for wrapper script if configured"""
        if task.wrapper_script is not None:
            return aiida.orm.SinglefileData(str(task.wrapper_script))
        return AiidaWorkGraph._get_default_wrapper_script()

    @staticmethod
    def _get_default_wrapper_script() -> aiida.orm.SinglefileData | None:
        """Get default wrapper script based on task type"""

        # Import the script directory from aiida-icon
        from aiida_icon.site_support.cscs.alps import SCRIPT_DIR

        default_script_path = SCRIPT_DIR / "todi_cpu.sh"
        return aiida.orm.SinglefileData(file=default_script_path)

    @staticmethod
    def _parse_mpi_cmd_to_aiida(mpi_cmd: str) -> str:
        for placeholder in core.MpiCmdPlaceholder:
            mpi_cmd = mpi_cmd.replace(
                f"{{{placeholder.value}}}",
                f"{{{AiidaWorkGraph._translate_mpi_cmd_placeholder(placeholder)}}}",
            )
        return mpi_cmd

    @staticmethod
    def _translate_mpi_cmd_placeholder(placeholder: core.MpiCmdPlaceholder) -> str:
        match placeholder:
            case core.MpiCmdPlaceholder.MPI_TOTAL_PROCS:
                return "tot_num_mpiprocs"
            case _:
                assert_never(placeholder)

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


# def build_dynamic_sirocco_workgraph(
#     core_workflow: core.Workflow,
#     aiida_data_nodes: dict,
#     shell_task_specs: dict,
#     icon_task_specs: dict,
# ):
#     from aiida_workgraph.manager import set_current_graph
#
#     wg = WorkGraph("FULL-WG")
#     set_current_graph(wg)
#
#     job_ids = {}  # Store SLURM job ID futures by label
#     task_handles = {}  # Store task handles for dependency management
#
#     # Helper to get task label
#     def get_label(task):
#         return AiidaWorkGraph.get_aiida_label_from_graph_item(task)
#
#     # First pass: Create all tasks without job_id dependencies
#     # This allows us to reference them for data dependencies
#     for cycle in core_workflow.cycles:
#         for task in cycle.tasks:
#             task_label = get_label(task)
#
#             # Store basic task info for second pass
#             task_handles[task_label] = {
#                 "task": task,
#                 "input_data_for_task": {},
#                 "depends_on": set(),  # Which tasks this one depends on
#             }
#
#             # Collect input data
#             for port, input_data in task.input_data_items():
#                 input_label = get_label(input_data)
#                 if isinstance(input_data, core.AvailableData):
#                     task_handles[task_label]["input_data_for_task"][port] = (
#                         aiida_data_nodes[input_label]
#                     )
#
#             # Find data dependencies
#             for port, input_data in task.input_data_items():
#                 if isinstance(input_data, core.GeneratedData):
#                     for prev_task in core_workflow.tasks:
#                         for out_port, out_data in prev_task.output_data_items():
#                             if get_label(out_data) == get_label(input_data):
#                                 prev_task_label = get_label(prev_task)
#                                 task_handles[task_label]["depends_on"].add(
#                                     prev_task_label
#                                 )
#                                 break
#
#     # Second pass: Create tasks with proper job_id dependencies
#     # Process in cycle order to ensure dependencies are available
#     for cycle in core_workflow.cycles:
#         for task in cycle.tasks:
#             task_label = get_label(task)
#             task_info = task_handles[task_label]
#
#             # Build job_ids dict from dependencies
#             dep_job_ids = {}
#             for dep_label in task_info["depends_on"]:
#                 if dep_label in job_ids:
#                     dep_job_ids[dep_label] = job_ids[dep_label]
#                     print(
#                         f"DEBUG: Task '{task_label}' will depend on job_id from '{dep_label}'"
#                     )
#
#             if isinstance(task, core.IconTask):
#                 task_spec = icon_task_specs[task_label]
#                 launcher_name = f"launch_{task_label}"
#
#                 # Create launcher task
#                 icon_launcher = wg.add_task(
#                     launch_icon_task_with_dependency,
#                     name=launcher_name,
#                     task_spec=task_spec,
#                     input_data_nodes=task_info["input_data_for_task"]
#                     if task_info["input_data_for_task"]
#                     else None,
#                     job_ids=dep_job_ids if dep_job_ids else None,
#                 )
#
#                 # Create job_id fetcher for THIS launcher
#                 job_id_task = wg.add_task(
#                     get_job_id,
#                     name=f"get_job_id_{task_label}",
#                     workgraph_name=launcher_name,
#                     task_name=task_label,
#                 )
#
#                 # CRITICAL: Make job_id_task wait for THIS launcher only
#                 # This doesn't block other tasks, just ensures we get the job ID for this specific task
#                 icon_launcher >> job_id_task
#
#                 # Store the job_id future for dependent tasks
#                 job_ids[task_label] = job_id_task.outputs["result"]
#
#                 print(
#                     f"DEBUG: Created task '{task_label}' with {len(dep_job_ids)} dependencies"
#                 )
#
#             elif isinstance(task, core.ShellTask):
#                 task_spec = shell_task_specs[task_label]
#                 launcher_name = f"launch_{task_label}"
#
#                 # Create launcher task
#                 shell_launcher = wg.add_task(
#                     launch_shell_task_with_dependency,
#                     name=launcher_name,
#                     task_spec=task_spec,
#                     input_data_nodes=task_info["input_data_for_task"]
#                     if task_info["input_data_for_task"]
#                     else None,
#                     job_ids=dep_job_ids if dep_job_ids else None,
#                 )
#
#                 # Create job_id fetcher for THIS launcher
#                 job_id_task = wg.add_task(
#                     get_job_id,
#                     name=f"get_job_id_{task_label}",
#                     workgraph_name=launcher_name,
#                     task_name=task_label,
#                 )
#
#                 # Make job_id_task wait for THIS launcher only
#                 shell_launcher >> job_id_task
#
#                 # Store the job_id future for dependent tasks
#                 job_ids[task_label] = job_id_task.outputs["result"]
#
#                 print(
#                     f"DEBUG: Created task '{task_label}' with {len(dep_job_ids)} dependencies"
#                 )
#
#             else:
#                 raise TypeError(f"Unknown task type: {type(task)}")
#
#     return wg


# def build_dynamic_sirocco_workgraph(
#     core_workflow: core.Workflow,
#     aiida_data_nodes: dict,
#     shell_task_specs: dict,
#     icon_task_specs: dict,
# ):
#     from aiida_workgraph.manager import set_current_graph
#
#     wg = WorkGraph("FULL-WG")
#     set_current_graph(wg)
#
#     job_ids = {}  # Store SLURM job IDs by label
#     previous_job_id_task = None  # Track the previous job_id task
#
#     # Helper to get task label
#     def get_label(task):
#         return AiidaWorkGraph.get_aiida_label_from_graph_item(task)
#
#     # Collect all tasks in cycle order
#     all_tasks = []
#     for cycle in core_workflow.cycles:
#         for task in cycle.tasks:
#             all_tasks.append(task)
#
#     # Process tasks in cycle order
#     for task in all_tasks:
#         task_label = get_label(task)
#
#         # Collect only AvailableData (pre-existing files) for input_data_nodes
#         input_data_for_task = {}
#         for port, input_data in task.input_data_items():
#             input_label = get_label(input_data)
#             if isinstance(input_data, core.AvailableData):
#                 input_data_for_task[port] = aiida_data_nodes[input_label]
#
#         # Collect job_id sockets for SLURM dependencies
#         dep_job_ids = {}
#
#         # 1. Explicit wait_on dependencies
#         for wait_task in task.wait_on:
#             wait_label = get_label(wait_task)
#             if wait_label in job_ids:
#                 dep_job_ids[wait_label] = job_ids[wait_label]
#                 print(
#                     f"DEBUG: Task '{task_label}' will depend on job_id from '{wait_label}' (wait_on)"
#                 )
#
#         # 2. Data dependencies - find which tasks produce data this task needs
#         for port, input_data in task.input_data_items():
#             if isinstance(input_data, core.GeneratedData):
#                 for (
#                     prev_task
#                 ) in all_tasks:  # Use all_tasks instead of core_workflow.tasks
#                     for out_port, out_data in prev_task.output_data_items():
#                         if get_label(out_data) == get_label(input_data):
#                             prev_task_label = get_label(prev_task)
#                             if (
#                                 prev_task_label in job_ids
#                                 and prev_task_label not in dep_job_ids
#                             ):
#                                 dep_job_ids[prev_task_label] = job_ids[prev_task_label]
#                                 print(
#                                     f"DEBUG: Task '{task_label}' will depend on job_id from '{prev_task_label}' (data: {input_data.name})"
#                                 )
#                             break
#
#         if isinstance(task, core.IconTask):
#             task_spec = icon_task_specs[task_label]
#             launcher_name = f"launch_{task_label}"
#
#             # Launch as separate graph task
#             icon_launcher = wg.add_task(
#                 launch_icon_task_with_dependency,
#                 name=launcher_name,
#                 task_spec=task_spec,
#                 input_data_nodes=input_data_for_task if input_data_for_task else None,
#                 job_ids=dep_job_ids if dep_job_ids else None,
#             )
#
#             # Get job ID using the LAUNCHER's workgraph name
#             job_id_task = wg.add_task(
#                 get_job_id,
#                 name=f"get_job_id_{task_label}",
#                 workgraph_name=launcher_name,
#                 task_name=task_label,
#             )
#
#             # CRITICAL: Make job_id_task wait for the launcher to START (not finish)
#             # This ensures job_id_task starts immediately after icon_launcher begins execution
#             icon_launcher >> job_id_task
#
#             # CRITICAL: Make this launcher wait for the previous job_id task to complete
#             # This ensures tasks are submitted in the correct order
#             if previous_job_id_task is not None:
#                 previous_job_id_task >> icon_launcher
#
#             # Store the job_id socket for dependencies
#             job_ids[task_label] = job_id_task.outputs["result"]
#
#             # Update previous job_id task for next iteration
#             previous_job_id_task = job_id_task
#
#             print(f"DEBUG: Created job_id socket for task '{task_label}'")
#
#         elif isinstance(task, core.ShellTask):
#             task_spec = shell_task_specs[task_label]
#             launcher_name = f"launch_{task_label}"
#
#             # Launch as separate graph task
#             shell_launcher = wg.add_task(
#                 launch_shell_task_with_dependency,
#                 name=launcher_name,
#                 task_spec=task_spec,
#                 input_data_nodes=input_data_for_task if input_data_for_task else None,
#                 job_ids=dep_job_ids if dep_job_ids else None,
#             )
#
#             # Get job ID using the LAUNCHER's workgraph name
#             job_id_task = wg.add_task(
#                 get_job_id,
#                 name=f"get_job_id_{task_label}",
#                 workgraph_name=launcher_name,
#                 task_name=task_label,
#             )
#
#             # Make job_id_task wait for the launcher to START
#             shell_launcher >> job_id_task
#
#             # Make this launcher wait for the previous job_id task to complete
#             if previous_job_id_task is not None:
#                 previous_job_id_task >> shell_launcher
#
#             # Store the job_id socket for dependencies
#             job_ids[task_label] = job_id_task.outputs["result"]
#
#             # Update previous job_id task for next iteration
#             previous_job_id_task = job_id_task
#
#             print(f"DEBUG: Created job_id socket for task '{task_label}'")
#
#         else:
#             raise TypeError(f"Unknown task type: {type(task)}")
#
#         print(f"DEBUG: Created task '{task_label}'")
#
#     return wg


# def build_dynamic_sirocco_workgraph(
#     core_workflow: core.Workflow,
#     aiida_data_nodes: dict,
#     shell_task_specs: dict,
#     icon_task_specs: dict,
# ):
#     """Build Sirocco workgraph dynamically with SLURM job dependencies.
#
#     This function creates tasks in dependency order, waiting for job IDs
#     before submitting dependent tasks.
#
#     Args:
#         core_workflow: The unrolled Sirocco core workflow
#         aiida_data_nodes: Pre-created available data nodes
#         shell_task_specs: Dict mapping task labels to shell task specs
#         icon_task_specs: Dict mapping task labels to icon task specs
#
#     Returns:
#         Dict with final workflow outputs
#     """
#     from aiida_workgraph.manager import set_current_graph
#
#     wg = WorkGraph("FULL-WG")
#     set_current_graph(wg)
#
#     job_ids = {}  # Store SLURM job IDs by label
#
#     # Helper to get task label
#     def get_label(task):
#         return AiidaWorkGraph.get_aiida_label_from_graph_item(task)
#
#     # Process all tasks in the workflow
#     # Note: We iterate through cycles which already have correct ordering
#     for cycle in core_workflow.cycles:
#         for task in cycle.tasks:
#             task_label = get_label(task)
#
#             # Collect only AvailableData (pre-existing files) for input_data_nodes
#             # Skip GeneratedData connections - rely on SLURM dependencies + filesystem
#             input_data_for_task = {}
#             for port, input_data in task.input_data_items():
#                 input_label = get_label(input_data)
#                 if isinstance(input_data, core.AvailableData):
#                     # Use pre-created available data node
#                     input_data_for_task[port] = aiida_data_nodes[input_label]
#
#             # Collect job_id sockets for SLURM dependencies
#             # These are futures - they don't block task creation!
#             dep_job_ids = {}
#
#             # 1. Explicit wait_on dependencies
#             for wait_task in task.wait_on:
#                 wait_label = get_label(wait_task)
#                 if wait_label in job_ids:
#                     dep_job_ids[wait_label] = job_ids[wait_label]
#                     print(
#                         f"DEBUG: Task '{task_label}' will depend on job_id from '{wait_label}' (wait_on)"
#                     )
#
#             # 2. Data dependencies - find which tasks produce data this task needs
#             for port, input_data in task.input_data_items():
#                 if isinstance(input_data, core.GeneratedData):
#                     for prev_task in core_workflow.tasks:
#                         for out_port, out_data in prev_task.output_data_items():
#                             if get_label(out_data) == get_label(input_data):
#                                 prev_task_label = get_label(prev_task)
#                                 # Only add if we already have the job_id socket from a previous iteration
#                                 if (
#                                     prev_task_label in job_ids
#                                     and prev_task_label not in dep_job_ids
#                                 ):
#                                     dep_job_ids[prev_task_label] = job_ids[
#                                         prev_task_label
#                                     ]
#                                     print(
#                                         f"DEBUG: Task '{task_label}' will depend on job_id from '{prev_task_label}' (data: {input_data.name})"
#                                     )
#                                 break
#
#             if isinstance(task, core.IconTask):
#                 task_spec = icon_task_specs[task_label]
#
#                 # Create UNIQUE launcher name for each task
#                 launcher_name = f"launch_{task_label}"
#
#                 # Launch as separate graph task with unique metadata
#                 icon_launcher = wg.add_task(
#                     launch_icon_task_with_dependency,
#                     name=launcher_name,
#                     task_spec=task_spec,
#                     input_data_nodes=input_data_for_task
#                     if input_data_for_task
#                     else None,
#                     job_ids=dep_job_ids if dep_job_ids else None,
#                 )
#
#                 # Get job ID using the LAUNCHER's workgraph name, not the task label
#                 job_id_task = wg.add_task(
#                     get_job_id,
#                     name=f"get_job_id_{task_label}",
#                     workgraph_name=launcher_name,  # Use launcher name, not task_label
#                     task_name=task_label,  # But task name remains the CalcJob name
#                 )
#
#                 # icon_launcher.outputs >> job_id_task
#
#                 # Store the job_id socket (future) for dependencies
#                 job_ids[task_label] = job_id_task.outputs["result"]
#                 print(f"DEBUG: Created job_id socket for task '{task_label}'")
#
#             elif isinstance(task, core.ShellTask):
#                 task_spec = shell_task_specs[task_label]
#
#                 # Create UNIQUE launcher name for each task
#                 launcher_name = f"launch_{task_label}"
#
#                 # Launch as separate graph task with unique metadata
#                 shell_launcher = wg.add_task(
#                     launch_shell_task_with_dependency,
#                     name=launcher_name,
#                     task_spec=task_spec,
#                     input_data_nodes=input_data_for_task
#                     if input_data_for_task
#                     else None,
#                     job_ids=dep_job_ids if dep_job_ids else None,
#                 )
#
#                 # Get job ID using the LAUNCHER's workgraph name, not the task label
#                 job_id_task = wg.add_task(
#                     get_job_id,
#                     name=f"get_job_id_{task_label}",
#                     workgraph_name=launcher_name,  # Use launcher name, not task_label
#                     task_name=task_label,  # But task name remains the CalcJob name
#                 )
#
#                 # shell_launcher.outputs >> job_id_task
#
#                 # Store the job_id socket (future) for dependencies
#                 job_ids[task_label] = job_id_task.outputs["result"]
#                 print(f"DEBUG: Created job_id socket for task '{task_label}'")
#
#             else:
#                 raise TypeError(f"Unknown task type: {type(task)}")
#
#             print(f"DEBUG: Created task '{task_label}'")
#
#     return wg
