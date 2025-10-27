from __future__ import annotations

import asyncio
import io
import time
import uuid
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
from aiida_workgraph import WorkGraph, dynamic, task

from sirocco import core
from sirocco.parsing._utils import TimeUtils

if TYPE_CHECKING:

    WorkgraphDataNode: TypeAlias = (
        aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData
    )


@task
async def get_job_id(
    workgraph_name: str, task_name: str, interval: int = 2, timeout: int = 600
):
    """Get the job_id of a CalcJob task in a workgraph by polling.

    Args:
        workgraph_name: Name of the workgraph containing the task
        task_name: Name of the task to get job_id from
        interval: Polling interval in seconds
        timeout: Maximum time to wait in seconds

    Returns:
        The SLURM job ID as an integer

    Raises:
        TimeoutError: If job_id is not available within timeout period
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
    while True:
        if builder.count() > 0:
            workgraph_node = builder.all()[-1][0]
            node = yaml.load(
                workgraph_node.task_processes.get(task_name, ""), Loader=AiiDALoader
            )
            if node:
                job_id = node.get_job_id()
                if job_id is not None:
                    return job_id
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timeout waiting for job_id for task {task_name}")
        await asyncio.sleep(interval)


@task.graph
def launch_shell_task_with_dependency(
    task_spec: dict,
    input_data_nodes: dict,
    job_ids: Annotated[dict, dynamic(int)] = None,
):
    """Launch a shell task with optional SLURM job dependencies.

    Args:
        task_spec: Dict from _build_shell_task_spec() containing:
            - label: Task label
            - code: ShellCode instance
            - nodes: Dict of script files
            - metadata: Base metadata dict
            - arguments_template: Command arguments template
            - filenames: Input filename mappings
            - outputs: List of output file paths
            - task: Original core.ShellTask instance
        input_data_nodes: Dict mapping port names to AiiDA data nodes
        job_ids: Optional dict of {dependency_label: job_id} for SLURM dependencies

    Returns:
        Dict with task outputs
    """
    from aiida_workgraph.tasks.shelljob_task import _build_shelljob_nodespec

    # Get task reference
    task = task_spec["task"]
    label = task_spec["label"]

    # Copy and modify metadata with runtime job_ids
    metadata = dict(task_spec["metadata"])
    metadata["options"] = dict(metadata["options"])

    if job_ids:
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"

        if "custom_scheduler_commands" in metadata["options"]:
            metadata["options"]["custom_scheduler_commands"] += f"\n{custom_cmd}"
        else:
            metadata["options"]["custom_scheduler_commands"] = custom_cmd

    # Build input labels for argument resolution
    input_labels = {}
    for port_name, input_ in task.input_data_items():
        input_label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)
        if port_name not in input_labels:
            input_labels[port_name] = []
        input_labels[port_name].append(f"{{{input_label}}}")

    # Resolve arguments using the template
    arguments_with_placeholders = task.resolve_ports(input_labels)
    _, arguments = AiidaWorkGraph.split_cmd_arg(arguments_with_placeholders)

    # Merge script nodes with input data nodes
    all_nodes = {**task_spec["nodes"]}
    for port, data_node in input_data_nodes.items():
        node_label = AiidaWorkGraph.get_aiida_label_from_graph_item(
            next(d for p, d in task.input_data_items() if p == port)
        )
        all_nodes[node_label] = data_node

    # Build output paths with formatted names
    outputs = []
    for output in task.output_data_nodes():
        if isinstance(output, core.GeneratedData) and output.path is not None:
            outputs.append(str(output.path))

    # Build filenames dict
    filenames = {}
    for input_ in task.input_data_nodes():
        input_label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)
        if isinstance(input_, core.AvailableData):
            filenames[input_.name] = input_.path.name
        elif isinstance(input_, core.GeneratedData):
            same_name_count = sum(
                1 for inp in task.input_data_nodes() if inp.name == input_.name
            )
            if same_name_count > 1:
                filenames[input_label] = input_label
            else:
                filenames[input_label] = (
                    input_.path.name if input_.path is not None else input_.name
                )

    # Build the shelljob NodeSpec
    spec = _build_shelljob_nodespec(
        identifier=f"shelljob_{label}",
        outputs=None,
        parser_outputs=None,
    )

    # Create the shell task
    # Note: This returns a namespace with the task's outputs
    from aiida_workgraph import WorkGraph

    wg = WorkGraph.get_current_workgraph()

    shell_task = wg.tasks._new(
        spec,
        name=label,
        command=task_spec["code"],
        arguments=[arguments],
        nodes=all_nodes,
        outputs=outputs,
        filenames=filenames,
        metadata=metadata,
        resolve_command=False,
    )

    # Return outputs as a namespace
    return {
        output.name: getattr(
            shell_task.outputs, ShellParser.format_link_label(str(output.path))
        )
        for output in task.output_data_nodes()
        if output.path
    }


@task.graph
def launch_icon_task_with_dependency(
    task_spec: dict,
    input_data_nodes: dict,
    job_ids: Annotated[dict, dynamic(int)] = None,
):
    """Launch an ICON task with optional SLURM job dependencies.

    Args:
        task_spec: Dict from _build_icon_task_spec() containing:
            - label: Task label
            - builder: IconCalculation builder
            - task: Original core.IconTask instance
        input_data_nodes: Dict mapping port names to AiiDA data nodes
        job_ids: Optional dict of {dependency_label: job_id} for SLURM dependencies

    Returns:
        Builder outputs (output_streams, restart_file, finish_status, etc.)
    """
    from copy import deepcopy

    # Deep copy the builder to avoid modifying the original
    builder = deepcopy(task_spec["builder"])
    task = task_spec["task"]
    label = task_spec["label"]

    # Add runtime job dependencies to metadata
    if job_ids:
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"

        current_cmds = builder.metadata.options.get("custom_scheduler_commands", "")
        if current_cmds:
            builder.metadata.options["custom_scheduler_commands"] = (
                current_cmds + f"\n{custom_cmd}"
            )
        else:
            builder.metadata.options["custom_scheduler_commands"] = custom_cmd

    # Set input data nodes on the builder
    for port_name, data_node in input_data_nodes.items():
        if port_name == "restart_file":
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
    from aiida_workgraph import WorkGraph

    wg = WorkGraph.get_current_workgraph()

    icon_task = wg.add_task(builder, name=label)

    # Return outputs as dict
    outputs = {}
    for port_name, output_list in task.outputs.items():
        if port_name == "output_streams":
            outputs["output_streams"] = icon_task.outputs.output_streams
        elif port_name == "latest_restart_file":
            outputs["restart_file"] = icon_task.outputs.restart_file
        elif port_name == "finish_status":
            outputs["finish_status"] = icon_task.outputs.finish_status
        elif port_name is None:
            outputs["retrieved"] = icon_task.outputs.retrieved

    return outputs


@task.graph
def build_dynamic_sirocco_workgraph(
    core_workflow: core.Workflow,
    aiida_data_nodes: dict,
    shell_task_specs: dict,
    icon_task_specs: dict,
):
    """Build Sirocco workgraph dynamically with SLURM job dependencies.

    This function creates tasks in dependency order, waiting for job IDs
    before submitting dependent tasks.

    Args:
        core_workflow: The unrolled Sirocco core workflow
        aiida_data_nodes: Pre-created available data nodes
        shell_task_specs: Dict mapping task labels to shell task specs
        icon_task_specs: Dict mapping task labels to icon task specs

    Returns:
        Dict with final workflow outputs
    """
    task_outputs = {}  # Store task outputs by label
    job_ids = {}  # Store SLURM job IDs by label

    # Helper to get task label
    def get_label(task):
        return AiidaWorkGraph.get_aiida_label_from_graph_item(task)

    # Process all tasks in the workflow
    # Note: We iterate through cycles which already have correct ordering
    for cycle in core_workflow.cycles:
        for task in cycle.tasks:
            task_label = get_label(task)

            # Collect job IDs of tasks this one waits on
            dep_job_ids = {}
            for wait_task in task.wait_on:
                wait_label = get_label(wait_task)
                if wait_label in job_ids:
                    dep_job_ids[wait_label] = job_ids[wait_label]

            # Collect input data nodes for this task
            input_data_for_task = {}
            for port, input_data in task.input_data_items():
                input_label = get_label(input_data)

                if isinstance(input_data, core.AvailableData):
                    # Use pre-created available data node
                    input_data_for_task[port] = aiida_data_nodes[input_label]

                elif isinstance(input_data, core.GeneratedData):
                    # Find which task generated this data
                    for prev_task in core_workflow.tasks:
                        for out_port, out_data in prev_task.output_data_items():
                            if get_label(out_data) == input_label:
                                prev_task_label = get_label(prev_task)
                                if prev_task_label in task_outputs:
                                    # Get the specific output
                                    input_data_for_task[port] = task_outputs[
                                        prev_task_label
                                    ].get(out_data.name, task_outputs[prev_task_label])
                                break

            # Launch the task with dependencies
            if isinstance(task, core.ShellTask):
                task_spec = shell_task_specs[task_label]
                outputs = launch_shell_task_with_dependency(
                    task_spec=task_spec,
                    input_data_nodes=input_data_for_task,
                    job_ids=dep_job_ids if dep_job_ids else None,
                )

            elif isinstance(task, core.IconTask):
                task_spec = icon_task_specs[task_label]
                outputs = launch_icon_task_with_dependency(
                    task_spec=task_spec,
                    input_data_nodes=input_data_for_task,
                    job_ids=dep_job_ids if dep_job_ids else None,
                )
            else:
                raise TypeError(f"Unknown task type: {type(task)}")

            # Store task outputs
            task_outputs[task_label] = outputs

            # Get the SLURM job ID (blocks until task is submitted)
            job_id = get_job_id(
                workgraph_name=task_label,
                task_name=task_label,
            ).result
            job_ids[task_label] = job_id

    # Return summary of completion
    return {
        "status": "completed",
        "num_tasks": len(task_outputs),
        "final_outputs": task_outputs,
    }


class AiidaWorkGraph:
    def __init__(self, core_workflow: core.Workflow):
        """Initialize with minimal setup - only validate and prepare data."""
        breakpoint()
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

        # custom_scheduler_commands
        options["custom_scheduler_commands"] = ""
        if isinstance(task, core.IconTask) and task.uenv is not None:
            options["custom_scheduler_commands"] += f"#SBATCH --uenv={task.uenv}\n"
        if isinstance(task, core.IconTask) and task.view is not None:
            options["custom_scheduler_commands"] += f"#SBATCH --view={task.view}\n"

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
        metadata["options"]["use_symlinks"] = True
        metadata["options"]["account"] = "cwd01"
        metadata["options"]["additional_retrieve"] = [
            "_scheduler-stdout.txt",
            "_scheduler-stderr.txt",
        ]
        metadata["options"].update(self._from_task_get_scheduler_options(task))

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
            metadata["computer"] = computer
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        return metadata

    def _build_shell_task_spec(self, task: core.ShellTask) -> dict:
        """Build all parameters needed to create a shell task.

        Returns a dict with keys: label, code, nodes, metadata,
        arguments_template, filenames, outputs, task

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

        # Build nodes (input files like scripts)
        nodes = {}
        if task.path is not None:
            nodes[f"SCRIPT__{label}"] = aiida.orm.SinglefileData(str(task.path))

        # Create code
        label_uuid = str(uuid.uuid4())
        code = ShellCode(
            label=f"{cmd}-{label_uuid}",
            computer=computer,
            filepath_executable=cmd,
            default_calc_job_plugin="core.shell",
            use_double_quotes=True,
        ).store()

        # Arguments template (will be resolved at runtime)
        _, arguments_template = self.split_cmd_arg(task.command)

        # Build filenames mapping
        filenames = {}
        for input_ in task.input_data_nodes():
            input_label = self.get_aiida_label_from_graph_item(input_)
            if isinstance(input_, core.AvailableData):
                filenames[input_.name] = input_.path.name
            elif isinstance(input_, core.GeneratedData):
                same_name_count = sum(
                    1 for inp in task.input_data_nodes() if inp.name == input_.name
                )
                if same_name_count > 1:
                    filenames[input_label] = input_label
                else:
                    filenames[input_label] = (
                        input_.path.name if input_.path is not None else input_.name
                    )

        # Build outputs list
        outputs = []
        for output in task.output_data_nodes():
            if isinstance(output, core.GeneratedData) and output.path is not None:
                outputs.append(str(output.path))

        return {
            "label": label,
            "code": code,
            "nodes": nodes,
            "metadata": metadata,
            "arguments_template": arguments_template,
            "filenames": filenames,
            "outputs": outputs,
            "task": task,
        }

    def _build_icon_task_spec(self, task: core.IconTask) -> dict:
        """Build all parameters needed to create an ICON task.

        Returns a dict with keys: label, builder, task

        Note: Job dependencies are NOT included here - they're added at runtime.
        """

        task_label = self.get_aiida_label_from_graph_item(task)

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        # Create ICON code
        icon_code = aiida.orm.InstalledCode(
            label=f"icon-{task_label}",
            description="aiida_icon",
            default_calc_job_plugin="icon.icon",
            computer=computer,
            filepath_executable=str(task.bin),
            with_mpi=bool(task.mpi_cmd),
            use_double_quotes=True,
        ).store()

        # Build builder
        builder = IconCalculation.get_builder()
        builder.code = icon_code

        # Build base metadata (no job dependencies yet)
        metadata = self._build_base_metadata(task)

        # Update task namelists
        task.update_icon_namelists_from_workflow()

        # Master namelist
        with io.StringIO() as buffer:
            task.master_namelist.namelist.write(buffer)
            buffer.seek(0)
            builder.master_namelist = aiida.orm.SinglefileData(
                buffer, task.master_namelist.name
            )

        # Model namelists
        for model_name, model_nml in task.model_namelists.items():
            with io.StringIO() as buffer:
                model_nml.namelist.write(buffer)
                buffer.seek(0)
                setattr(
                    builder.models,
                    model_name,
                    aiida.orm.SinglefileData(buffer, model_nml.name),
                )

        # Add wrapper script
        wrapper_script_data = self.get_wrapper_script_aiida_data(task)
        if wrapper_script_data is not None:
            builder.wrapper_script = wrapper_script_data

        # Set scheduler options
        options = metadata["options"]
        options["additional_retrieve_list"] = []

        # Set metadata on builder
        builder.metadata = metadata

        return {
            "label": task_label,
            "builder": builder,
            "task": task,
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
        wg = build_dynamic_sirocco_workgraph.build(
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
