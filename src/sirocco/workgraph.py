from __future__ import annotations

import functools
import io
import uuid
from typing import TYPE_CHECKING, Any, TypeAlias, assert_never

import aiida.common
import aiida.orm
import aiida.transports
import aiida.transports.plugins.local
from aiida.common.exceptions import NotExistent
from aiida_icon.calculations import IconCalculation
from aiida_shell.parsers.shell import ShellParser
from aiida_workgraph import WorkGraph, Task, task

from sirocco import core
from sirocco.core.graph_items import GeneratedData
from sirocco.parsing._utils import TimeUtils

if TYPE_CHECKING:
    from aiida_workgraph.socket import TaskSocket  # type: ignore[import-untyped]

    WorkgraphDataNode: TypeAlias = (
        aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData
    )


@task
async def get_job_id(workgraph_name: str, task_name: str, interval: int = 2, timeout: int = 600):
    """Get the job_id of a CalcJob task in a workgraph by polling the workgraph node."""
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine
    from aiida.orm.utils.serialize import AiiDALoader
    import yaml
    import time
    import asyncio

    # Query the WorkGraph node by its name
    builder = orm.QueryBuilder()
    builder.append(
        WorkGraphEngine,
        filters={"attributes.process_label": {"==": f"WorkGraph<{workgraph_name}>"}},
        tag="process",
    )
    start_time = time.time()
    while True:
        if builder.count() > 0:
            # Get the last node in the workgraph
            workgraph_node = builder.all()[-1][0]
            # Load the AiiDA process node for the specified task
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


@task
def add_slurm_dependency_to_metadata(metadata: dict, job_ids: dict | None = None):
    """Add SLURM dependency command to task metadata based on job IDs.

    This task runs at execution time and modifies metadata to include SLURM dependencies.
    """
    if job_ids:
        # Filter out None values and build dependency string
        valid_ids = [str(jid) for jid in job_ids.values() if jid is not None]
        if valid_ids:
            dep_str = ":".join(valid_ids)
            custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"

            # Add to existing custom commands
            current_cmds = metadata.get("options", {}).get("custom_scheduler_commands", "")
            if current_cmds and not current_cmds.endswith("\n"):
                current_cmds += "\n"

            if "options" not in metadata:
                metadata["options"] = {}
            metadata["options"]["custom_scheduler_commands"] = current_cmds + custom_cmd

    return metadata




class AiidaWorkGraph:
    """AiiDA WorkGraph wrapper for Sirocco workflows.

    This class converts a Sirocco core workflow into an AiiDA WorkGraph for execution.

    Args:
        core_workflow: The Sirocco core workflow to execute
        pre_submission_depth: Controls how many dependency levels ahead to pre-submit using SLURM dependencies.
            - 0: Fully sequential (WorkGraph controls flow, no SLURM dependencies)
            - 1 (default): Submit one step ahead - direct dependencies use SLURM --dependency flags
            - N > 1: Submit N steps ahead - dependencies up to N levels apart use SLURM dependencies
            - Large number (e.g., 9999): Submit entire workflow at once with all dependencies handled by SLURM

    Example:
        # Fully sequential, no SLURM dependencies
        wg = AiidaWorkGraph(workflow, pre_submission_depth=0)

        # Submit one step ahead (default) - direct dependencies use SLURM deps
        wg = AiidaWorkGraph(workflow, pre_submission_depth=1)

        # Pre-submit chains where dependencies are within 3 levels
        wg = AiidaWorkGraph(workflow, pre_submission_depth=3)

        # Submit entire workflow to SLURM at once
        wg = AiidaWorkGraph(workflow, pre_submission_depth=9999)

    Note:
        When pre_submission_depth > 0, the workflow uses SLURM job dependencies (--dependency=afterok:jobid)
        to control task execution order, allowing tasks to be submitted before their dependencies complete.
        This can significantly improve workflow throughput for long-running tasks.

        The depth refers to the maximum difference in dependency levels between a task and its dependencies
        that will use SLURM dependencies. For example, with depth=1, a task at level 2 will use SLURM
        dependencies for its level-1 dependencies (difference=1), but not for level-0 dependencies (difference=2).
    """

    def __init__(self, core_workflow: core.Workflow, pre_submission_depth: int = 1):
        # the core workflow that unrolled the time constraints for the whole graph
        self._core_workflow = core_workflow

        # Number of dependency levels that use SLURM dependencies
        # 0 = fully sequential (WorkGraph control)
        # 1 = one step ahead (direct dependencies use SLURM)
        # N = N steps ahead
        # Large number = entire workflow uses SLURM dependencies
        self._pre_submission_depth = pre_submission_depth

        self._validate_workflow()

        self._workgraph = WorkGraph(core_workflow.name)

        # stores the input data available on initialization
        self._aiida_data_nodes: dict[str, WorkgraphDataNode] = {}
        # stores the outputs sockets of tasks
        self._aiida_socket_nodes: dict[str, TaskSocket] = {}
        self._aiida_task_nodes: dict[str, Task] = {}
        # stores job_id retrieval tasks for SLURM dependency chains
        self._job_id_tasks: dict[str, Task] = {}

        # create input data nodes
        for data in self._core_workflow.data:
            if isinstance(data, core.AvailableData):
                self._add_aiida_input_data_node(data)

        # create workgraph task nodes and output sockets
        for task in self._core_workflow.tasks:
            self.create_task_node(task)
            # Create and link corresponding output sockets
            for port, output in task.output_data_items():
                self._link_output_node_to_task(task, port, output)

        # link input nodes to workgraph tasks
        for task in self._core_workflow.tasks:
            for port, input_ in task.input_data_items():
                self._link_input_node_to_task(task, port, input_)

        # set shelljob arguments
        for task in self._core_workflow.tasks:
            if isinstance(task, core.ShellTask):
                self._set_shelljob_arguments(task)
                self._set_shelljob_filenames(task)

        # link wait on to workgraph tasks
        for task in self._core_workflow.tasks:
            self._link_wait_on_to_task(task)

        # Apply SLURM dependency pre-submission if depth > 0
        if self._pre_submission_depth > 0:
            self._apply_slurm_dependencies()

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

    def data_from_core(
        self, core_available_data: core.AvailableData
    ) -> WorkgraphDataNode:
        return self._aiida_data_nodes[
            self.get_aiida_label_from_graph_item(core_available_data)
        ]

    def socket_from_core(self, core_generated_data: core.GeneratedData) -> TaskSocket:
        return self._aiida_socket_nodes[
            self.get_aiida_label_from_graph_item(core_generated_data)
        ]

    def task_from_core(self, core_task: core.Task) -> Task:
        return self._aiida_task_nodes[self.get_aiida_label_from_graph_item(core_task)]

    def _add_available_data(self):
        """Adds the available data on initialization to the workgraph"""
        for data in self._core_workflow.data:
            if isinstance(data, core.AvailableData):
                self._add_aiida_input_data_node(data)

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

    @functools.singledispatchmethod
    def create_task_node(self, task: core.Task):
        """dispatch creating task nodes based on task type"""

        if isinstance(task, core.IconTask):
            msg = "method not implemented yet for Icon tasks"
        else:
            msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @create_task_node.register
    def _create_shell_task_node(self, task: core.ShellTask):
        from aiida_workgraph.tasks.shelljob_task import _build_shelljob_nodespec
        from aiida_shell import ShellCode

        label = self.get_aiida_label_from_graph_item(task)
        cmd, _ = self.split_cmd_arg(task.command)

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database."
            raise ValueError(msg) from err

        # Build metadata
        metadata = {}
        metadata["options"] = {}
        metadata["options"]["use_symlinks"] = True
        metadata["options"]["account"] = "cwd01"
        metadata["options"]["additional_retrieve"] = [
            "_scheduler-stdout.txt",
            "_scheduler-stderr.txt",
        ]
        metadata["options"].update(self._from_task_get_scheduler_options(task))

        if task.computer is not None:
            metadata["computer"] = computer

        # Prepare nodes (input files)
        nodes = {}
        if task.path is not None:
            nodes[f"SCRIPT__{label}"] = aiida.orm.SinglefileData(str(task.path))

        # Create ShellCode
        # TODO: don't create new ones
        label_uuid = str(uuid.uuid4())
        code = ShellCode(
            label=f"{cmd}-{label_uuid}",
            computer=computer,
            filepath_executable=cmd,
            default_calc_job_plugin="core.shell",
            use_double_quotes=True,
        ).store()

        # Build the shelljob NodeSpec directly
        spec = _build_shelljob_nodespec(
            identifier=f"shelljob_{label}",
            outputs=None,
            parser_outputs=None,
        )

        # Create task from spec
        workgraph_task = self._workgraph.tasks._new(
            spec,
            name=label,
            command=code,
            arguments=[],
            nodes=nodes,
            outputs=[],
            metadata=metadata,
            resolve_command=False,
        )

        self._aiida_task_nodes[label] = workgraph_task

    @create_task_node.register
    def _create_icon_task_node(self, task: core.IconTask):
        task_label = self.get_aiida_label_from_graph_item(task)

        try:
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database. One needs to create and configure the computer before running a workflow."
            raise ValueError(msg) from err

        # Use the original computer directly
        icon_code = aiida.orm.InstalledCode(
            label=f"icon-{task_label}",
            description="aiida_icon",
            default_calc_job_plugin="icon.icon",
            computer=computer,
            filepath_executable=str(task.bin),
            with_mpi=bool(task.mpi_cmd),
            use_double_quotes=True,
        ).store()

        builder = IconCalculation.get_builder()
        builder.code = icon_code
        metadata = {}

        task.update_icon_namelists_from_workflow()

        # Master namelist
        with io.StringIO() as buffer:
            task.master_namelist.namelist.write(buffer)
            buffer.seek(0)
            builder.master_namelist = aiida.orm.SinglefileData(
                buffer, task.master_namelist.name
            )

        # Handle multiple model namelists
        for model_name, model_nml in task.model_namelists.items():
            with io.StringIO() as buffer:
                model_nml.namelist.write(buffer)
                buffer.seek(0)
                setattr(
                    builder.models,  # type: ignore[attr-defined]
                    model_name,
                    aiida.orm.SinglefileData(buffer, model_nml.name),
                )

        # Add wrapper script
        wrapper_script_data = AiidaWorkGraph.get_wrapper_script_aiida_data(task)
        if wrapper_script_data is not None:
            builder.wrapper_script = wrapper_script_data

        # Set runtime information
        options = {}
        options.update(self._from_task_get_scheduler_options(task))
        options["additional_retrieve_list"] = []
        options["account"] = "cwd01"

        metadata["options"] = options
        builder.metadata = metadata

        self._aiida_task_nodes[task_label] = self._workgraph.add_task(
            builder, name=task_label
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

    @functools.singledispatchmethod
    def _link_output_node_to_task(
        self,
        task: core.Task,
        port: str,  # noqa: ARG002
        output: core.GeneratedData,  # noqa: ARG002
    ):
        """Dispatch linking input to task based on task type."""

        msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @_link_output_node_to_task.register
    def _link_output_node_to_icon_task(
        self, task: core.IconTask, port: str | None, output: core.GeneratedData
    ):
        workgraph_task = self.task_from_core(task)
        output_label = self.get_aiida_label_from_graph_item(output)

        if port == "output_streams":
            # Use the existing output_streams namespace from IconCalculation
            output_socket = workgraph_task.outputs._sockets.get("output_streams")  # noqa: SLF001
            if output_socket is None:
                msg = "Output socket 'output_streams' was not found for ICON task."
                raise ValueError(msg)
        elif port is None:
            # For unnamed outputs, add to additional_retrieve_list
            # The output will be available through the 'retrieved' folder
            output_path = str(output.path)
            workgraph_task.inputs.metadata.options.additional_retrieve_list.value.append(
                output_path
            )
            # Use the 'retrieved' output socket instead of creating a new one
            # The file will be accessible through the retrieved FolderData
            output_socket = workgraph_task.outputs._sockets.get("retrieved")  # noqa: SLF001
            if output_socket is None:
                msg = "Output socket 'retrieved' was not found for ICON task."
                raise ValueError(msg)
        else:
            # Named ports should already exist (restart_file, finish_status, etc.)
            output_socket = workgraph_task.outputs._sockets.get(port)  # noqa: SLF001
            if output_socket is None:
                msg = f"Output socket '{port}' not found. Available: {list(workgraph_task.outputs._sockets.keys())}"
                raise ValueError(msg)

        self._aiida_socket_nodes[output_label] = output_socket

    @_link_output_node_to_task.register
    def _link_output_node_to_shell_task(
        self, task: core.ShellTask, _: str, output: core.GeneratedData
    ):
        """Links the output to the workgraph task."""
        workgraph_task = self.task_from_core(task)
        output_label = self.get_aiida_label_from_graph_item(output)

        if not isinstance(output, GeneratedData):
            msg = f"Only generated data may be specified as output but found output {output} of type {type(output)}"
            raise TypeError(msg)

        output_path = str(output.path)

        # Add to outputs list for retrieval
        current_outputs = workgraph_task.inputs.outputs.value or []
        if output_path not in current_outputs:
            current_outputs.append(output_path)
            workgraph_task.inputs.outputs.value = current_outputs

        # Format the output name according to ShellParser convention
        formatted_name = ShellParser.format_link_label(output_path)

        # Add output socket
        output_socket = workgraph_task.add_output("workgraph.any", formatted_name)
        # output_socket = workgraph_task.add_output_spec("workgraph.any", formatted_name)
        self._aiida_socket_nodes[output_label] = output_socket

    @functools.singledispatchmethod
    def _link_input_node_to_task(self, task: core.Task, port: str, input_: core.Data):  # noqa: ARG002
        """ "Dispatch linking input to task based on task type"""

        msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @_link_input_node_to_task.register
    def _link_input_node_to_icon_task(
        self, task: core.IconTask, port: str, input_: core.Data
    ):
        """Links the input to the workgraph shell task."""

        workgraph_task = self.task_from_core(task)

        # resolve data
        if isinstance(input_, core.AvailableData):
            setattr(workgraph_task.inputs, f"{port}", self.data_from_core(input_))
        elif isinstance(input_, core.GeneratedData):
            setattr(workgraph_task.inputs, f"{port}", self.socket_from_core(input_))
        else:
            raise TypeError

    @_link_input_node_to_task.register
    def _link_input_node_to_shell_task(
        self, task: core.ShellTask, _: str, input_: core.Data
    ):
        """Links the input to the workgraph shell task."""
        workgraph_task = self.task_from_core(task)
        input_label = self.get_aiida_label_from_graph_item(input_)

        # Add input socket if it doesn't exist
        # workgraph_task.add_input_spec("workgraph.any", f"nodes.{input_label}")
        workgraph_task.add_input("workgraph.any", f"nodes.{input_label}")

        # resolve data
        if isinstance(input_, core.AvailableData):
            socket = getattr(workgraph_task.inputs.nodes, input_label)
            socket.value = self.data_from_core(input_)
        elif isinstance(input_, core.GeneratedData):
            self._workgraph.add_link(
                self.socket_from_core(input_),
                workgraph_task.inputs[f"nodes.{input_label}"],
            )
        else:
            raise TypeError(f"Unexpected input type: {type(input_)}")

    def _link_wait_on_to_task(self, task: core.Task):
        """link wait on tasks to workgraph task"""

        workgraph_task = self.task_from_core(task)
        workgraph_task.waiting_on.clear()
        workgraph_task.waiting_on.add([self.task_from_core(wt) for wt in task.wait_on])

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

    def _set_shelljob_arguments(self, task: core.ShellTask):
        """Set AiiDA ShellJob arguments by replacing port placeholders with AiiDA labels."""
        workgraph_task = self.task_from_core(task)

        # Build input_labels dictionary for port resolution
        input_labels: dict[str, list[str]] = {}
        for port_name, input_list in task.inputs.items():
            input_labels[port_name] = []
            for input_ in input_list:
                input_label = self.get_aiida_label_from_graph_item(input_)
                input_labels[port_name].append(f"{{{input_label}}}")

        # Resolve the command with port placeholders replaced by input labels
        _, arguments_str = self.split_cmd_arg(task.resolve_ports(input_labels))

        # Update the task's arguments input
        workgraph_task.inputs.arguments.value = arguments_str

    def _set_shelljob_filenames(self, task: core.ShellTask):
        """Set AiiDA ShellJob filenames for data entities, including parameterized data."""
        workgraph_task = self.task_from_core(task)

        # Check if filenames input exists
        if not hasattr(workgraph_task.inputs, "filenames"):
            return

        filenames = {}

        # Handle input files
        for input_ in task.input_data_nodes():
            input_label = self.get_aiida_label_from_graph_item(input_)

            if isinstance(input_, core.AvailableData):
                filename = input_.path.name
                filenames[input_.name] = filename
            elif isinstance(input_, core.GeneratedData):
                same_name_count = sum(
                    1 for inp in task.input_data_nodes() if inp.name == input_.name
                )

                if same_name_count > 1:
                    filename = input_label
                else:
                    filename = (
                        input_.path.name if input_.path is not None else input_.name
                    )

                filenames[input_label] = filename
            else:
                msg = f"Found input {input_} of type {type(input_)} but only 'AvailableData' and 'GeneratedData' are supported."
                raise TypeError(msg)

        if filenames:
            workgraph_task.inputs.filenames.value = filenames

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

    def _calculate_task_depths(self) -> dict[str, int]:
        """Calculate the dependency depth for each task.

        Depth is the length of the longest dependency chain from a root task.
        Root tasks (no dependencies) have depth 0.
        """
        depths: dict[str, int] = {}

        def get_depth(task: core.Task) -> int:
            task_label = self.get_aiida_label_from_graph_item(task)
            if task_label in depths:
                return depths[task_label]

            # If no dependencies, depth is 0
            if not task.wait_on:
                depths[task_label] = 0
                return 0

            # Otherwise, depth is 1 + max depth of dependencies
            max_dep_depth = max(get_depth(dep_task) for dep_task in task.wait_on)
            depths[task_label] = max_dep_depth + 1
            return depths[task_label]

        # Calculate depths for all tasks
        for task in self._core_workflow.tasks:
            get_depth(task)

        return depths

    def _apply_slurm_dependencies(self):
        """Apply SLURM job dependencies for pre-submission of task chains."""
        from aiida_workgraph import task as wg_task
        from typing import Annotated

        # Calculate task depths
        task_depths = self._calculate_task_depths()

        # Group tasks by depth
        tasks_by_depth: dict[int, list[core.Task]] = {}
        for task in self._core_workflow.tasks:
            task_label = self.get_aiida_label_from_graph_item(task)
            depth = task_depths[task_label]
            if depth not in tasks_by_depth:
                tasks_by_depth[depth] = []
            tasks_by_depth[depth].append(task)

        # For each task, if its dependencies are within pre_submission_depth,
        # wrap it with SLURM dependency logic
        for task in self._core_workflow.tasks:
            task_label = self.get_aiida_label_from_graph_item(task)
            task_depth = task_depths[task_label]

            # Check if any dependency needs SLURM linking
            deps_to_link = []
            for dep_task in task.wait_on:
                dep_label = self.get_aiida_label_from_graph_item(dep_task)
                dep_depth = task_depths[dep_label]
                # If dependency is within depth range, add SLURM dependency
                if task_depth - dep_depth <= self._pre_submission_depth:
                    deps_to_link.append(dep_task)

            if deps_to_link:
                self._wrap_task_with_slurm_dependency(task, deps_to_link)

    def _wrap_task_with_slurm_dependency(self, task: core.Task, dependency_tasks: list[core.Task]):
        """Enable SLURM job dependencies for pre-submission.

        This method:
        1. Creates get_job_id tasks to monitor SLURM job IDs from dependencies
        2. Creates a task to build the SLURM dependency command string
        3. Injects the dependency command into the task's custom_scheduler_commands
        4. Removes WorkGraph waiting (so SLURM handles the dependency instead)
        """
        workgraph_task = self.task_from_core(task)
        task_label = self.get_aiida_label_from_graph_item(task)

        # Create get_job_id monitoring tasks for each dependency
        job_id_outputs = {}
        for dep_task in dependency_tasks:
            dep_label = self.get_aiida_label_from_graph_item(dep_task)

            if dep_label not in self._job_id_tasks:
                job_id_task = self._workgraph.add_task(
                    get_job_id,
                    name=f"get_job_id_{dep_label}",
                    workgraph_name=self._core_workflow.name,
                    task_name=dep_label,
                )
                self._job_id_tasks[dep_label] = job_id_task

                # Make get_job_id wait for the dependency task to be submitted
                dep_workgraph_task = self.task_from_core(dep_task)
                job_id_task.waiting_on.add(dep_workgraph_task)

            job_id_outputs[dep_label] = self._job_id_tasks[dep_label].outputs.result

        # Create a task to build the SLURM dependency string
        @task(outputs=[{"name": "slurm_cmd"}])
        def build_slurm_dep_cmd(**job_ids):
            """Build SLURM dependency command from job IDs."""
            if job_ids:
                valid_ids = [str(jid) for jid in job_ids.values() if jid is not None]
                if valid_ids:
                    return {"slurm_cmd": f"#SBATCH --dependency=afterok:{':'.join(valid_ids)}"}
            return {"slurm_cmd": ""}

        # Add the dependency builder task
        dep_cmd_task = self._workgraph.add_task(
            build_slurm_dep_cmd,
            name=f"slurm_dep_cmd_{task_label}",
            **job_id_outputs
        )

        # Create a task to inject the SLURM command into metadata
        @task(outputs=[{"name": "updated_metadata"}])
        def inject_slurm_dep(metadata: dict, slurm_cmd: str):
            """Inject SLURM dependency into task metadata."""
            if slurm_cmd:
                current_cmds = metadata.get("options", {}).get("custom_scheduler_commands", "")
                if current_cmds and not current_cmds.endswith("\n"):
                    current_cmds += "\n"
                if "options" not in metadata:
                    metadata["options"] = {}
                metadata["options"]["custom_scheduler_commands"] = current_cmds + slurm_cmd
            return {"updated_metadata": metadata}

        # Add the injection task
        inject_task = self._workgraph.add_task(
            inject_slurm_dep,
            name=f"inject_slurm_{task_label}",
            metadata=workgraph_task.inputs.metadata.value,
            slurm_cmd=dep_cmd_task.outputs.slurm_cmd
        )

        # Link updated metadata back to the task
        # Note: This approach assumes we can update metadata dynamically
        # workgraph_task.inputs.metadata = inject_task.outputs.updated_metadata

        # Make the task wait for the injection to complete
        workgraph_task.waiting_on.add(inject_task)

        # Remove direct WorkGraph dependencies (SLURM will handle them)
        for dep_task in dependency_tasks:
            dep_workgraph_task = self.task_from_core(dep_task)
            if dep_workgraph_task in workgraph_task.waiting_on:
                workgraph_task.waiting_on.remove(dep_workgraph_task)

    def run(
        self,
        inputs: None | dict[str, Any] = None,
        metadata: None | dict[str, Any] = None,
    ) -> aiida.orm.Node:
        self._workgraph.run(inputs=inputs, metadata=metadata)
        if (output_node := self._workgraph.process) is None:
            # The node should not be None after a run, it should contain exit code and message so if the node is None something internal went wrong
            msg = "Something went wrong when running workgraph. Please contact a developer."
            raise RuntimeError(msg)
        return output_node

    def submit(
        self,
        *,
        inputs: None | dict[str, Any] = None,
        wait: bool = False,
        timeout: int = 60,
        metadata: None | dict[str, Any] = None,
    ) -> aiida.orm.Node:
        self._workgraph.submit(
            inputs=inputs, wait=wait, timeout=timeout, metadata=metadata
        )
        if (output_node := self._workgraph.process) is None:
            # The node should not be None after a run, it should contain exit code and message so if the node is None something internal went wrong
            msg = "Something went wrong when running workgraph. Please contact a developer."
            raise RuntimeError(msg)
        return output_node
