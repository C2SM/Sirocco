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
from aiida_workgraph import WorkGraph, Task

from sirocco import core
from sirocco.core.graph_items import GeneratedData
from sirocco.parsing._utils import TimeUtils

if TYPE_CHECKING:
    from aiida_workgraph.socket import TaskSocket  # type: ignore[import-untyped]

    WorkgraphDataNode: TypeAlias = (
        aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData
    )


class AiidaWorkGraph:
    def __init__(self, core_workflow: core.Workflow):
        # the core workflow that unrolled the time constraints for the whole graph
        self._core_workflow = core_workflow

        self._validate_workflow()

        self._workgraph = WorkGraph(core_workflow.name)

        # stores the input data available on initialization
        self._aiida_data_nodes: dict[str, WorkgraphDataNode] = {}
        # stores the outputs sockets of tasks
        self._aiida_socket_nodes: dict[str, TaskSocket] = {}
        self._aiida_task_nodes: dict[str, Task] = {}

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
        icon_label = f"icon@{computer.label}"
        try:
            icon_code = aiida.orm.load_code(icon_label)
        except NotExistent:
            icon_code = aiida.orm.InstalledCode(
                label=icon_label,
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
