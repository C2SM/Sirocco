from __future__ import annotations

import functools
import io
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias

import aiida.common
import aiida.orm
import aiida.transports
import aiida_workgraph  # type: ignore[import-untyped] # does not have proper typing and stubs
import aiida_workgraph.tasks.factory.shelljob_task  # type: ignore[import-untyped]  # is only for a workaround
from aiida.common.exceptions import NotExistent
from aiida_icon.calculations import IconCalculation
from aiida_shell.parsers.shell import ShellParser

from sirocco import core

if TYPE_CHECKING:
    from aiida_workgraph.socket import TaskSocket  # type: ignore[import-untyped]
    from aiida_workgraph.sockets.builtins import SocketAny

    WorkgraphDataNode: TypeAlias = aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData


# This is a workaround required when splitting the initialization of the task and its linked nodes Merging this into
# aiida-workgraph properly would require significant changes see issues
# https://github.com/aiidateam/aiida-workgraph/issues/168 The function is a copy of the original function in
# aiida-workgraph. The modifications are marked by comments.
def _execute(self, engine_process, args=None, kwargs=None, var_kwargs=None):  # noqa: ARG001 # unused arguments need name because the name is given as keyword in usage
    from aiida_shell import ShellJob
    from aiida_workgraph.utils import create_and_pause_process  # type: ignore[import-untyped]

    inputs = aiida_workgraph.tasks.factory.shelljob_task.prepare_for_shell_task(kwargs)

    # Workaround starts here
    # This part is part of the workaround. We need to manually add the outputs from the task.
    # Because kwargs are not populated with outputs
    default_outputs = {
        "remote_folder",
        "remote_stash",
        "retrieved",
        "_outputs",
        "_wait",
        "stdout",
        "stderr",
    }
    task_outputs = set(self.outputs._sockets.keys())  # noqa SLF001 # there so public accessor
    task_outputs = task_outputs.union(set(inputs.pop("outputs", [])))
    missing_outputs = task_outputs.difference(default_outputs)
    inputs["outputs"] = list(missing_outputs)
    # Workaround ends here

    inputs["metadata"].update({"call_link_label": self.name})
    if self.action == "PAUSE":
        engine_process.report(f"Task {self.name} is created and paused.")
        process = create_and_pause_process(
            engine_process.runner,
            ShellJob,
            inputs,
            state_msg="Paused through WorkGraph",
        )
        state = "CREATED"
        process = process.node
    else:
        process = engine_process.submit(ShellJob, **inputs)
        state = "RUNNING"
    process.label = self.name

    return process, state


aiida_workgraph.tasks.factory.shelljob_task.ShellJobTask.execute = _execute


class AiidaWorkGraph:
    def __init__(self, core_workflow: core.Workflow):
        # the core workflow that unrolled the time constraints for the whole graph
        self._core_workflow = core_workflow

        self._validate_workflow()

        self._workgraph = aiida_workgraph.WorkGraph(core_workflow.name)

        # stores the input data available on initialization
        self._aiida_data_nodes: dict[str, WorkgraphDataNode] = {}
        # stores the outputs sockets of tasks
        self._aiida_socket_nodes: dict[str, TaskSocket] = {}
        self._aiida_task_nodes: dict[str, aiida_workgraph.Task] = {}

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
            f"{obj.name}" + "__".join(f"_{key}_{value}" for key, value in obj.coordinates.items())
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

    def data_from_core(self, core_available_data: core.AvailableData) -> WorkgraphDataNode:
        return self._aiida_data_nodes[self.get_aiida_label_from_graph_item(core_available_data)]

    def socket_from_core(self, core_generated_data: core.GeneratedData) -> TaskSocket:
        return self._aiida_socket_nodes[self.get_aiida_label_from_graph_item(core_generated_data)]

    def task_from_core(self, core_task: core.Task) -> aiida_workgraph.Task:
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
            if not transport.path_exists(str(data.src)):
                msg = f"Could not find available data {data.name} in path {data.src} on computer {data.computer}."
                raise FileNotFoundError(msg)

        if computer.get_transport_class() is aiida.transports.plugins.local.LocalTransport:
            if data.src.is_file():
                self._aiida_data_nodes[label] = aiida.orm.SinglefileData(file=str(data.src), label=label)
            else:
                self._aiida_data_nodes[label] = aiida.orm.FolderData(tree=str(data.src), label=label)

        else:
            self._aiida_data_nodes[label] = aiida.orm.RemoteData(
                remote_path=str(data.src), label=label, computer=computer
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
        label = self.get_aiida_label_from_graph_item(task)
        # Split command line between command and arguments (this is required by aiida internals)
        cmd, _ = self.split_cmd_arg(task.command)

        from aiida_shell import ShellCode

        label_uuid = str(uuid.uuid4())

        code = ShellCode(
            label=f"{cmd}-{label_uuid}",
            computer=aiida.orm.load_computer(task.computer),
            filepath_executable=cmd,
            default_calc_job_plugin="core.shell",
            use_double_quotes=True,
        ).store()

        metadata: dict[str, Any] = {}
        # Files that are sourced before the execution of the script
        env_source_paths = [Path(env_source_file) for env_source_file in task.env_source_files]
        prepend_text = "\n".join([f"source {env_source_path}" for env_source_path in env_source_paths])

        metadata["options"] = {"prepend_text": prepend_text}
        # NOTE: Hardcoded for now, possibly make user-facing option (see issue #159)
        metadata["options"]["use_symlinks"] = True

        ## computer
        if task.computer is not None:
            try:
                metadata["computer"] = aiida.orm.load_computer(task.computer)
            except NotExistent as err:
                msg = f"Could not find computer {task.computer} for task {task}."
                raise ValueError(msg) from err

        # NOTE: The input and output nodes of the task are populated in a separate function
        nodes = {}
        # We need to add the files to nodes to copy it to remote
        if task.src is not None:
            nodes[f"SCRIPT__{label}"] = aiida.orm.SinglefileData(str(task.src))

        workgraph_task = self._workgraph.add_task(
            "workgraph.shelljob",
            name=label,
            nodes=nodes,
            command=code,
            arguments="",
            outputs=[],
            metadata=metadata,
        )

        self._aiida_task_nodes[label] = workgraph_task

    @create_task_node.register
    def _create_icon_task_node(self, task: core.IconTask):
        task_label = self.get_aiida_label_from_graph_item(task)

        try:
            # PRCOMMENT move to parsing? But then it has aiida logic
            computer = aiida.orm.Computer.collection.get(label=task.computer)
        except NotExistent as err:
            msg = f"Could not find computer {task.computer!r} in AiiDA database. One needs to create and configure the computer before running a workflow."
            raise ValueError(msg) from err

        label_uuid = str(uuid.uuid4())
        icon_code = aiida.orm.InstalledCode(
            label=f"icon-{label_uuid}",
            description="aiida_icon",
            default_calc_job_plugin="icon.icon",
            computer=computer,
            filepath_executable=str(task.bin),
            with_mpi=False,
            use_double_quotes=True,
        ).store()

        builder = IconCalculation.get_builder()
        builder.code = icon_code
        builder.metadata.options.additional_retrieve_list = []  # type: ignore[attr-defined] # aiida creates attrs automatically overwriting __getattr__

        task.update_icon_namelists_from_workflow()

        with io.StringIO() as buffer:
            task.master_namelist.namelist.write(buffer)
            buffer.seek(0)
            builder.master_namelist = aiida.orm.SinglefileData(buffer, task.master_namelist.name)

        with io.StringIO() as buffer:
            task.model_namelist.namelist.write(buffer)
            buffer.seek(0)
            builder.model_namelist = aiida.orm.SinglefileData(buffer, task.model_namelist.name)

        self._aiida_task_nodes[task_label] = self._workgraph.add_task(builder)

    @functools.singledispatchmethod
    def _link_output_node_to_task(self, task: core.Task, port: str, output: core.Data):  # noqa: ARG002
        """Dispatch linking input to task based on task type."""

        msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @_link_output_node_to_task.register
    def _link_output_node_to_shell_task(self, task: core.ShellTask, _: str, output: core.Data):
        """Links the output to the workgraph task."""

        workgraph_task = self.task_from_core(task)
        output_label = self.get_aiida_label_from_graph_item(output)
        output_socket = workgraph_task.add_output("workgraph.any", str(output.src))
        self._aiida_socket_nodes[output_label] = output_socket

    @_link_output_node_to_task.register
    def _link_output_node_to_icon_task(self, task: core.IconTask, port: str | None, output: core.Data):
        workgraph_task = self.task_from_core(task)
        output_label = self.get_aiida_label_from_graph_item(output)

        if port is None:
            # To avoid nested namespaces due to dots in name
            output_socket = workgraph_task.add_output("workgraph.any", ShellParser.format_link_label(str(output.src)))

            workgraph_task.inputs.metadata.options.additional_retrieve_list.value.append(str(output.src))
        else:
            output_socket = workgraph_task.outputs._sockets.get(port)  # noqa SLF001 # there so public accessor

        if output_socket is None:
            msg = f"Output socket {output_label!r} was not successfully created. Please contact a developer."
            raise ValueError(msg)
        self._aiida_socket_nodes[output_label] = output_socket

    @functools.singledispatchmethod
    def _link_input_node_to_task(self, task: core.Task, port: str, input_: core.Data):  # noqa: ARG002
        """ "Dispatch linking input to task based on task type"""

        msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @_link_input_node_to_task.register
    def _link_input_node_to_shell_task(self, task: core.ShellTask, _: str, input_: core.Data):
        """Links the input to the workgraph shell task."""

        workgraph_task = self.task_from_core(task)
        input_label = self.get_aiida_label_from_graph_item(input_)
        workgraph_task.add_input("workgraph.any", f"nodes.{input_label}")

        # resolve data
        if isinstance(input_, core.AvailableData):
            if not hasattr(workgraph_task.inputs.nodes, f"{input_label}"):
                msg = f"Socket {input_label!r} was not found in workgraph. Please contact a developer."
                raise ValueError(msg)
            socket = getattr(workgraph_task.inputs.nodes, f"{input_label}")
            socket.value = self.data_from_core(input_)
        elif isinstance(input_, core.GeneratedData):
            self._workgraph.add_link(
                self.socket_from_core(input_),
                workgraph_task.inputs[f"nodes.{input_label}"],
            )
        else:
            raise TypeError

    @_link_input_node_to_task.register
    def _link_input_node_to_icon_task(self, task: core.IconTask, port: str, input_: core.Data):
        """Links the input to the workgraph shell task."""

        workgraph_task = self.task_from_core(task)

        # resolve data
        if isinstance(input_, core.AvailableData):
            setattr(workgraph_task.inputs, f"{port}", self.data_from_core(input_))
        elif isinstance(input_, core.GeneratedData):
            setattr(workgraph_task.inputs, f"{port}", self.socket_from_core(input_))
        else:
            raise TypeError

    def _link_wait_on_to_task(self, task: core.Task):
        """link wait on tasks to workgraph task"""

        workgraph_task = self.task_from_core(task)
        workgraph_task.waiting_on.clear()
        workgraph_task.waiting_on.add([self.task_from_core(wt) for wt in task.wait_on])

    def _set_shelljob_arguments(self, task: core.ShellTask):
        """Set AiiDA ShellJob arguments by replacing port placeholders with AiiDA labels."""
        workgraph_task = self.task_from_core(task)
        workgraph_task_arguments: SocketAny = workgraph_task.inputs.arguments

        if workgraph_task_arguments is None:
            msg = (
                f"Workgraph task {workgraph_task.name!r} did not initialize arguments nodes in the workgraph "
                f"before linking. This is a bug in the code, please contact developers."
            )
            raise ValueError(msg)

        # Build input_labels dictionary for port resolution
        input_labels: dict[str, list[str]] = {}
        for port_name, input_list in task.inputs.items():
            input_labels[port_name] = []
            for input_ in input_list:
                # Use the full AiiDA label as the placeholder content
                input_label = self.get_aiida_label_from_graph_item(input_)
                input_labels[port_name].append(f"{{{input_label}}}")

        # Resolve the command with port placeholders replaced by input labels
        _, arguments = self.split_cmd_arg(task.resolve_ports(input_labels))
        workgraph_task_arguments.value = arguments

    def _set_shelljob_filenames(self, task: core.ShellTask):
        """Set AiiDA ShellJob filenames for data entities, including parameterized data."""
        filenames = {}
        workgraph_task = self.task_from_core(task)

        if not workgraph_task.inputs.filenames:
            return

        # Handle input files
        for input_ in task.input_data_nodes():
            input_label = self.get_aiida_label_from_graph_item(input_)

            if isinstance(input_, core.AvailableData):
                filename = input_.src.name
                filenames[input_.name] = filename
            elif isinstance(input_, core.GeneratedData):
                # We need to handle parameterized data in this case.
                # Importantly, multiple data nodes with the same base name but different
                # coordinates need unique filenames to avoid conflicts in the working directory

                # Count how many inputs have the same base name
                same_name_count = sum(1 for inp in task.input_data_nodes() if inp.name == input_.name)

                # NOTE: One could also always use the `input_label` consistently here and remove the if-else
                # to obtain more predictable labels, which, however, might be unnecessarily lengthy.
                # To be thought about...
                if same_name_count > 1:
                    # Multiple data nodes with same base name - use full label as filename
                    # to ensure uniqueness in working directory
                    filename = input_label
                else:
                    # Single data node with this name - can use simple filename
                    filename = input_.src.name if input_.src is not None else input_.name

                # The key in filenames dict should be the input label (what's used in nodes dict)
                filenames[input_label] = filename
            else:
                msg = f"Found output {input_} of type {type(input_)} but only 'AvailableData' and 'GeneratedData' are supported."
                raise TypeError(msg)

        workgraph_task.inputs.filenames.value = filenames

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
        self._workgraph.submit(inputs=inputs, wait=wait, timeout=timeout, metadata=metadata)
        if (output_node := self._workgraph.process) is None:
            # The node should not be None after a run, it should contain exit code and message so if the node is None something internal went wrong
            msg = "Something went wrong when running workgraph. Please contact a developer."
            raise RuntimeError(msg)
        return output_node
