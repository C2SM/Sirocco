from __future__ import annotations

import functools
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias

import aiida.common
import aiida.orm
import aiida_workgraph.engine.utils  # type: ignore[import-untyped]
from aiida.common.exceptions import NotExistent
from aiida_workgraph import WorkGraph

from sirocco import core

if TYPE_CHECKING:
    from aiida_workgraph.socket import TaskSocket  # type: ignore[import-untyped]
    from aiida_workgraph.sockets.builtins import SocketAny

    WorkgraphDataNode: TypeAlias = aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData


# This is a workaround required when splitting the initialization of the task and its linked nodes Merging this into
# aiida-workgraph properly would require significant changes see issues
# https://github.com/aiidateam/aiida-workgraph/issues/168 The function is a copy of the original function in
# aiida-workgraph. The modifications are marked by comments.
def _prepare_for_shell_task(task: dict, inputs: dict) -> dict:
    """Prepare the inputs for ShellJob"""
    import inspect

    from aiida_shell.launch import prepare_shell_job_inputs

    # Retrieve the signature of `prepare_shell_job_inputs` to determine expected input parameters.
    signature = inspect.signature(prepare_shell_job_inputs)
    aiida_shell_input_keys = signature.parameters.keys()

    # Iterate over all WorkGraph `inputs`, and extract the ones which are expected by `prepare_shell_job_inputs`
    inputs_aiida_shell_subset = {key: inputs[key] for key in inputs if key in aiida_shell_input_keys}

    try:
        aiida_shell_inputs = prepare_shell_job_inputs(**inputs_aiida_shell_subset)
    except ValueError:  # noqa: TRY302
        raise

    # We need to remove the original input-keys, as they might be offending for the call to `launch_shell_job`
    # E.g., `inputs` originally can contain `command`, which gets, however, transformed to #
    # `code` by `prepare_shell_job_inputs`
    for key in inputs_aiida_shell_subset:
        inputs.pop(key)

    # Finally, we update the original `inputs` with the modified ones from the call to `prepare_shell_job_inputs`
    inputs = {**inputs, **aiida_shell_inputs}
    inputs.setdefault("metadata", {})
    inputs["metadata"].update({"call_link_label": task["name"]})

    # Workaround starts here
    # This part is part of the workaround. We need to manually add the outputs from the task.
    # Because kwargs are not populated with outputs
    default_outputs = {"remote_folder", "remote_stash", "retrieved", "_outputs", "_wait", "stdout", "stderr"}
    task_outputs = set(task["outputs"].keys())
    task_outputs = task_outputs.union(set(inputs.pop("outputs", [])))
    missing_outputs = task_outputs.difference(default_outputs)
    inputs["outputs"] = list(missing_outputs)

    # NOTE: Hardcoded for now, possibly make user-facing option
    inputs["metadata"]["options"]["use_symlinks"] = True

    # Workaround ends here
    # FIXME: The `GeneratedData.name` ends up as the actual filename
    # try:
    #     computer = inputs['code'].computer
    #     with computer.get_transport() as transport:
    #         remote_is_file = transport.isfile(path=data.src)
    # except:
    #     raise

    # import ipdb; ipdb.set_trace()

    return inputs



aiida_workgraph.engine.utils.prepare_for_shell_task = _prepare_for_shell_task


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
        self._aiida_task_nodes: dict[str, aiida_workgraph.Task] = {}

        # create input data nodes
        for data in self._core_workflow.data:
            if isinstance(data, core.AvailableData):
                self._add_aiida_input_data_node(data)

        # create workgraph task nodes and output sockets
        for task in self._core_workflow.tasks:
            self.create_task_node(task)
            # Create and link corresponding output sockets
            for output in task.outputs:
                self._link_output_node_to_task(task, output)

        # link input nodes to workgraph tasks
        for task in self._core_workflow.tasks:
            for input_ in task.input_data_nodes():
                self._link_input_node_to_task(task, input_)

        # set shelljob arguments
        for task in self._core_workflow.tasks:
            if isinstance(task, core.ShellTask):
                self._set_shelljob_arguments(task)

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
            for output in task.outputs:
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

    def _add_aiida_input_data_node(self, data: core.Data):
        """
        Create an `aiida.orm.Data` instance from the provided graph item.
        """
        label = self.get_aiida_label_from_graph_item(data)
        data_path = Path(data.src)
        data_full_path = data.src if data_path.is_absolute() else self._core_workflow.config_rootdir / data_path

        # ? Explicitly check for data type "remote" ?
        if data.computer is not None:
            try:
                computer = aiida.orm.load_computer(data.computer)
            except NotExistent as err:
                msg = f"Could not find computer {data.computer!r} for input {data}."
                raise ValueError(msg) from err

            with computer.get_transport() as transport:
                remote_is_file = transport.isfile(path=data.src)

            # ? Is the `label` being used to construct the symlink
            # TODO: If file provided as input, set the filename as label
            # TODO: What to do for Folder
            if not remote_is_file:
                remote_data = aiida.orm.RemoteData(remote_path=data.src, label=label, computer=computer)
            else:
                # FIXME: Currently behavior of both cases is the same. Should this be handled in `aiida-shell` ???
                # remote_data = aiida.orm.RemoteData(remote_path=str(Path(data.src).parent), label=label, computer=computer)
                remote_data = aiida.orm.RemoteData(remote_path=data.src, label=label, computer=computer)

            self._aiida_data_nodes[label] = remote_data

        elif data.type == "file":
            self._aiida_data_nodes[label] = aiida.orm.SinglefileData(label=label, file=data_full_path)
        elif data.type == "dir":
            self._aiida_data_nodes[label] = aiida.orm.FolderData(label=label, tree=data_full_path)
        else:
            msg = f"Data type {data.type!r} not supported. Please use 'file' or 'dir'."
            raise ValueError(msg)

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
        cmd_path = Path(cmd)
        # FIXME: https://github.com/C2SM/Sirocco/issues/127
        if cmd_path.is_absolute():
            command = str(cmd_path)
        else:
            if task.src is None:
                msg = "src must be specified when command path is relative"
                raise ValueError(msg)
            command = str((task.config_rootdir / task.src).parent / cmd_path)

        # metadata
        metadata: dict[str, Any] = {}
        ## Source file
        # FIXME: Paths are resolved to paths on the local machine if not given as absolute paths. config_rootdir points
        # to local path, so this breaks on the remote
        env_source_paths = [
            env_source_path
            if (env_source_path := Path(env_source_file)).is_absolute()
            else (task.config_rootdir / env_source_path)
            for env_source_file in task.env_source_files
        ]
        prepend_text = "\n".join([f"source {env_source_path}" for env_source_path in env_source_paths])
        metadata["options"] = {"prepend_text": prepend_text}

        ## computer
        if task.computer is not None:
            try:
                metadata["computer"] = aiida.orm.load_computer(task.computer)
            except NotExistent as err:
                msg = f"Could not find computer {task.computer} for task {task}."
                raise ValueError(msg) from err

        # NOTE: We don't pass the `nodes` dictionary here, as then we would need to have the sockets available when
        # we create the task. Instead, they are being updated via the WG internals when linking inputs/outputs to
        # tasks
        workgraph_task = self._workgraph.add_task(
            "ShellJob",
            name=label,
            command=command,
            arguments="",
            outputs=[],
            metadata=metadata,
        )

        self._aiida_task_nodes[label] = workgraph_task

    def _link_output_node_to_task(self, task: core.Task, output: core.Data):
        """Links the output to the workgraph task."""

        workgraph_task = self.task_from_core(task)
        output_label = self.get_aiida_label_from_graph_item(output)
        output_socket = workgraph_task.add_output("workgraph.any", output.src)
        self._aiida_socket_nodes[output_label] = output_socket

    @functools.singledispatchmethod
    def _link_input_node_to_task(self, task: core.Task, input_: core.Data):  # noqa: ARG002
        """ "Dispatch linking input to task based on task type"""

        msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @_link_input_node_to_task.register
    def _link_input_node_to_shelltask(self, task: core.ShellTask, input_: core.Data):
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
            self._workgraph.add_link(self.socket_from_core(input_), workgraph_task.inputs[f"nodes.{input_label}"])
        else:
            raise TypeError

    def _link_wait_on_to_task(self, task: core.Task):
        """link wait on tasks to workgraph task"""

        self.task_from_core(task).wait = [self.task_from_core(wt) for wt in task.wait_on]

    def _set_shelljob_arguments(self, task: core.ShellTask):
        """set AiiDA ShellJob arguments by replacing port placeholders by aiida labels"""

        workgraph_task = self.task_from_core(task)
        workgraph_task_arguments: SocketAny = workgraph_task.inputs.arguments
        if workgraph_task_arguments is None:
            msg = (
                f"Workgraph task {workgraph_task.name!r} did not initialize arguments nodes in the workgraph "
                f"before linking. This is a bug in the code, please contact developers."
            )
            raise ValueError(msg)

        input_labels = {port: list(map(self.label_placeholder, task.inputs[port])) for port in task.inputs}
        _, arguments = self.split_cmd_arg(task.resolve_ports(input_labels))
        workgraph_task_arguments.value = arguments

        # filenames = []
        # FIXME: This currently breaks...
        # ipdb> input_socket
        # SocketAny(name='icon_restart_date_2026_01_01_00_00_00', value=None)
        # ipdb> workgraph_task
        # DecoratedNode(name='icon_date_2026_03_01_00_00_00', properties=[], inputs=['metadata', 'code', 'monitors', 'remote_folder', 'nodes', 'filenames', 'arguments', 'outputs', 'parser', '_wait', 'command', 'resolve_command'], outputs=['remote_folder', 'remote_stash', 'retrieved', '_wait', '_outputs', 'stdout', 'stderr', 'icon_output', 'restart'])
        # ipdb> workgraph_task.inputs
        # NodeSocketNamespace(name='inputs', sockets=['metadata', 'code', 'monitors', 'remote_folder', 'nodes', 'filenames', 'arguments', 'outputs', 'parser', '_wait', 'command', 'resolve_command'])
        # ipdb> workgraph_task.inputs.nodes
        # TaskSocketNamespace(name='nodes', sockets=['icon_namelist', 'icon_restart_date_2026_01_01_00_00_00'])
        # for input_socket in workgraph_task.inputs.nodes:
        #     try:
        #         aiida_data_node = self._aiida_data_nodes[input_socket._name]
        #     except:
        #         # Fails for non-existing data nodes
        #         # Somehow restrict to only GeneratedData
        #         continue
        #         # import ipdb; ipdb.set_trace()

        #     if isinstance(aiida_data_node, aiida.orm.FolderData):
        #         pass
        #     elif isinstance(aiida_data_node, aiida.orm.SinglefileData):
        #         filenames.append(aiida_data_node.filename)
        #         pass
        #     elif isinstance(aiida_data_node, aiida.orm.RemoteData):
        #         try:
        #             # This fails if it is a file
        #             # Could also use `.is_empty`, but seems more fragile
        #             aiida_data_node.listdir()
        #             remote_is_file = False
        #         except OSError:
        #             remote_is_file = True

        #         if remote_is_file:
        #             filenames.append(Path(aiida_data_node.get_remote_path()).name)
        #     else:
        #         raise Exception

        # setattr(workgraph_task.inputs, "filenames", filenames)

        # import ipdb; ipdb.set_trace()


    def run(
        self,
        inputs: None | dict[str, Any] = None,
        metadata: None | dict[str, Any] = None,
    ) -> dict[str, Any]:
        return self._workgraph.run(inputs=inputs, metadata=metadata)

    def submit(
        self,
        *,
        inputs: None | dict[str, Any] = None,
        wait: bool = False,
        timeout: int = 60,
        metadata: None | dict[str, Any] = None,
    ) -> dict[str, Any]:
        return self._workgraph.submit(inputs=inputs, wait=wait, timeout=timeout, metadata=metadata)
