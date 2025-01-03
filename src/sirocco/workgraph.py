from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiida.common
import aiida.orm
import aiida_workgraph.engine.utils  # type: ignore[import-untyped]
from aiida.common.exceptions import NotExistent
from aiida_workgraph import WorkGraph

from sirocco.core._tasks.icon_task import IconTask
from sirocco.core._tasks.shell_task import ShellTask

if TYPE_CHECKING:
    from aiida_workgraph.socket import TaskSocket  # type: ignore[import-untyped]

    from sirocco import core
    from sirocco.core import graph_items


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
    # Workaround ends here

    return inputs


aiida_workgraph.engine.utils.prepare_for_shell_task = _prepare_for_shell_task


class AiidaWorkGraph:
    def __init__(self, core_workflow: core.Workflow):
        # the core workflow that unrolled the time constraints for the whole graph
        self._core_workflow = core_workflow

        self._validate_workflow()

        self._workgraph = WorkGraph(core_workflow.name)

        # stores the input data available on initialization
        self._aiida_data_nodes: dict[str, aiida_workgraph.orm.Data] = {}
        # stores the outputs sockets of tasks
        self._aiida_socket_nodes: dict[str, TaskSocket] = {}
        self._aiida_task_nodes: dict[str, aiida_workgraph.Task] = {}

        self._add_available_data()
        self._add_tasks()

    def _validate_workflow(self):
        """Checks if the core workflow uses for its tasks and data valid names for AiiDA."""
        for task in self._core_workflow.tasks:
            try:
                aiida.common.validate_link_label(task.name)
            except ValueError as exception:
                msg = f"Raised error when validating task name '{task.name}': {exception.args[0]}"
                raise ValueError(msg) from exception
            for input_ in task.inputs:
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

    def _add_available_data(self):
        """Adds the available data on initialization to the workgraph"""
        for task in self._core_workflow.tasks:
            for input_ in task.inputs:
                if input_.available:
                    self._add_aiida_input_data_node(task, input_)

    @staticmethod
    def replace_invalid_chars_in_label(label: str) -> str:
        """Replaces chars in the label that are invalid for AiiDA.

        The invalid chars ["-", " ", ":", "."] are replaced with underscores.
        """
        invalid_chars = ["-", " ", ":", "."]
        for invalid_char in invalid_chars:
            label = label.replace(invalid_char, "_")
        return label

    @staticmethod
    def get_aiida_label_from_graph_item(obj: graph_items.GraphItem) -> str:
        """Returns a unique AiiDA label for the given graph item.

        The graph item object is uniquely determined by its name and its coordinates. There is the possibility that
        through the replacement of invalid chars in the coordinates duplication can happen but it is unlikely.
        """
        return AiidaWorkGraph.replace_invalid_chars_in_label(
            f"{obj.name}" + "__".join(f"_{key}_{value}" for key, value in obj.coordinates.items())
        )

    def _add_aiida_input_data_node(self, task: graph_items.Task, input_: graph_items.Data):
        """
        Create an `aiida.orm.Data` instance from the provided graph item.
        """
        label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)
        input_path = Path(input_.src)
        input_full_path = input_.src if input_path.is_absolute() else task.config_rootdir / input_path

        if input_.type == "file":
            self._aiida_data_nodes[label] = aiida.orm.SinglefileData(label=label, file=input_full_path)
        elif input_.type == "dir":
            self._aiida_data_nodes[label] = aiida.orm.FolderData(label=label, tree=input_full_path)
        else:
            msg = f"Data type {input_.type!r} not supported. Please use 'file' or 'dir'."
            raise ValueError(msg)

    def _add_tasks(self):
        """Creates the AiiDA task nodes from the `GraphItem.Task`s in the core workflow.

        This includes the linking of all input and output nodes, the arguments and wait_on tasks
        """
        for task in self._core_workflow.tasks:
            self._create_task_node(task)

        # NOTE: The wait on tasks has to be added after the creation of the tasks because it might reference tasks from
        # the future
        for task in self._core_workflow.tasks:
            self._link_wait_on_to_task(task)

        for task in self._core_workflow.tasks:
            for output in task.outputs:
                self._link_output_nodes_to_task(task, output)
            for input_ in task.inputs:
                self._link_input_nodes_to_task(task, input_)
            self._link_arguments_to_task(task)

    def _create_task_node(self, task: graph_items.Task):
        label = AiidaWorkGraph.get_aiida_label_from_graph_item(task)
        if isinstance(task, ShellTask):
            command_path = Path(task.command)
            command_full_path = task.command if command_path.is_absolute() else task.config_rootdir / command_path
            command = str(command_full_path)

            # metadata
            metadata = {}
            ## Source file
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
                arguments=[],
                outputs=[],
                metadata=metadata,
            )

            self._aiida_task_nodes[label] = workgraph_task

        elif isinstance(task, IconTask):
            exc = "IconTask not implemented yet."
            raise NotImplementedError(exc)
        else:
            exc = f"Task: {task.name} not implemented yet."
            raise NotImplementedError(exc)

    def _link_wait_on_to_task(self, task: graph_items.Task):
        label = AiidaWorkGraph.get_aiida_label_from_graph_item(task)
        workgraph_task = self._aiida_task_nodes[label]
        wait_on_tasks = []
        for wait_on in task.wait_on:
            wait_on_task_label = AiidaWorkGraph.get_aiida_label_from_graph_item(wait_on)
            wait_on_tasks.append(self._aiida_task_nodes[wait_on_task_label])
        workgraph_task.wait = wait_on_tasks

    def _link_input_nodes_to_task(self, task: graph_items.Task, input_: graph_items.Data):
        """Links the input to the workgraph task."""
        task_label = AiidaWorkGraph.get_aiida_label_from_graph_item(task)
        input_label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)
        workgraph_task = self._aiida_task_nodes[task_label]
        workgraph_task.add_input("workgraph.any", f"nodes.{input_label}")

        # resolve data
        if (data_node := self._aiida_data_nodes.get(input_label)) is not None:
            if not hasattr(workgraph_task.inputs.nodes, f"{input_label}"):
                msg = f"Socket {input_label!r} was not found in workgraph. Please contact a developer."
                raise ValueError(msg)
            socket = getattr(workgraph_task.inputs.nodes, f"{input_label}")
            socket.value = data_node
        elif (output_socket := self._aiida_socket_nodes.get(input_label)) is not None:
            self._workgraph.add_link(output_socket, workgraph_task.inputs[f"nodes.{input_label}"])
        else:
            msg = (
                f"Input data node {input_label!r} was neither found in socket nodes nor in data nodes. The task "
                f"{task_label!r} must have dependencies on inputs before they are created."
            )
            raise ValueError(msg)

    def _link_arguments_to_task(self, task: graph_items.Task):
        """Links the arguments to the workgraph task.

        Parses `cli_arguments` of the graph item task and links all arguments to the task node. It only adds arguments
        corresponding to inputs if they are contained in the task.
        """
        task_label = AiidaWorkGraph.get_aiida_label_from_graph_item(task)
        workgraph_task = self._aiida_task_nodes[task_label]
        if (workgraph_task_arguments := workgraph_task.inputs.arguments) is None:
            msg = (
                f"Workgraph task {workgraph_task.name!r} did not initialize arguments nodes in the workgraph "
                f"before linking. This is a bug in the code, please contact developers."
            )
            raise ValueError(msg)

        name_to_input_map = {input_.name: input_ for input_ in task.inputs}
        # we track the linked input arguments, to ensure that all linked input nodes got linked arguments
        linked_input_args = []
        for arg in task.cli_arguments:
            if arg.references_data_item:
                # We only add an input argument to the args if it has been added to the nodes
                # This ensures that inputs and their arguments are only added
                # when the time conditions are fulfilled
                if (input_ := name_to_input_map.get(arg.name)) is not None:
                    input_label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)

                    if arg.cli_option_of_data_item is not None:
                        workgraph_task_arguments.value.append(f"{arg.cli_option_of_data_item}")
                    workgraph_task_arguments.value.append(f"{{{input_label}}}")
                    linked_input_args.append(input_.name)
            else:
                workgraph_task_arguments.value.append(f"{arg.name}")
        # Adding remaining input nodes as positional arguments
        for input_name in name_to_input_map:
            if input_name not in linked_input_args:
                input_ = name_to_input_map[input_name]
                input_label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)
                workgraph_task_arguments.value.append(f"{{{input_label}}}")

    def _link_output_nodes_to_task(self, task: graph_items.Task, output: graph_items.Data):
        """Links the output to the workgraph task."""

        workgraph_task = self._aiida_task_nodes[AiidaWorkGraph.get_aiida_label_from_graph_item(task)]
        output_label = AiidaWorkGraph.get_aiida_label_from_graph_item(output)
        output_socket = workgraph_task.add_output("workgraph.any", output.src)
        self._aiida_socket_nodes[output_label] = output_socket

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
