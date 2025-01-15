from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiida.common
import aiida.orm
import aiida.plugins
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
        """Checks if the core workflow uses valid AiiDA names for its tasks and data."""
        for task in self._core_workflow.tasks:
            try:
                aiida.common.validate_link_label(task.name)
            except ValueError as exception:
                msg = f"Raised error when validating task name '{task.name}': {exception.args[0]}"
                raise ValueError(msg) from exception
            for input_, _ in task.inputs:
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
        for data in self._core_workflow.data:
            if data.available:
                self._add_aiida_input_data_node(data)

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

    def get_data_full_path(self, path : str | Path) -> Path:
        data_path = path if isinstance(path, str) else Path(path)
        return data_path if data_path.is_absolute() else self._core_workflow.config_rootdir / data_path

    def _add_aiida_input_data_node(self, data: graph_items.Data):
        """
        Create an `aiida.orm.Data` instance from the provided graph item.
        """
        label = AiidaWorkGraph.get_aiida_label_from_graph_item(data)
        data_path = Path(data.src)
        data_full_path = data.src if data_path.is_absolute() else self._core_workflow.config_rootdir / data_path

        if data.computer is not None:
            try:
                computer = aiida.orm.load_computer(data.computer)
            except NotExistent as err:
                msg = f"Could not find computer {data.computer!r} for input {data}."
                raise ValueError(msg) from err
            # TODO This only makes sense for localhost, maybe raise warning or error if relative path?
            self._aiida_data_nodes[label] = aiida.orm.RemoteData(remote_path=str(data_full_path), label=label, computer=computer)
        elif data.type == "file":
            self._aiida_data_nodes[label] = aiida.orm.SinglefileData(label=label, file=data_full_path)
        elif data.type == "dir":
            self._aiida_data_nodes[label] = aiida.orm.FolderData(label=label, tree=data_full_path)
        else:
            msg = f"Data type {data.type!r} not supported. Please use 'file' or 'dir'."
            raise ValueError(msg)

    def _add_tasks(self):
        """Creates the AiiDA task nodes from the `GraphItem.Task`s in the core workflow.

        This includes the linking of all input and output nodes, the arguments and wait_on tasks
        """
        for task in self._core_workflow.tasks:
            self._create_task_node(task)

        # NOTE: The wait on tasks has to be added after the creation of the tasks
        #       because it might reference tasks defined after the current one
        for task in self._core_workflow.tasks:
            self._link_wait_on_to_task(task)

        for task in self._core_workflow.tasks:
            for output in task.outputs:
                if isinstance(task, ShellTask):
                    # TODO naming, it is not really linking but rather creating
                    self._link_output_nodes_to_shell_task(task, output)
                elif isinstance(task, IconTask):
                    # TODO naming, this just adding the output to the storage timestamping it  
                    self._link_restart_file_to_icon_task(task)
                else:
                   raise ValueError(f"Provided task {task} is not supported. This should not happen. Please contact a developer.")
            for bound_data in task.inputs:
                self._link_input_nodes_to_task(task, bound_data)
            if isinstance(task, ShellTask):
                self._link_arguments_to_task(task)

    def _create_task_node(self, task: graph_items.Task):
        if isinstance(task, ShellTask):
            return self._create_shell_task_node(task) 
        elif isinstance(task, IconTask):
            return self._create_icon_task_node(task) 
        else:
            raise ValueError(f"Task type {type(task)} is not supported.")

    def _create_icon_task_node(self, task: IconTask):
        IconCalculation = aiida.plugins.CalculationFactory('icon.icon')
        task_label = AiidaWorkGraph.get_aiida_label_from_graph_item(task)

        try:
            icon_code = aiida.orm.load_code(task.code)
        except NotExistent as exc:
            raise ValueError(f"Could not find the code {task.code} in AiiDA. Did you create it beforehand?") from exc

        icon_task = self._workgraph.add_task(IconCalculation, code=icon_code)

        task.init_core_namelists()
        task.update_core_namelists_from_config()
        task.update_core_namelists_from_workflow()

        self._aiida_task_nodes[task_label] = icon_task
        # TODO remove
        #input_ = task.inputs[0][0]
        #input_label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)
        #icon_task.add_input("workgraph.Any", f"{input_label}")
        

        icon_task.inputs.master_namelist.value = aiida.orm.SinglefileData(self.get_data_full_path(task.master_namelist.path))
        icon_task.inputs.model_namelist.value = aiida.orm.SinglefileData(self.get_data_full_path(task.model_namelist.path))


        # TODO remove
        #builder = IconCalculation.get_builder()


        #builder.master_namelist = aiida.orm.SinglefileData(self.get_data_full_path(task.master_namelist.path))
        #builder.model_namelist = aiida.orm.SinglefileData(self.get_data_full_path(task.model_namelist.path))
        #for input_, port in task.inputs:
        #    if port == "restart_file":
        #        workgraph_task = self._aiida_task_nodes[task_label]
        #        workgraph_task.add_input("workgraph.any", f"nodes.{input_label}")

        #        ## resolve data
        #        #if (data_node := self._aiida_data_nodes.get(input_label)) is not None:
        #        #    if not hasattr(workgraph_task.inputs.nodes, f"{input_label}"):
        #        #        msg = f"Socket {input_label!r} was not found in workgraph. Please contact a developer."
        #        #        raise ValueError(msg)
        #        #    socket = getattr(workgraph_task.inputs.nodes, f"{input_label}")
        #        #    socket.value = data_node
        #        #elif (output_socket := self._aiida_socket_nodes.get(input_label)) is not None:
        #        #    self._workgraph.add_link(output_socket, workgraph_task.inputs[f"nodes.{input_label}"])
        #        #else:
        #        #    msg = (
        #        #        f"Input data node {input_label!r} was neither found in socket nodes nor in data nodes. The task "
        #        #        f"{task_label!r} must have dependencies on inputs before they are created."
        #        #    )
        #        #    raise ValueError(msg)

        #        builder.restart_file
        #        spec.input("restart_file", valid_type=orm.RemoteData, required=False)
        #    elif port == "wrapper_script":
        #        #spec.input("wrapper_script", valid_type=orm.SinglefileData, required=False)
        #        pass
        #    elif port == "dynamics_grid_file":
        #        spec.input( "dynamics_grid_file", valid_type=orm.RemoteData)
        #spec.input("ecrad_data", valid_type=orm.RemoteData)
        #spec.input("cloud_opt_props", valid_type=orm.RemoteData)
        #spec.input("dmin_wetgrowth_lookup", valid_type=orm.RemoteData)
        #spec.input("rrtmg_sw", valid_type=orm.RemoteData)
        #spec.output("latest_restart_file")
        #spec.output_namespace("all_restart_files", dynamic=True)
        #spec.output("finish_status")

        #node = submit(builder)


    def _create_shell_task_node(self, task: ShellTask):
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

    def _link_input_nodes_to_task(self, task: graph_items.Task, bound_data: graph_items.BoundData):
        """Links the input to the workgraph task."""
        input_, port = bound_data
        task_label = AiidaWorkGraph.get_aiida_label_from_graph_item(task)
        input_label = AiidaWorkGraph.get_aiida_label_from_graph_item(input_)
        workgraph_task = self._aiida_task_nodes[task_label]

        if isinstance(task, ShellTask):
            input_namespace = f"nodes.{input_label}"
            workgraph_task.add_input("workgraph.any", f"nodes.{input_label}")
        elif isinstance(task, IconTask):
            input_namespace = f"{port}"
        else:
           raise ValueError(f"Provided task {task} is not supported. This should not happen. Please contact a developer.")


        # resolve data
        if (data_node := self._aiida_data_nodes.get(input_label)) is not None:
            if not AiidaWorkGraph.iterative_hasattr(workgraph_task.inputs, input_namespace):
                msg = f"Socket {input_label!r} was not found in workgraph. Please contact a developer."
                raise ValueError(msg)
            socket = AiidaWorkGraph.iterative_getattr(workgraph_task.inputs, input_namespace)
            socket.value = data_node
        elif (output_socket := self._aiida_socket_nodes.get(input_label)) is not None:
            self._workgraph.add_link(output_socket, workgraph_task.inputs[input_namespace])
        else:
            msg = (
                f"Input data node {input_label!r} was neither found in socket nodes nor in data nodes. The task "
                f"{task_label!r} must have dependencies on inputs before they are created."
            )
            raise ValueError(msg)

    @staticmethod
    def iterative_hasattr(obj: Any, attrs: str) -> bool:
        """
        Utility to use hasattr on AiiDA namespace strings as "namespace.input".

        For example:
        iterative_hasattr(obj, "integer.sum") --> checks if exists obj.integer.sum
        """
        for attr in attrs.split("."):
            if hasattr(obj, attr):
                obj = getattr(obj, attr)
            else:
                return False
        return True
    @staticmethod
    def iterative_getattr(obj: Any, attrs: str) -> Any:
        """
        Utility to use getattr on AiiDA namespace strings as "namespace.input".

        For example:
        iterative_getattr(obj, "integer.sum") --> return obj.integer.sum
        """
        for attr in attrs.split("."):
            if hasattr(obj, attr):
                obj = getattr(obj, attr)
        return obj


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

        name_to_input_map = {input_.name: input_ for input_, _ in task.inputs}
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

    def _link_output_nodes_to_shell_task(self, task: ShellTask, output: graph_items.Data):
        """Links the output to the workgraph shell task."""

        workgraph_task = self._aiida_task_nodes[AiidaWorkGraph.get_aiida_label_from_graph_item(task)]
        output_label = AiidaWorkGraph.get_aiida_label_from_graph_item(output)
        output_socket = workgraph_task.add_output("workgraph.any", output.src)
        self._aiida_socket_nodes[output_label] = output_socket

    def _link_restart_file_to_icon_task(self, task: IconTask):
        if (output := task.restart_file) is not None:
            output_label = AiidaWorkGraph.get_aiida_label_from_graph_item(output)
            workgraph_task = self._aiida_task_nodes[AiidaWorkGraph.get_aiida_label_from_graph_item(task)]
            self._aiida_socket_nodes[output_label] = workgraph_task.outputs.latest_restart_file



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
