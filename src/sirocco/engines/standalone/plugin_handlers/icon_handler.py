"""Standalone engine support for `core.IconTask`

What a port means for ICON (valid model types, namelist parameter, link name) is
declared in core by `IconPort`. Applying it is engine specific: link the data in the
run directory, point the namelist to it, and resolve the paths of output data.

Regular ports are applied generically from their declaration. Special ports (see
`IconPort.is_special`) are implemented by `IconHandler` methods registered with
`@special_port(<core port>)`.
"""

from __future__ import annotations

import shlex
import shutil
from collections.abc import Callable
from functools import cached_property, partial
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from sirocco.core.graph_items import GeneratedData
from sirocco.core.tasks.icon_task import IconTask, launch
from sirocco.core.tasks.icon_task import ports as core_icon_ports
from sirocco.core.tasks.icon_task.ports import IconPort
from sirocco.engines.standalone.plugin_handlers.plugin_handler import PluginHandler

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sirocco.core.graph_items import Data
    from sirocco.core.tasks.icon_task.models import IconModel


type SPECIAL_PORT_METHOD_T = Callable[[IconHandler, IconPort, IconModel], None]

# `IconHandler` methods implementing the special ports, by port name
special_port_methods: dict[str, SPECIAL_PORT_METHOD_T] = {}


def special_port(port: IconPort) -> Callable[[SPECIAL_PORT_METHOD_T], SPECIAL_PORT_METHOD_T]:
    """Register the decorated `IconHandler` method as the implementation of the special `port`"""

    def register(method: SPECIAL_PORT_METHOD_T) -> SPECIAL_PORT_METHOD_T:
        if not port.is_special:
            msg = f"port {port.port_name} is not special, it is applied from its declaration"
            raise ValueError(msg)
        if port.port_name in special_port_methods:
            msg = f"special port {port.port_name} already implemented"
            raise ValueError(msg)
        special_port_methods[port.port_name] = method
        return method

    return register


class IconHandler(PluginHandler):
    task_type = IconTask
    task: IconTask

    # The uenv is not set by the scheduler but passed to the runtime scripts (ICON_UENV),
    # together with the mount points of the squashed images
    scheduler_uenv: ClassVar[bool] = False

    def resolve_output_paths(self) -> None:
        for model in self.task.models.values():
            for port in model.outputs:
                self.apply_port(port, model)

    def prepare_rundir_content(self) -> None:
        rundir = self.get_rundir()

        launch.check_machine(self.task)

        # Mount points of the squashed images
        for _, mount_point in self.task.get_squash_mounts(rundir):
            mount_point.mkdir(parents=True, exist_ok=True)

        # Link ICON binaries
        if self.task.exe.cpu is not None:
            (rundir / launch.ICON_CPU_LINK).symlink_to(self.task.exe.cpu.path)
        if self.task.exe.gpu is not None:
            (rundir / launch.ICON_GPU_LINK).symlink_to(self.task.exe.gpu.path)

        # Take input/output ports specifications into account:
        # - adapt namelist parameters
        # - resolve data path
        # - link data
        for model in self.task.models.values():
            for port in chain(model.inputs, model.outputs):
                self.apply_port(port, model)

        # Dump ICON config files
        self.task.dump_namelists(directory=rundir, filename_mode="raw")
        if self.task.is_coupled() and self.task.yac_coupling is not None:
            shutil.copy(
                self.task.config_rootdir / self.task.yac_coupling,
                rundir / self.task.yac_coupling.name,
            )

        # Runtime scripts and the files they expect
        shutil.copytree(launch.runtime_dir(self.task), rundir, dirs_exist_ok=True)
        (rundir / launch.HOSTFILE).write_text(launch.hostfile_content(self.task))
        if self.task.target == "hybrid":
            (rundir / launch.MULTI_PROG_CONFIG).write_text(launch.multi_prog_content(self.task))

    def get_runscript_lines(self) -> list[str]:
        squash_mounts = [
            (self.get_data_path(image), mount) for image, mount in self.task.get_squash_mounts(self.get_rundir())
        ]
        env = launch.runtime_environment(self.task, squash_mounts)
        return [
            *(f"export {name}={shlex.quote(value)}" for name, value in env.items()),
            f"source ./{launch.RUNTIME_MAIN}",
        ]

    # =========== Ports ===========

    @cached_property
    def special_ports(self) -> dict[str, Callable[[IconPort, IconModel], None]]:
        """Implementations of the special ports bound to this handler, by port name"""
        return {name: partial(method, self) for name, method in special_port_methods.items()}

    def apply_port(self, port_name: str, model: IconModel) -> None:
        port = IconPort.get(port_name, model)
        if port.is_special:
            self.special_ports[port_name](port, model)
        else:
            self.apply_link_and_namelist(port, model)

    def apply_link_and_namelist(self, port: IconPort, model: IconModel) -> None:
        if len(model.inputs[port.port_name]) != 1:
            msg = f"port {port.port_name} accepts one and only one data object"
            raise ValueError(msg)
        data = model.inputs[port.port_name][0]
        rundir = self.get_rundir()
        # Adapt namelist and link
        if port.section is not None and port.parameter is not None:
            if isinstance(data, GeneratedData):
                target_link_name = port.target_link_name if port.target_link_name else self.get_data_path(data).name
                if port.section not in model.namelist:
                    model.namelist[port.section] = {}
                model.namelist[port.section][port.parameter] = f"./{target_link_name}"
                if not (origin := rundir / target_link_name).is_symlink():
                    origin.symlink_to(self.get_data_path(data))
            else:
                model.namelist[port.section][port.parameter] = str(self.get_data_path(data))
        # Only link
        elif port.target_link_name is not None:
            (rundir / port.target_link_name).symlink_to(self.get_data_path(data))
        else:
            msg = "wrong combination of section, parameter and target_link_name"
            raise ValueError(msg)

    # =========== Special ports ===========

    @staticmethod
    def restart_data(port: IconPort, model: IconModel, data_list: Sequence[Data]) -> Data | None:
        """The single data node of a restart port, if any, after checking the restart mode of `model`"""
        if (restart_write_mode := model.namelist["io_nml"].get("restart_write_mode")) != "joint procs multifile":
            msg = f"Only supported restart_write_mode is 'joint procs multifile', got {restart_write_mode!r}"
            raise ValueError(msg)
        # Ignore empty restart port or possible when conditions
        if not data_list:
            return None
        if len(data_list) > 1:
            msg = f"port '{port.port_name}' only accepts a single data object"
            raise ValueError(msg)
        return data_list[0]

    @special_port(core_icon_ports.restart_in_port)
    def apply_restart_in_port(self, port: IconPort, model: IconModel) -> None:
        if (data := self.restart_data(port, model, model.inputs[port.port_name])) is not None:
            (self.get_rundir() / f"multifile_restart_{model.name}.mfr").symlink_to(self.get_data_path(data))

    @special_port(core_icon_ports.restart_out_port)
    def apply_restart_out_port(self, port: IconPort, model: IconModel) -> None:
        if (data := self.restart_data(port, model, model.outputs[port.port_name])) is not None:
            self.set_data_path(data, self.get_rundir() / f"multifile_restart_{model.name}.mfr")

    @special_port(core_icon_ports.output_streams_port)
    def apply_output_streams_port(self, port: IconPort, model: IconModel) -> None:
        nml_streams = [*model.namelist.iter_nml("output_nml")]
        if (n_nml := len(nml_streams)) != (n_yaml := len(model.outputs[port.port_name])):
            msg = f"task {model.task_label}, model {model.name}: number of output streams speficied in namelist ({n_nml}) differs from number of streams specified in the workflow config ({n_yaml})"
            raise ValueError(msg)
        for k, (nml_stream, output_data) in enumerate(zip(nml_streams, model.outputs[port.port_name], strict=False)):
            filename_format = nml_stream.get("filename_format", "<output_filename>_XXX_YYY")
            output_filename = nml_stream.get("output_filename", "")
            # for type checkers
            if not isinstance(filename_format, str) or not isinstance(output_filename, str):
                msg = f"task {model.task_label}, model {model.name}, output stream number {k}: 'filename_format' and 'output_filename' namelist parameters must be strings"
                raise TypeError(msg)
            stream_dir = Path(filename_format.replace("<output_filename>", output_filename)).parent
            if stream_dir == Path("."):
                msg = f"task {model.task_label}, model {model.name}: output stream number {k} specifies an output stream directly in the run directory. Please specify a subdirectory using the 'filename_format' and 'output_filename' parameters (see ICON documentation)"
                raise ValueError(msg)
            self.set_data_path(output_data, stream_dir if stream_dir.is_absolute() else self.get_rundir() / stream_dir)
            self.get_data_path(output_data).mkdir(parents=True, exist_ok=True)

    @special_port(core_icon_ports.finish_status_port)
    def apply_finish_status_port(self, port: IconPort, model: IconModel) -> None:
        if len(model.outputs[port.port_name]) != 1:
            msg = f"port {port.port_name} accepts one and only one data object"
            raise ValueError(msg)
        self.set_data_path(model.outputs[port.port_name][0], self.get_rundir() / f"finish_{model.name}.status")

    @special_port(core_icon_ports.ocean_restart_init_port)
    def apply_ocean_restart_init_port(self, port: IconPort, model: IconModel) -> None:
        """Initialize the ocean state from a restart file without making the whole task a restart run"""
        if not (data_list := model.inputs[port.port_name]):
            return
        if len(data_list) > 1:
            msg = f"port '{port.port_name}' only accepts a single data object"
            raise ValueError(msg)
        if not model.namelist.get("ocean_initialConditions_nml", {}).get("initialize_fromRestart", False):
            msg = f"task {model.task_label}, model {model.name}: port '{port.port_name}' requires 'initialize_fromRestart = .true.' in 'ocean_initialConditions_nml'"
            raise ValueError(msg)
        (self.get_rundir() / f"multifile_restart_{model.name}.mfr").symlink_to(self.get_data_path(data_list[0]))
