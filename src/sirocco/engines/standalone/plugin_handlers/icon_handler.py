"""Standalone engine support for `core.IconTask`

What a port means for ICON (valid model types, namelist parameter, link name) is
declared in core by `IconPort`. Applying it is engine specific and done by
`IconPortHandler`, for one port, given a model and a run directory:

- input ports: link the input data in the run directory and point the namelist to it,
  from the input data paths resolved beforehand by `IconHandler`,
- output ports: decide the paths of the output data, set by `IconHandler`, and possibly
  create directories ICON expects to exist.

`IconPortHandler` mirrors `IconPort`: it applies regular ports generically from their
declaration, and each special port (see `IconPort.is_special`) is implemented by a
subclass registered with `@special_port_handler(<core port>)`. One handler per port is
created at import time and stored in `port_handlers`.
"""

from __future__ import annotations

import shlex
import shutil
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from sirocco.core.graph_items import GeneratedData
from sirocco.core.tasks.icon_task import IconTask, launch
from sirocco.core.tasks.icon_task import ports as core_icon_ports
from sirocco.core.tasks.icon_task.ports import IconPort
from sirocco.engines.standalone.plugin_handlers.plugin_handler import PluginHandler

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sirocco.core.graph_items import Data
    from sirocco.core.tasks.icon_task.models import IconModel


class IconHandler(PluginHandler):
    task_type = IconTask
    task: IconTask

    # The uenv is not set by the scheduler but passed to the runtime scripts (ICON_UENV),
    # together with the mount points of the squashed images
    scheduler_uenv: ClassVar[bool] = False

    def resolve_output_paths(self) -> None:
        for model in self.task.models.values():
            for port_name in model.outputs:
                self.apply_port(port_name, model)

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
            for port_name in chain(model.inputs, model.outputs):
                self.apply_port(port_name, model)

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

    def apply_port(self, port_name: str, model: IconModel) -> None:
        """Apply port `port_name` of `model` and set the paths of the output data it resolves"""
        # NOTE: keep check as explicit step here before refactoring ports and checking at parsing time
        IconPort.check(port_name, model)
        port_handler = port_handlers[port_name]
        input_paths = {data.label: self.get_data_path(data) for data in model.inputs.get(port_name, [])}
        for data, path in port_handler(model, self.get_rundir(), input_paths):
            self.set_data_path(data, path)

    def get_runscript_lines(self) -> list[str]:
        squash_mounts = [
            (self.get_data_path(image), mount) for image, mount in self.task.get_squash_mounts(self.get_rundir())
        ]
        env = launch.runtime_environment(self.task, squash_mounts)
        return [
            *(f"export {name}={shlex.quote(value)}" for name, value in env.items()),
            f"source ./{launch.RUNTIME_MAIN}",
        ]


type OUTPUT_PATHS_T = list[tuple[GeneratedData, Path]]


@dataclass(frozen=True)
class IconPortHandler:
    """Applies one ICON port

    The base class applies regular ports from their declaration. Special ports are
    implemented by subclasses registered with `@special_port_handler(<core port>)`.
    """

    port: IconPort

    def __call__(self, model: IconModel, rundir: Path, input_paths: Mapping[str, Path]) -> OUTPUT_PATHS_T:
        """Apply the port of `model` in `rundir` and return the paths of the output data it resolves

        `input_paths`: resolved paths of the port input data, by data label

        Regular ports link their input data in the run directory and point the namelist to it.
        """
        if len(data_list := model.inputs[self.port.port_name]) != 1:
            msg = f"port {self.port.port_name} accepts one and only one data object"
            raise ValueError(msg)
        data = data_list[0]
        path = input_paths[data.label]
        section, parameter, target_link_name = self.port.section, self.port.parameter, self.port.target_link_name
        # Adapt namelist and link
        if section is not None and parameter is not None:
            if isinstance(data, GeneratedData):
                link_name = target_link_name if target_link_name else path.name
                if section not in model.namelist:
                    model.namelist[section] = {}
                model.namelist[section][parameter] = f"./{link_name}"
                # NOTE: the piece of data could be linked by different ports, hence the check.
                if not (link := rundir / link_name).is_symlink():
                    link.symlink_to(path)
            else:
                model.namelist[section][parameter] = str(path)
        # Only link
        elif target_link_name is not None:
            (rundir / target_link_name).symlink_to(path)
        else:
            msg = "wrong combination of section, parameter and target_link_name"
            raise ValueError(msg)
        return []


# Handlers of all the ICON ports, by port name: special ports are registered below by
# `@special_port_handler`, regular ones are then added from their declaration.
port_handlers: dict[str, IconPortHandler] = {}


def special_port_handler[H: type[IconPortHandler]](port: IconPort) -> Callable[[H], H]:
    """Register the decorated `IconPortHandler` subclass as the implementation of the special `port`"""

    def register(cls: H) -> H:
        if not port.is_special:
            msg = f"port {port.port_name} is not special, it is applied from its declaration"
            raise ValueError(msg)
        if port.port_name in port_handlers:
            msg = f"special port {port.port_name} already implemented"
            raise ValueError(msg)
        port_handlers[port.port_name] = cls(port)
        return cls

    return register


# =========== Special ports ===========


class RestartPortHandler(IconPortHandler):
    def get_restart_data[D: Data](self, model: IconModel, data_list: Sequence[D]) -> D | None:
        """The single data node of the port, if any, after checking the restart mode of `model`"""
        restart_write_mode = model.namelist["io_nml"].get("restart_write_mode")
        if restart_write_mode != "joint procs multifile":
            msg = f"Only supported restart_write_mode is 'joint procs multifile', got {restart_write_mode!r}"
            raise ValueError(msg)
        # Ignore empty restart port or possible when conditions
        if not data_list:
            return None
        if len(data_list) > 1:
            msg = f"port '{self.port.port_name}' only accepts a single data object"
            raise ValueError(msg)
        return data_list[0]

    @staticmethod
    def restart_path(model: IconModel, rundir: Path) -> Path:
        return rundir / f"multifile_restart_{model.name}.mfr"


@special_port_handler(core_icon_ports.restart_in_port)
class RestartInPortHandler(RestartPortHandler):
    def __call__(self, model: IconModel, rundir: Path, input_paths: Mapping[str, Path]) -> OUTPUT_PATHS_T:
        if (data := self.get_restart_data(model, model.inputs[self.port.port_name])) is not None:
            self.restart_path(model, rundir).symlink_to(input_paths[data.label])
        return []


@special_port_handler(core_icon_ports.restart_out_port)
class RestartOutPortHandler(RestartPortHandler):
    def __call__(self, model: IconModel, rundir: Path, input_paths: Mapping[str, Path]) -> OUTPUT_PATHS_T:  # noqa: ARG002
        if (data := self.get_restart_data(model, model.outputs[self.port.port_name])) is None:
            return []
        return [(data, self.restart_path(model, rundir))]


@special_port_handler(core_icon_ports.output_streams_port)
class OutputStreamsPortHandler(IconPortHandler):
    def __call__(self, model: IconModel, rundir: Path, input_paths: Mapping[str, Path]) -> OUTPUT_PATHS_T:  # noqa: ARG002
        output_data = model.outputs[self.port.port_name]
        nml_streams = [*model.namelist.iter_nml("output_nml")]
        if (n_nml := len(nml_streams)) != (n_yaml := len(output_data)):
            msg = f"task {model.task_label}, model {model.name}: number of output streams speficied in namelist ({n_nml}) differs from number of streams specified in the workflow config ({n_yaml})"
            raise ValueError(msg)
        paths: OUTPUT_PATHS_T = []
        for k, (nml_stream, data) in enumerate(zip(nml_streams, output_data, strict=False)):
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
            path = stream_dir if stream_dir.is_absolute() else rundir / stream_dir
            path.mkdir(parents=True, exist_ok=True)
            paths.append((data, path))
        return paths


@special_port_handler(core_icon_ports.finish_status_port)
class FinishStatusPortHandler(IconPortHandler):
    def __call__(self, model: IconModel, rundir: Path, input_paths: Mapping[str, Path]) -> OUTPUT_PATHS_T:  # noqa: ARG002
        if len(output_data := model.outputs[self.port.port_name]) != 1:
            msg = f"port {self.port.port_name} accepts one and only one data object"
            raise ValueError(msg)
        return [(output_data[0], rundir / f"finish_{model.name}.status")]


# =========== Regular ports ===========


def _complete_port_handlers() -> None:
    """Add the regular port handlers and check every special port is implemented

    All ICON ports are declared in core by the time this module is imported.
    """
    for port in IconPort.registered_ports.values():
        if not port.is_special:
            port_handlers[port.port_name] = IconPortHandler(port)
        elif port.port_name not in port_handlers:
            msg = f"special port {port.port_name} not implemented in the standalone engine"
            raise NotImplementedError(msg)


_complete_port_handlers()
