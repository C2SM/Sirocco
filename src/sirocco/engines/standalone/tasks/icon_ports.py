"""Apply ICON ports to a task run directory, for the standalone engine.

What a port means for ICON (valid model types, namelist parameter, link name) is
declared in core by `IconPort`. Applying it is engine specific: link the data in the
run directory, point the namelist to it, and resolve the paths of output data.

Regular ports are applied generically from their declaration. Special ports (see
`IconPort.is_special`) are implemented below by functions registered with
`@special_port(<core port>)`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sirocco.core.graph_items import GeneratedData
from sirocco.core.tasks.icon_task import ports as core_icon_ports
from sirocco.core.tasks.icon_task.ports import IconPort

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sirocco.core.graph_items import Data
    from sirocco.core.tasks.icon_task.models import IconModel
    from sirocco.engines.standalone.path_resolver import PathResolver


type SpecialPortFunction = Callable[[IconPort, IconModel, Path, PathResolver], None]

# Implementations of the special ports, by port name
special_ports: dict[str, SpecialPortFunction] = {}


def special_port(port: IconPort) -> Callable[[SpecialPortFunction], SpecialPortFunction]:
    """Register the decorated function as the implementation of the special `port`"""

    def register(function: SpecialPortFunction) -> SpecialPortFunction:
        if not port.is_special:
            msg = f"port {port.port_name} is not special, it is applied from its declaration"
            raise ValueError(msg)
        if port.port_name in special_ports:
            msg = f"special port {port.port_name} already implemented"
            raise ValueError(msg)
        special_ports[port.port_name] = function
        return function

    return register


def apply_port(port_name: str, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    port = IconPort.get(port_name, model)
    if port.is_special:
        if (apply_special_port := special_ports.get(port_name)) is None:
            msg = f"special port {port_name} not implemented in the standalone engine"
            raise NotImplementedError(msg)
        apply_special_port(port, model, run_dir, path_resolver)
    else:
        apply_link_and_namelist(port, model, run_dir, path_resolver)


def apply_link_and_namelist(port: IconPort, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    if len(model.inputs[port.port_name]) != 1:
        msg = f"port {port.port_name} accepts one and only one data object"
        raise ValueError(msg)
    data = model.inputs[port.port_name][0]
    # Adapt namelist and link
    if port.section is not None and port.parameter is not None:
        if isinstance(data, GeneratedData):
            target_link_name = port.target_link_name if port.target_link_name else path_resolver.get_path(data).name
            if port.section not in model.namelist:
                model.namelist[port.section] = {}
            model.namelist[port.section][port.parameter] = f"./{target_link_name}"
            if not (origin := run_dir / target_link_name).is_symlink():
                origin.symlink_to(path_resolver.get_path(data))
        else:
            model.namelist[port.section][port.parameter] = str(path_resolver.get_path(data))
    # Only link
    elif port.target_link_name is not None:
        (run_dir / port.target_link_name).symlink_to(path_resolver.get_path(data))
    else:
        msg = "wrong combination of section, parameter and target_link_name"
        raise ValueError(msg)


# =========== Special ports ===========


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
def apply_restart_in_port(port: IconPort, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    if (data := restart_data(port, model, model.inputs[port.port_name])) is not None:
        (run_dir / f"multifile_restart_{model.name}.mfr").symlink_to(path_resolver.get_path(data))


@special_port(core_icon_ports.restart_out_port)
def apply_restart_out_port(port: IconPort, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    if (data := restart_data(port, model, model.outputs[port.port_name])) is not None:
        path_resolver.set_path(data, run_dir / f"multifile_restart_{model.name}.mfr")


@special_port(core_icon_ports.output_streams_port)
def apply_output_streams_port(port: IconPort, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
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
        path_resolver.set_path(output_data, stream_dir if stream_dir.is_absolute() else run_dir / stream_dir)
        path_resolver.get_path(output_data).mkdir(parents=True, exist_ok=True)


@special_port(core_icon_ports.finish_status_port)
def apply_finish_status_port(port: IconPort, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    if len(model.outputs[port.port_name]) != 1:
        msg = f"port {port.port_name} accepts one and only one data object"
        raise ValueError(msg)
    path_resolver.set_path(model.outputs[port.port_name][0], run_dir / f"finish_{model.name}.status")
