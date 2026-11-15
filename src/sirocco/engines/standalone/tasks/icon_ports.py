"""Apply ICON ports to a task run directory, for the standalone engine.

What a port means for ICON (valid model types, namelist parameter, link name) is
declared in core by `IconPort`. Applying it is engine specific: link the data in the
run directory, point the namelist to it, and resolve the paths of output data.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sirocco.core.graph_items import GeneratedData
from sirocco.core.tasks.icon_task.ports import IconPort

if TYPE_CHECKING:
    from collections.abc import Callable

    from sirocco.core.tasks.icon_task.models import IconModel
    from sirocco.engines.standalone.path_resolver import PathResolver


def apply_port(port_name: str, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    port = IconPort.get(port_name, model)
    if (apply_special_port := SPECIAL_PORTS.get(port_name)) is not None:
        apply_special_port(port_name, model, run_dir, path_resolver)
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
            target_link_name = port.target_link_name if port.target_link_name else path_resolver.path(data).name
            if port.section not in model.namelist:
                model.namelist[port.section] = {}
            model.namelist[port.section][port.parameter] = f"./{target_link_name}"
            if not (origin := run_dir / target_link_name).is_symlink():
                origin.symlink_to(path_resolver.path(data))
        else:
            model.namelist[port.section][port.parameter] = str(path_resolver.path(data))
    # Only link
    elif port.target_link_name is not None:
        (run_dir / port.target_link_name).symlink_to(path_resolver.path(data))
    else:
        msg = "wrong combination of section, parameter and target_link_name"
        raise ValueError(msg)


# =========== Special ports ===========


def apply_restart_in_port(port_name: str, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    if restart_write_mode := model.namelist["io_nml"].get("restart_write_mode") != "joint procs multifile":
        msg = f"Only supported restart_write_mode is 'joint procs multifile', got {restart_write_mode}"
        raise ValueError(msg)
    # Ignore empty restart port or possible when conditions
    if not (data_list := model.inputs[port_name]):
        return
    if len(data_list) > 1:
        msg = f"port '{port_name}' only accepts a single data object"
        raise ValueError(msg)
    (run_dir / f"multifile_restart_{model.name}.mfr").symlink_to(path_resolver.path(data_list[0]))


def apply_restart_out_port(port_name: str, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    if restart_write_mode := model.namelist["io_nml"].get("restart_write_mode") != "joint procs multifile":
        msg = f"Only supported restart_write_mode is 'joint procs multifile', got {restart_write_mode}"
        raise ValueError(msg)
    # Ignore empty restart port or possible when conditions
    if not (data_list := model.outputs[port_name]):
        return
    if len(data_list) > 1:
        msg = f"port '{port_name}' only accepts a single data object"
        raise ValueError(msg)
    path_resolver.set_path(data_list[0], run_dir / f"multifile_restart_{model.name}.mfr")


def apply_output_streams_port(port_name: str, model: IconModel, run_dir: Path, path_resolver: PathResolver) -> None:
    nml_streams = [*model.namelist.iter_nml("output_nml")]
    if (n_nml := len(nml_streams)) != (n_yaml := len(model.outputs[port_name])):
        msg = f"task {model.task_label}, model {model.name}: number of output streams speficied in namelist ({n_nml}) differs from number of streams specified in the workflow config ({n_yaml})"
        raise ValueError(msg)
    for k, (nml_stream, output_data) in enumerate(zip(nml_streams, model.outputs[port_name], strict=False)):
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
        path_resolver.path(output_data).mkdir(parents=True, exist_ok=True)


def apply_finish_status_port(
    port_name: str,  # noqa: ARG001
    model: IconModel,
    run_dir: Path,
    path_resolver: PathResolver,
) -> None:
    if len(model.outputs["finish_status"]) != 1:
        msg = "port finish_status accepts one and only one data object"
        raise ValueError(msg)
    path_resolver.set_path(model.outputs["finish_status"][0], run_dir / f"finish_{model.name}.status")


SPECIAL_PORTS: dict[str, Callable[[str, IconModel, Path, PathResolver], None]] = {
    "restart_in": apply_restart_in_port,
    "restart_out": apply_restart_out_port,
    "output_streams": apply_output_streams_port,
    "finish_status": apply_finish_status_port,
}
