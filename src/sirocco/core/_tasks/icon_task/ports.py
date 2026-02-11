import enum
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Self

from sirocco.core.graph_items import Data, GeneratedData, TaskComponent
from sirocco.core.namelistfile import NamelistFile


class ModelType(enum.Enum):
    _value_: int
    ATMOSPHERE = 1
    OCEAN = 2


@dataclass(kw_only=True)
class IconModel:
    core_component: TaskComponent
    name: str
    task_run_dir: Path
    task_label: str
    namelist: NamelistFile
    model_type: ModelType

    @property
    def inputs(self) -> dict[str, list[Data]]:
        return self.core_component.inputs

    @property
    def outputs(self) -> dict[str, list[GeneratedData]]:
        return self.core_component.outputs


@dataclass(kw_only=True)
class PortHandler:
    registered_handlers: ClassVar[dict[str, Self]] = {}
    port_name: str
    valid_model_types: list[ModelType]
    section: str | None = None
    parameter: str | None = None
    target_link_name: str | None = None
    custom_callable: Callable[[str, IconModel], None] | None = None

    def __post_init__(self) -> None:
        if self.port_name in self.registered_handlers:
            msg = f"PortHandler for port {self.port_name} already set"
            raise ValueError(msg)
        PortHandler.registered_handlers[self.port_name] = self

    def __call__(self, model: IconModel) -> None:
        if model.model_type not in self.valid_model_types:
            msg = f"port {self.port_name} not valid for model type {model.model_type}"
            raise ValueError(msg)
        if self.custom_callable:
            self.custom_callable(self.port_name, model)
        else:
            if self.section is None or self.parameter is None:
                msg = "section and parameter are required if a custom handler is not provided"
                raise ValueError(msg)
            if len(model.inputs[self.port_name]) != 1:
                msg = f"port {self.port_name} accepts one and only one data object"
                raise ValueError(msg)
            data = model.inputs[self.port_name][0]
            if isinstance(data, GeneratedData):
                target_link_name = self.target_link_name if self.target_link_name else data.resolved_path.name
                model.namelist[self.section][self.parameter] = f"./{target_link_name}"
                (model.task_run_dir / target_link_name).symlink_to(data.resolved_path)
            else:
                model.namelist[self.section][self.parameter] = str(data.resolved_path)

    @classmethod
    def handle(cls: type[Self], port_name: str, model: IconModel) -> None:
        if port_name not in cls.registered_handlers:
            msg = f"IconTask {model.task_label}, model {model.name}: unsopported input port {port_name}"
            raise KeyError(msg)
        cls.registered_handlers[port_name](model)


dynamics_grid_file_handler = PortHandler(
    port_name="dynamics_grid_file",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="initicon_nml",
    parameter="ifs2icon_filename",
)
ifs2icon_handler = PortHandler(
    port_name="ifs2icon",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="initicon_nml",
    parameter="ifs2icon_filename",
)
ecrad_data_handler = PortHandler(
    port_name="ecrad_data",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="radiation_nml",
    parameter="ecrad_data_path",
    target_link_name="ecrad_data",
)
extpar_file_handler = PortHandler(
    port_name="extpar_file",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="extpar_nml",
    parameter="extpar_filename",
)
cloud_opt_props_handler = PortHandler(
    port_name="cloud_opt_props",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="nwp_phy_nml",
    parameter="cldopt_filename",
    target_link_name="CldOptProps.nc",
)
rrtmg_lw_handler = PortHandler(
    port_name="rrtmg_lw",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="nwp_phy_nml",
    parameter="lrtm_filename",
    target_link_name="rrtmg_lw.nc",
)


def restart_in_handler_callable(port_name: str, model: IconModel) -> None:
    if restart_write_mode := model.namelist["io_nml"].get("restart_write_mode") != "joint procs multifile":
        msg = f"Only supported restart_write_mode is 'joint procs multifile', got {restart_write_mode}"
        raise ValueError(msg)
    # Ignore empty restart port or possible when conditions
    if not (data_list := model.inputs[port_name]):
        return
    if len(data_list) > 1:
        msg = f"port '{port_name}' only accepts a single data object"
        raise ValueError(msg)
    (model.task_run_dir / f"multifile_restart_{model.name}.mfr").symlink_to(data_list[0].resolved_path)


restart_in_handler = PortHandler(
    port_name="restart_file",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
    custom_callable=restart_in_handler_callable,
)


def restart_out_handler_callable(port_name: str, model: IconModel) -> None:
    if restart_write_mode := model.namelist["io_nml"].get("restart_write_mode") != "joint procs multifile":
        msg = f"Only supported restart_write_mode is 'joint procs multifile', got {restart_write_mode}"
        raise ValueError(msg)
    # Ignore empty restart port or possible when conditions
    if not (data_list := model.inputs[port_name]):
        return
    if len(data_list) > 1:
        msg = f"port '{port_name}' only accepts a single data object"
        raise ValueError(msg)
    data_list[0].resolved_path = model.task_run_dir / f"multifile_restart_{model.name}.mfr"


restart_out_handler = PortHandler(
    port_name="latest_restart_file",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
    custom_callable=restart_out_handler_callable,
)


def output_streams_handler_callable(port_name: str, model: IconModel) -> None:  # noqa: ARG001
    nml_streams = [*model.namelist.iter_nml("output_nml")]
    if (n_nml := len(nml_streams)) != (n_yaml := len(model.outputs["output_nml"])):
        msg = f"task {model.task_label}, model {model.name}: number of output streams speficied in namelist ({n_nml}) differs from number of streams specified in the workflow config ({n_yaml})"
        raise ValueError(msg)
    for k, (nml_stream, output_data) in enumerate(zip(nml_streams, model.outputs["output_nml"], strict=False)):
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
        output_data.resolved_path = stream_dir if stream_dir.is_absolute() else model.task_run_dir / stream_dir
        output_data.resolved_path.mkdir(parents=True, exist_ok=True)


output_streams_handler = PortHandler(
    port_name="output_streams",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
    custom_callable=output_streams_handler_callable,
)


def finish_status_handler_callable(port_name: str, model: IconModel) -> None:  # noqa: ARG001
    if len(model.outputs["finish_status"]) != 1:
        msg = "port finish_status accepts one and only one data object"
        raise ValueError(msg)
    model.outputs["finish_status"][0].resolved_path = model.task_run_dir / f"finish_{model.name}.status"

finish_status_handler = PortHandler(
    port_name="finish_status",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
    custom_callable=finish_status_handler_callable,
)
