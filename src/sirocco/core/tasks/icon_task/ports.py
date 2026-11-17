"""What ICON input and output ports mean for ICON.

Each `IconPort` declares the model types a port is valid for and, for regular ports,
which namelist parameter refers to the port data and under which name it is linked.
Special ports declare none of these: their behaviour cannot be described this way, so
every engine has to implement it. Applying a port to an actual run directory is engine
specific (see for instance `sirocco.engines.standalone.tasks.icon`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Self

from sirocco.core.tasks.icon_task.models import ModelType

if TYPE_CHECKING:
    from sirocco.core.tasks.icon_task.models import IconModel


@dataclass(kw_only=True, frozen=True)
class IconPort:
    registered_ports: ClassVar[dict[str, Self]] = {}
    port_name: str
    valid_model_types: list[ModelType]
    section: str | None = None
    parameter: str | None = None
    target_link_name: str | None = None

    def __post_init__(self) -> None:
        if self.port_name in self.registered_ports:
            msg = f"IconPort {self.port_name} already set"
            raise ValueError(msg)
        IconPort.registered_ports[self.port_name] = self

    @property
    def is_special(self) -> bool:
        """Special ports have no declarative handling, each engine implements them"""
        return self.section is None and self.parameter is None and self.target_link_name is None

    @classmethod
    def get(cls: type[Self], port_name: str, model: IconModel) -> Self:
        """Port `port_name`, checked to be supported and valid for `model`"""
        if (port := cls.registered_ports.get(port_name)) is None:
            msg = f"IconTask {model.task_label}, model {model.name}: unsopported input port {port_name}"
            raise KeyError(msg)
        if model.model_type not in port.valid_model_types:
            msg = f"port {port_name} not valid for model {model.name} of type {model.model_type}"
            raise ValueError(msg)
        return port


# TODO: Check port names with regards to:
#       - overall consistency, e.g. across components
#       - corresponding namelist parameters
#       - users/scientists

dynamics_grid_file_port = IconPort(
    port_name="dynamics_grid_file",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
    section="grid_nml",
    parameter="dynamics_grid_filename",
)
ifs2icon_port = IconPort(
    port_name="ifs2icon",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="initicon_nml",
    parameter="ifs2icon_filename",
)
latbc_port = IconPort(
    port_name="latbc",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="limarea_nml",
    parameter="latbc_path",
    target_link_name="latbc",
)
ecrad_data_port = IconPort(
    port_name="ecrad_data",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="radiation_nml",
    parameter="ecrad_data_path",
    target_link_name="ecrad_data",
)
extpar_file_port = IconPort(
    port_name="extpar_file",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="extpar_nml",
    parameter="extpar_filename",
)
cloud_opt_props_port = IconPort(
    port_name="cloud_opt_props",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="nwp_phy_nml",
    parameter="cldopt_filename",
    target_link_name="CldOptProps.nc",
)
rrtmg_lw_port = IconPort(
    port_name="rrtmg_lw",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="nwp_phy_nml",
    parameter="lrtm_filename",
    target_link_name="rrtmg_lw.nc",
)
rrtmg_sw_port = IconPort(
    port_name="rrtmg_sw",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="rrtmg_sw.nc",
)
bc_solar_sw_port = IconPort(
    port_name="bc_solar_sw",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_solar_irradiance_sw_b14.nc",
)
atm_plumes_port = IconPort(
    port_name="atm_plumes",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="MACv2.0-SP_v1.nc",
)
ghg_filename_port = IconPort(
    port_name="ghg_file",
    valid_model_types=[ModelType.ATMOSPHERE],
    section="radiation_nml",
    parameter="ghg_filename",
)
jsb_ifs_port = IconPort(
    port_name="jsb_ifs",
    valid_model_types=[ModelType.LAND],
    section="jsb_model_nml",
    parameter="ifs_filename",
)
jsb_seb_bc_port = IconPort(
    port_name="jsb_seb_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_seb_nml",
    parameter="bc_filename",
)
jsb_seb_ic_port = IconPort(
    port_name="jsb_seb_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_seb_nml",
    parameter="ic_filename",
)
jsb_rad_bc_port = IconPort(
    port_name="jsb_rad_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_rad_nml",
    parameter="bc_filename",
)
jsb_rad_ic_port = IconPort(
    port_name="jsb_rad_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_rad_nml",
    parameter="ic_filename",
)
jsb_turb_bc_port = IconPort(
    port_name="jsb_turb_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_turb_nml",
    parameter="bc_filename",
)
jsb_turb_ic_port = IconPort(
    port_name="jsb_turb_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_turb_nml",
    parameter="ic_filename",
)
jsb_sse_bc_port = IconPort(
    port_name="jsb_sse_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_sse_nml",
    parameter="bc_filename",
)
jsb_sse_ic_port = IconPort(
    port_name="jsb_sse_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_sse_nml",
    parameter="ic_filename",
)
jsb_hydro_bc_port = IconPort(
    port_name="jsb_hydro_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_hydro_nml",
    parameter="bc_filename",
)
jsb_hydro_bc_sso_port = IconPort(
    port_name="jsb_hydro_bc_sso",
    valid_model_types=[ModelType.LAND],
    section="jsb_hydro_nml",
    parameter="bc_sso_filename",
)
jsb_hydro_ic_port = IconPort(
    port_name="jsb_hydro_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_hydro_nml",
    parameter="ic_filename",
)
jsb_pheno_bc_port = IconPort(
    port_name="jsb_pheno_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_pheno_nml",
    parameter="bc_filename",
)
jsb_pheno_ic_port = IconPort(
    port_name="jsb_pheno_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_pheno_nml",
    parameter="ic_filename",
)
jsb_disturb_bc_port = IconPort(
    port_name="jsb_disturb_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_disturb_nml",
    parameter="bc_filename",
)
jsb_disturb_ic_port = IconPort(
    port_name="jsb_disturb_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_disturb_nml",
    parameter="ic_filename",
)
jsb_hd_bc_port = IconPort(
    port_name="jsb_hd_bc",
    valid_model_types=[ModelType.LAND],
    section="jsb_hd_nml",
    parameter="bc_filename",
)
jsb_hd_ic_port = IconPort(
    port_name="jsb_hd_ic",
    valid_model_types=[ModelType.LAND],
    section="jsb_hd_nml",
    parameter="ic_filename",
)
jsb_fract_port = IconPort(
    port_name="jsb_fract",
    valid_model_types=[ModelType.LAND],
    section="jsb_model_nml",
    parameter="fract_filename",
)
ocean_inistate_port = IconPort(
    port_name="ocean_inistate",
    valid_model_types=[ModelType.OCEAN],
    section="ocean_initialConditions_nml",
    parameter="InitialState_InputFileName",
)

# Special ports, implemented by each engine
restart_in_port = IconPort(
    port_name="restart_in",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
)
restart_out_port = IconPort(
    port_name="restart_out",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
)
output_streams_port = IconPort(
    port_name="output_streams",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
)
finish_status_port = IconPort(
    port_name="finish_status",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
)
