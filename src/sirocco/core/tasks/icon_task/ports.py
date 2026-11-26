"""What ICON input and output ports mean for ICON.

Each `IconPort` declares the model types a port is valid for and, for regular ports,
which namelist parameter refers to the port data and under which name it is linked.
Special ports declare none of these: their behaviour cannot be described this way, so
every engine has to implement it. Applying a port to an actual run directory is engine
specific (see for instance `sirocco.engines.standalone.plugin_handlers.icon_handler`).
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
    def check(cls, port_name: str, model: IconModel) -> None:
        """Check that port `port_name` is supported and valid for `model`"""
        if (port := cls.registered_ports.get(port_name)) is None:
            msg = f"IconTask {model.task_label}, model {model.name}: unsupported input port {port_name}"
            raise KeyError(msg)
        if model.model_type not in port.valid_model_types:
            msg = f"port {port_name} not valid for model {model.name} of type {model.model_type}"
            raise ValueError(msg)


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
# AES physics reads the following files under fixed names
aes_ghg_file_port = IconPort(
    port_name="aes_ghg_file",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_greenhouse_gases.nc",
)
aes_land_sso_port = IconPort(
    port_name="aes_land_sso",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_land_sso.nc",
)
aes_land_frac_port = IconPort(
    port_name="aes_land_frac",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_land_frac.nc",
)
aes_land_phys_port = IconPort(
    port_name="aes_land_phys",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_land_phys.nc",
)
aes_bc_sst_port = IconPort(
    port_name="aes_bc_sst",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_sst.nc",
)
aes_bc_sic_port = IconPort(
    port_name="aes_bc_sic",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_sic.nc",
)
cariolle_coeff_port = IconPort(
    port_name="cariolle_coeff",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="cariolle_coeff.nc",
)
rrtmgp_gas_lw_port = IconPort(
    port_name="rrtmgp_gas_lw",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="coefficients_lw.nc",
)
rrtmgp_gas_sw_port = IconPort(
    port_name="rrtmgp_gas_sw",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="coefficients_sw.nc",
)
rrtmgp_cloud_optics_lw_port = IconPort(
    port_name="rrtmgp_cloud_optics_lw",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="rrtmgp-cloud-optics-coeffs-lw.nc",
)
rrtmgp_cloud_optics_sw_port = IconPort(
    port_name="rrtmgp_cloud_optics_sw",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="rrtmgp-cloud-optics-coeffs-sw.nc",
)
aeropt_kinne_lw_coa_port = IconPort(
    port_name="aeropt_kinne_lw_coa",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_aeropt_kinne_lw_b16_coa.nc",
)
aeropt_kinne_sw_coa_port = IconPort(
    port_name="aeropt_kinne_sw_coa",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_aeropt_kinne_sw_b14_coa.nc",
)
aeropt_kinne_sw_fin_port = IconPort(
    port_name="aeropt_kinne_sw_fin",
    valid_model_types=[ModelType.ATMOSPHERE],
    target_link_name="bc_aeropt_kinne_sw_b14_fin.nc",
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
jsb_lctlib_port = IconPort(
    port_name="jsb_lctlib",
    valid_model_types=[ModelType.LAND],
    target_link_name="lctlib_nlct21.def",
)
ocean_inistate_port = IconPort(
    port_name="ocean_inistate",
    valid_model_types=[ModelType.OCEAN],
    section="ocean_initialConditions_nml",
    parameter="InitialState_InputFileName",
)
# HAMOCC reads the following files under fixed names
hamocc_dust_port = IconPort(
    port_name="hamocc_dust",
    valid_model_types=[ModelType.OCEAN],
    target_link_name="dust.nc",
)
hamocc_nitrogen_port = IconPort(
    port_name="hamocc_nitrogen",
    valid_model_types=[ModelType.OCEAN],
    target_link_name="nitrogen.nc",
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
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN, ModelType.LAND],
)
finish_status_port = IconPort(
    port_name="finish_status",
    valid_model_types=[ModelType.ATMOSPHERE, ModelType.OCEAN],
)
ocean_restart_init_port = IconPort(
    port_name="ocean_restart_init",
    valid_model_types=[ModelType.OCEAN],
)
