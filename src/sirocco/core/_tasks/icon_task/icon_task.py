from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

import f90nml

from sirocco.core.graph_items import Data, GeneratedData, Task
from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    from collections.abc import Sequence


LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class IconTask(models.ConfigIconTaskSpecs, Task):
    _MASTER_NAMELIST_NAME: ClassVar[str] = field(default="icon_master.namelist", repr=False)
    _MASTER_MODEL_NML_SECTION: ClassVar[str] = field(default="master_model_nml", repr=False)
    _MODEL_NAMELIST_FILENAME_FIELD: ClassVar[str] = field(default="model_namelist_filename", repr=False)
    _AIIDA_ICON_RESTART_FILE_PORT_NAME: ClassVar[str] = field(default="restart_file", repr=False)
    namelists: list[NamelistFile]

    def __post_init__(self):
        super().__post_init__()
        # detect master namelist
        master_namelist = None
        for namelist in self.namelists:
            if namelist.name == self._MASTER_NAMELIST_NAME:
                master_namelist = namelist
                break
        if master_namelist is None:
            msg = f"Failed to read master namelists. Could not find {self._MASTER_NAMELIST_NAME!r} in namelists {self.namelists}"
            raise ValueError(msg)
        self._master_namelist = master_namelist

        # retrieve model namelist name from master namelist
        if (master_model_nml := self._master_namelist.namelist.get(self._MASTER_MODEL_NML_SECTION, None)) is None:
            msg = "No model filename specified in master namelist: Could not find section '&master_model_nml'"
            raise ValueError(msg)
        # TODO: Check if master_model_nml is of instance cogroup => raise not implemented error
        if isinstance(master_model_nml, f90nml.namelist.Cogroup):
            msg = f"multiple {self._MASTER_MODEL_NML_SECTION} not implemented yet"
            raise NotImplementedError(msg)
        if (model_namelist_filename := master_model_nml.get(self._MODEL_NAMELIST_FILENAME_FIELD, None)) is None:
            msg = f"No model filename specified in master namelist: Could not find entry '{self._MODEL_NAMELIST_FILENAME_FIELD}' under section '&{self._MASTER_MODEL_NML_SECTION}'"
            raise ValueError(msg)

        # detect model namelist
        model_namelist = None
        for namelist in self.namelists:
            if namelist.name == model_namelist_filename:
                model_namelist = namelist
                break
        if model_namelist is None:
            msg = f"Failed to read model namelist. Could not find {model_namelist_filename!r} in namelists {self.namelists}"
            raise ValueError(msg)
        self._model_namelist = model_namelist

        if self.wrapper_script is not None:
            self.wrapper_script = self._validate_wrapper_script(self.wrapper_script, self.config_rootdir)

        # Set default MPI variables
        self.nodes = 1 if self.nodes is None else self.nodes
        if self.ntasks_per_node is None:
            if self.target == "santis_cpu":
                self.ntasks_per_node = 288
            elif self.target == "santis_gpu":
                self.ntasks_per_node = 4

    @property
    def master_namelist(self) -> NamelistFile:
        return self._master_namelist

    @property
    def model_namelist(self) -> NamelistFile:
        return self._model_namelist

    @property
    def is_restart(self) -> bool:
        """Check if the icon task starts from the restart file."""
        # restart port must be present and nonempty
        return bool(self.inputs.get(self._AIIDA_ICON_RESTART_FILE_PORT_NAME, False))

    def update_icon_namelists_from_workflow(self) -> None:
        if not isinstance(self.cycle_point, DateCyclePoint):
            msg = "ICON task must have a DateCyclePoint"
            raise TypeError(msg)
        self.master_namelist.update_from_specs(
            {
                "master_time_control_nml": {
                    "experimentStartDate": self.cycle_point.start_date.isoformat() + "Z",
                    "experimentStopDate": self.cycle_point.stop_date.isoformat() + "Z",
                    "restarttimeintval": str(self.cycle_point.period),
                },
                "master_nml": {
                    "lrestart": self.is_restart,
                    # TODO: check what this really means and if we need it set
                    #       up automatically from the workflow
                    "read_restart_namelists": self.is_restart,
                },
            }
        )

    def dump_namelists(
        self, directory: Path, filename_mode: Literal["append_coordinates", "raw"] = "append_coordinates"
    ) -> None:
        # TODO: if standalone becomes the only orchestrator, no need for directory and filename_mode kw args
        if not directory.exists():
            msg = f"Dumping path {directory} does not exist."
            raise OSError(msg)
        if not directory.is_dir():
            msg = f"Dumping path {directory} is not directory."
            raise OSError(msg)

        for namelist in self.namelists:
            filename = namelist.name
            if filename_mode == "append_coordinates":
                suffix = ("_".join([str(p) for p in self.coordinates.values()])).replace(" ", "_")
                filename += "_" + suffix
            namelist.dump(directory / filename)

    def prepare_for_submission(self) -> None:
        # Ensure either target or runscript is set
        # NOTE: This code is there as it is the first available place where we know the standalone orchestrator is used
        # TODO: if standalone becomes the only orchestrator, make this a yaml model validator
        if self.target is None:
            if self.runscript_content is None:
                msg = f"task {self.name}: 'runscript_content' is required when 'target' is unset"
                raise ValueError(msg)
        elif self.runscript_content is not None or self.auxilary_run_files is not None:
            msg = f"task {self.name}: 'target' set to {self.target}: 'runscript_content' and 'auxilary_run_files' are ignored. Unset 'target' to take them into account."
            LOGGER.warning(msg)

        # Link ICON binary
        (self.run_dir / self.bin.name).symlink_to(self.bin)

        # Take input/output ports specifications into account:
        # - adapt namelist paramters
        # - resolve data path
        # - link data
        self.handle_input_ports()
        self.handle_output_ports()

        # Dump namelists
        self.dump_namelists(directory=self.run_dir, filename_mode="raw")

        # Copy required runtime files
        if self.target is None:
            shutil.copy(self.config_rootdir / self.runscript_content, self.run_dir / self.runscript_content.name)   # type: ignore[operator, union-attr] # check on runscript_content done above
            if self.auxilary_run_files is not None:
                for aux_path in self.auxilary_run_files:
                    shutil.copy(self.config_rootdir / aux_path, self.run_dir / aux_path.name)
        else:
            shutil.copy(Path(__file__).parent / "santis_run_environment.sh", self.run_dir / "santis_run_environment.sh")
            if self.target == "santis_cpu":
                shutil.copy(Path(__file__).parent / "santis_cpu.sh", self.run_dir / "santis_cpu.sh")
            elif self.target == "santis_gpu":
                shutil.copy(Path(__file__).parent / "santis_gpu.sh", self.run_dir / "santis_gpu.sh")

    def runscript_lines(self) -> list[str]:
        lines = []
        if self.target is None:
            # NOTE: Only for type checking. Type checkers cannot know this method is called after prepare_for_submission where the check is made
            #       Again, if standalone becomes the only orchestrator, make this check a yaml model validator
            if self.runscript_content is None:
                msg = f"task {self.name}: 'runscript_content' is required when 'target' is unset"
                raise ValueError(msg)
            lines.append(f"source ./{self.runscript_content.name}")
        else:
            lines.append("source santis_run_environment.sh")
            match self.target:
                case "santis_cpu":
                    lines.append(
                        f"srun -n {self.nodes*self.ntasks_per_node} --ntasks-per-node {self.ntasks_per_node} --threads-per-core=1 --distribution=block:block:block ./santis_cpu.sh {self.bin.name}"  # type: ignore[operator]
                    )
                case "santis_gpu":
                    lines.append(
                        f"srun -n {self.nodes*self.ntasks_per_node} --ntasks-per-node {self.ntasks_per_node} --threads-per-core=1 --distribution=cyclic ./santis_gpu.sh {self.bin.name}"  # type: ignore[operator]
                    )
        return lines

    def handle_input_ports(self) -> None:
        """Reflect port specs in namelist and link necessary input data"""

        for port, data_list in self.inputs.items():
            if not data_list:
                continue
            match port:
                case "dynamics_grid_file":
                    self.adapt_nml_param_and_link(
                        port=port,
                        data_list=data_list,
                        namelist=self.model_namelist,
                        section="grid_nml",
                        parameter="dynamics_grid_filename",
                    )
                case "ecrad_data":
                    self.adapt_nml_param_and_link(
                        port=port,
                        data_list=data_list,
                        namelist=self.model_namelist,
                        section="radiation_nml",
                        parameter="ecrad_data_path",
                        target_link_name="ecrad_data",
                    )
                case "cloud_opt_props":
                    self.adapt_nml_param_and_link(
                        port=port,
                        data_list=data_list,
                        namelist=self.model_namelist,
                        section="nwp_phy_nml",
                        parameter="cldopt_filename",
                        target_link_name="CldOptProps.nc",
                    )
                case "rrtmg_lw":
                    self.adapt_nml_param_and_link(
                        port=port,
                        data_list=data_list,
                        namelist=self.model_namelist,
                        section="nwp_phy_nml",
                        parameter="lrtm_filename",
                        target_link_name="rrtmg_lw.nc",
                    )
                case "restart_file":
                    if (
                        restart_write_mode := self.model_namelist["io_nml"].get("restart_write_mode")
                        != "joint procs multifile"
                    ):
                        msg = f"Only supported restart_write_mode is 'joint procs multifile', got {restart_write_mode}"
                        raise ValueError(msg)
                    data = self.ensure_single_data_port(port, data_list)
                    (self.run_dir / "multifile_restart_atm.mfr").symlink_to(data.resolved_path)
                case _:
                    msg = f"IconTask: unsopported input port {port}"
                    raise ValueError(msg)

    def handle_output_ports(self) -> None:
        """Check namelist parameters and resolve output data path"""

        for port, data_list in self.outputs.items():
            if not data_list:
                continue
            match port:
                case "latest_restart_file":
                    data = self.ensure_single_data_port(port, data_list)
                    data.resolved_path = self.run_dir / "multifile_restart_atm.mfr"
                case "output_streams":
                    output_nml = self.model_namelist.get("output_nml", [])
                    nml_streams: list[f90nml.Namelist] = (
                        [output_nml] if isinstance(output_nml, f90nml.Namelist) else output_nml
                    )
                    if (n_nml := len(nml_streams)) != (n_yaml := len(data_list)):
                        msg = f"for task {self.name}: number of output streams speficied in namelist ({n_nml}) differs from number of streams specified the workflow config ({n_yaml})"
                        raise ValueError(msg)
                    for k, (nml_stream, output_data) in enumerate(zip(nml_streams, data_list)):
                        filename_format = nml_stream.get("filename_format", "<output_filename>_XXX_YYY")
                        output_filename = nml_stream.get("output_filename", "")
                        # for type checkers
                        if not isinstance(filename_format, str) or not isinstance(output_filename, str):
                            msg = f"for task {self.name}, output stream number {k}: 'filename_format' and 'output_filename' namelist parameters must be strings"
                            raise TypeError(msg)
                        stream_dir = Path(filename_format.replace("<output_filename>", output_filename)).parent
                        if stream_dir == Path("."):
                            msg = f"for task {self.name}: output stream number {k} specifies an output stream directly in the run directory. Please specify a subdirectory using the 'filename_format' and 'output_filename' parameters (see ICON documentation)"
                            raise ValueError(msg)
                        output_data.resolved_path = stream_dir
                case "finish":
                    data = self.ensure_single_data_port(port, data_list)
                    data.resolved_path = self.run_dir / "finish.status"
                case _:
                    msg = f"IconTask: unsopported oputput port {port}"
                    raise ValueError(msg)

    @staticmethod
    def ensure_single_data_port(port: str | None, data_list: Sequence[Data]) -> Data:
        if len(data_list) > 1:
            msg = f"port {port} only accepts one a single object"
            raise ValueError(msg)
        return data_list[0]

    def resolve_output_data_paths(self) -> None:
        self.handle_output_ports()

    def adapt_nml_param_and_link(
        self,
        port: str,
        data_list: list[Data],
        namelist: NamelistFile,
        section: str,
        parameter: str,
        target_link_name: str | None = None,
    ) -> None:
        data = self.ensure_single_data_port(port, data_list)
        if isinstance(data, GeneratedData):
            target_link_name = target_link_name if target_link_name else data.resolved_path.name
            namelist[section][parameter] = f"./{target_link_name}"
            (self.run_dir / target_link_name).symlink_to(data.resolved_path)
        else:
            namelist[section][parameter] = f"'{data.resolved_path}'"

    @classmethod
    def build_from_config(cls: type[Self], config: models.ConfigTask, config_rootdir: Path, **kwargs: Any) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        # The following check is here for type checkers.
        # We don't want to narrow the type in the signature, as that would break liskov substitution.
        # We guarantee elsewhere this is called with the correct type at runtime
        if not isinstance(config, models.ConfigIconTask):
            raise TypeError

        config_kwargs["namelists"] = [
            NamelistFile.from_config(config=config_namelist, config_rootdir=config_rootdir)
            for config_namelist in config_kwargs["namelists"]
        ]

        self = cls(
            config_rootdir=config_rootdir,
            **kwargs,
            **config_kwargs,
        )
        self.update_icon_namelists_from_workflow()
        return self

    def _validate_wrapper_script(self, wrapper_script: Path, config_rootdir: Path) -> Path:
        """Validate and resolve wrapper script path"""
        resolved_path = wrapper_script if wrapper_script.is_absolute() else config_rootdir / wrapper_script

        if not resolved_path.exists():
            msg = f"Wrapper script in path {resolved_path} does not exist."
            raise FileNotFoundError(msg)
        if not resolved_path.is_file():
            msg = f"Wrapper script in path {resolved_path} is not a file."
            raise OSError(msg)

        return resolved_path
