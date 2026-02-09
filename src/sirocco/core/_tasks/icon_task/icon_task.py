from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

from sirocco.core._tasks.icon_task.ports import IconModel, ModelType, PortHandler
from sirocco.core.graph_items import Task
from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing import yaml_data_models
from sirocco.parsing.cycling import DateCyclePoint

LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class IconTask(yaml_data_models.ConfigIconTaskSpecs, Task):
    _MASTER_NAMELIST_NAME: ClassVar[str] = field(default="icon_master.namelist", repr=False)
    _MASTER_MODEL_NML_SECTION: ClassVar[str] = field(default="master_model_nml", repr=False)
    _AIIDA_ICON_RESTART_FILE_PORT_NAME: ClassVar[str] = field(default="restart_file", repr=False)
    _MAIN: ClassVar[str] = field(default="main.sh", repr=False)
    namelists: list[NamelistFile]
    master_namelist: NamelistFile = field(init=False, repr=False)
    models: dict[str, IconModel] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        # Detect and set master namelist
        master_namelist: NamelistFile | None = None
        for namelist in self.namelists:
            if namelist.name == self._MASTER_NAMELIST_NAME:
                master_namelist = namelist
                break
        if master_namelist is None:
            msg = f"Failed to read master namelists. Could not find {self._MASTER_NAMELIST_NAME!r} in namelists {self.namelists}"
            raise ValueError(msg)
        self.master_namelist = master_namelist

        # Build dictionnary mapping component names to model namelists
        namelist_by_filename: dict[str, NamelistFile] = {nml.name: nml for nml in self.namelists}
        model_namelists: dict[str, NamelistFile] = {}
        model_types: dict[str, int] = {}
        for master_model_nml in self.master_namelist.iter_nml("master_model_nml"):
            if (filename := master_model_nml["model_namelist_filename"]) not in namelist_by_filename:
                msg = f"namelist {filename} required by {self._MASTER_NAMELIST_NAME!r} not found in provided namelists"
                raise ValueError(msg)
            if (model_name := master_model_nml.get("model_name")) is None:
                msg = f"master_model_nml associated to {filename} has no 'model_name' parameter"
                raise ValueError(msg)
            model_namelists[model_name] = namelist_by_filename[filename]
            if (model_type := master_model_nml.get("model_type")) is None:
                msg = f"master_model_nml associated to {filename} has no 'model_type' parameter"
                raise ValueError(msg)
            model_types[model_name] = model_type

        # Check if models and config component names match
        model_names = set(model_namelists.keys())
        comp_names = {k for k in self.components if k != "master"}
        if (model_names := set(model_namelists.keys())) != (comp_names := set(self.components.keys())):
            msg = f"models specified in {self._MASTER_NAMELIST_NAME} ({model_names}) don't match components from the config ({comp_names})"
            raise ValueError(msg)

        # Build ICON models
        for comp_name, component in self.components.items():
            if comp_name != "master":
                namelist = model_namelists[comp_name]
                self.models[comp_name] = IconModel(
                    name=comp_name,
                    core_component=component,
                    task_name=self.name,
                    task_run_dir=self.run_dir,
                    namelist=namelist,
                    model_type=ModelType(model_types[comp_name]),
                )

        # Set wrapper script
        # TODO: sync this aiida-icon feature with standalone orchestration
        #       where wrapper script is not what the user provides
        if self.wrapper_script is not None:
            self.wrapper_script = self._validate_wrapper_script(self.wrapper_script, self.config_rootdir)

        # Set default MPI variables
        self.nodes = 1 if self.nodes is None else self.nodes
        if self.ntasks_per_node is None:
            if self.target == "santis_cpu":
                self.ntasks_per_node = 288
            elif self.target == "santis_gpu":
                self.ntasks_per_node = 4

    # FIXME: This is only used by workgraph, use self.models directly instead
    @property
    def model_namelists(self) -> dict[str, NamelistFile]:
        """Return mapping of model names to their namelist files."""
        return {name: model.namelist for name, model in self.models.items()}

    # NOTE: This is only used by workgraph, remove if unnecessary
    @property
    def model_namelist(self) -> NamelistFile:
        # NOTE: This is a workaround until multiple models are implemented
        if len(self.models) != 1:
            msg = "multiple models not yet implemented"
            raise NotImplementedError(msg)
        return next(iter(self.models.values())).namelist

    @property
    def is_restart(self) -> bool:
        """Check if the icon task starts from restart file(s)."""
        # Get restart status of first model
        restart_ref = bool(next(iter(self.models.values())).inputs.get(self._AIIDA_ICON_RESTART_FILE_PORT_NAME, False))
        # Check if all models have the same restart status
        for model in self.models.values():
            if bool(model.inputs.get(self._AIIDA_ICON_RESTART_FILE_PORT_NAME, False)) != restart_ref:
                msg = "All CON models must have the same restart status"
                raise ValueError(msg)
        return restart_ref

    def update_icon_namelists_from_workflow(self) -> None:
        if not isinstance(self.cycle_point, DateCyclePoint):
            msg = "ICON task must have a DateCyclePoint"
            raise TypeError(msg)
        self.master_namelist.update_from_specs(
            {
                "master_time_control_nml": {
                    "experimentStartDate": self.cycle_point.start_date.isoformat() + "Z",
                    "experimentStopDate": self.cycle_point.stop_date.isoformat() + "Z",
                    "restartTimeIntval": str(self.cycle_point.period),
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
        # TODO: unify `runtime` spec with aiida-icon and then make this a yaml model validator
        if self.target is None:
            if self.runtime is None:
                msg = f"task {self.name}: 'runtime' is required when 'target' is unset"
                raise ValueError(msg)
        elif self.runtime is not None:
            msg = f"task {self.name}: 'target' set to {self.target}: 'runtime' is ignored. Unset 'target' to take it into account."
            LOGGER.warning(msg)

        # Link ICON binary
        match self.target:
            case None:
                if self.bin is not None:
                    (self.run_dir / "icon").symlink_to(self.bin)
                if self.bin_cpu is not None:
                    (self.run_dir / "icon_cpu").symlink_to(self.bin_cpu)
                if self.bin_gpu is not None:
                    (self.run_dir / "icon_gpu").symlink_to(self.bin_gpu)
            case "santis_cpu":
                if self.bin_cpu is not None:
                    (self.run_dir / "icon_cpu").symlink_to(self.bin_cpu)
                elif self.bin is not None:
                    (self.run_dir / "icon_cpu").symlink_to(self.bin)
            case "santis_gpu":
                if self.bin_gpu is not None:
                    (self.run_dir / "icon_gpu").symlink_to(self.bin_gpu)
                elif self.bin is not None:
                    (self.run_dir / "icon_gpu").symlink_to(self.bin)

        # Take input/output ports specifications into account:
        # - adapt namelist parameters
        # - resolve data path
        # - link data
        for model in self.models.values():
            for port in chain(model.inputs, model.outputs):
                PortHandler.handle(port, model)

        # Dump namelists
        self.dump_namelists(directory=self.run_dir, filename_mode="raw")

        # Copy required runtime files
        if self.target is None:
            # NOTE: dupplicate validation for type checker
            if self.runtime is None:
                msg = f"task {self.name}: 'runtime' is required when 'target' is unset"
                raise ValueError(msg)
            runtime_dir = self.config_rootdir / self.runtime
            if not (runtime_dir / self._MAIN).is_file():
                msg = f"{self._MAIN} not found in runtime at {runtime_dir}"
                raise ValueError(msg)
            if not runtime_dir.is_dir():
                msg = f"{self.name}: 'runtime' directory not found at {runtime_dir}"
                raise ValueError(msg)
        else:
            runtime_dir = Path(__file__).parent / "target_runtime" / self.target
        shutil.copytree(runtime_dir, self.run_dir, dirs_exist_ok=True)

    def runscript_lines(self) -> list[str]:
        return [f"source ./{self._MAIN}"]

    def resolve_output_data_paths(self) -> None:
        for model in self.models.values():
            for port in model.outputs:
                PortHandler.handle(port, model)

    @classmethod
    def build_from_config(
        cls: type[Self], config: yaml_data_models.ConfigTask, config_rootdir: Path, **kwargs: Any
    ) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        # The following check is here for type checkers.
        # We don't want to narrow the type in the signature, as that would break liskov substitution.
        # We guarantee elsewhere this is called with the correct type at runtime
        if not isinstance(config, yaml_data_models.ConfigIconTask):
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
