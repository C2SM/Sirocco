from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

from sirocco.core._tasks.icon_task.ports import IconModel, ModelType, PortHandler, restart_in_handler
from sirocco.core.graph_items import Task
from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing import yaml_data_models
from sirocco.parsing.cycling import DateCyclePoint

LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class IconTask(yaml_data_models.ConfigIconTaskSpecs, Task):
    SUPPORTED_MACHINES: ClassVar[list[str]] = field(default=["santis"], repr=False)
    _MASTER_NAMELIST_NAME: ClassVar[str] = field(default="icon_master.namelist", repr=False)
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
            if not isinstance((filename := master_model_nml.get("model_namelist_filename")), str):
                msg = "master_model_nml does not contain a valid 'model_namelist_filename' parameter"
                raise KeyError(msg)
            if filename not in namelist_by_filename:
                msg = f"namelist {filename} required by {self._MASTER_NAMELIST_NAME!r} not found in provided namelists"
                raise KeyError(msg)
            if not isinstance(model_name := master_model_nml.get("model_name"), str):
                msg = f"master_model_nml associated to {filename} does not contain a valid 'model_name' parameter"
                raise KeyError(msg)
            model_namelists[model_name] = namelist_by_filename[filename]
            if not isinstance(model_type := master_model_nml.get("model_type"), int):
                msg = f"master_model_nml associated to {filename} does not contain a valid 'model_type' parameter"
                raise KeyError(msg)
            model_types[model_name] = model_type

        # Check if models and config component names match
        if (model_names := set(model_namelists.keys())) != (
            comp_names := {k for k in self.components if k != "master"}
        ):
            msg = f"models specified in {self._MASTER_NAMELIST_NAME} ({model_names}) don't match components from the config ({comp_names})"
            raise ValueError(msg)

        # Build ICON models
        for comp_name, component in self.components.items():
            if comp_name != "master":
                self.models[comp_name] = IconModel(
                    name=comp_name,
                    core_component=component,
                    task_label=self.label,
                    task_run_dir=self.run_dir,
                    namelist=model_namelists[comp_name],
                    model_type=ModelType(model_types[comp_name]),
                )

        # Validate wrapper script
        # TODO: Remove this once aiida-icon is synced with standalone
        if self.wrapper_script is not None:
            self.wrapper_script = self.config_rootdir / self.wrapper_script
            if not self.wrapper_script.exists():
                msg = f"Wrapper script in path {self.wrapper_script} does not exist."
                raise FileNotFoundError(msg)
            if not self.wrapper_script.is_file():
                msg = f"Wrapper script in path {self.wrapper_script} is not a file."
                raise OSError(msg)

    @property
    def is_restart(self) -> bool:
        """Check if the icon task starts from restart file(s)."""
        # Get restart status of first model
        restart_ref = bool(next(iter(self.models.values())).inputs.get(restart_in_handler.port_name, False))
        # Check if all models have the same restart status
        for model in self.models.values():
            if bool(model.inputs.get(restart_in_handler.port_name, False)) != restart_ref:
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
                    # Use Z instead of +00:00 for UTC (ISO 8601 compact form)
                    "experimentStartDate": self.cycle_point.chunk_start_date.isoformat().replace("+00:00", "Z"),
                    "experimentStopDate": self.cycle_point.chunk_stop_date.isoformat().replace("+00:00", "Z"),
                    "restartTimeIntval": str(self.cycle_point.period),
                    "checkpointTimeIntval": str(self.cycle_point.period),
                },
                "master_nml": {
                    "lrestart": self.is_restart,
                    # TODO: check what this really means and if we need it set
                    #       up automatically from the workflow
                    "read_restart_namelists": self.is_restart,
                },
            }
        )

    def allocate_tasks(self) -> None:
        """Allocate task ranges to icon components

        Takes the following into account:
        - target architecture (cpu, gpu, hybrid)
        - task layout (separate I/O, compute layout)
        - number of nodes
        - number of comput tasks per nodes
        - number of I/O tasks per I/O node (if separate I/O)
        - compute weights
        - IO tasks"""
        raise NotImplementedError

    def distribute_tasks(self) -> None:
        """Compute task distribution and dump to template file with placeholder node names"""
        raise NotImplementedError

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
                from datetime import datetime

                # Format coordinates - strip timezone from datetimes (all Sirocco datetimes are UTC)
                coord_strs = []
                for p in self.coordinates.values():
                    if isinstance(p, datetime):
                        # Remove timezone for filename (keeps original format otherwise)
                        coord_strs.append(str(p.replace(tzinfo=None)))
                    else:
                        coord_strs.append(str(p))
                suffix = "_".join(coord_strs).replace(" ", "_")
                filename += "_" + suffix
            namelist.dump(directory / filename)

    def prepare_for_submission(self) -> None:
        # Ensure either target or runscript is set
        # NOTE: Some validation code is there as it is the first available place where we know the standalone orchestrator is used
        # TODO: unify `runtime` spec with aiida-icon and move validation code to parsing if possible

        # Check supported machine and set target
        if self.computer not in self.SUPPORTED_MACHINES:
            msg = (
                f"machine {self.computer} not suportted for icon task. Supported machines are {self.SUPPORTED_MACHINES}"
            )
            raise ValueError(msg)

        executables: list[str] = []
        if self.exe.cpu is not None:
            executables.append("cpu")
        if self.exe.gpu is not None:
            executables.append("gpu")
        if self.exe.hiopy is not None:
            executables.append("hiopy")
        target = "-".join(executables)

        # Link ICON binaries
        if self.exe.cpu is not None:
            (self.run_dir / "icon_cpu").symlink_to(self.exe.cpu.path)
        if self.exe.gpu is not None:
            (self.run_dir / "icon_gpu").symlink_to(self.exe.gpu.path)

        # Take input/output ports specifications into account:
        # - adapt namelist parameters
        # - resolve data path
        # - link data
        for model in self.models.values():
            for port in chain(model.inputs, model.outputs):
                PortHandler.handle(port, model)

        # Dump namelists
        self.dump_namelists(directory=self.run_dir, filename_mode="raw")

        # Set default MPI variables
        # NOTE: If it grows, This can end up in site specific modules / methods / functions
        if self.nodes is None:
            self.nodes = 1
        if self.ntasks_per_node is None:  # noqa: SIM102  keep line for machine check for future supported machines
            if self.computer == "santis":
                match target:
                    case "cpu":
                        self.ntasks_per_node = 288
                    case "gpu":
                        self.ntasks_per_node = 4

        # Copy required runtime files
        if self.runtime is None:
            runtime_dir = Path(__file__).parent / self.computer / target
            if not runtime_dir.exists():
                msg = f"target {target} not defined for computer {self.computer}"
                raise ValueError(msg)
        else:
            runtime_dir = self.config_rootdir / self.runtime
            if not runtime_dir.is_dir():
                msg = f"{self.name}: 'runtime' directory not found at {runtime_dir}"
                raise ValueError(msg)
            if not (runtime_dir / self._MAIN).is_file():
                msg = f"{self._MAIN} not found in runtime at {runtime_dir}"
                raise ValueError(msg)
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
