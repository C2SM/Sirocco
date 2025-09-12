from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Self

import f90nml
from aiida_icon.iconutils import masternml

from sirocco.core.graph_items import Task
from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(kw_only=True)
class IconTask(models.ConfigIconTaskSpecs, Task):
    _MASTER_NAMELIST_NAME: ClassVar[str] = field(default="icon_master.namelist", repr=False)
    _AIIDA_ICON_RESTART_FILE_PORT_NAME: ClassVar[str] = field(default="restart_file", repr=False)
    namelists: list[NamelistFile]

    def __post_init__(self):
        super().__post_init__()
        # Detect master namelist
        master_namelist = None
        for namelist in self.namelists:
            if namelist.name == self._MASTER_NAMELIST_NAME:
                master_namelist = namelist
                break
        if master_namelist is None:
            msg = f"Failed to read master namelists. Could not find {self._MASTER_NAMELIST_NAME!r} in namelists {self.namelists}"
            raise ValueError(msg)
        self._master_namelist = master_namelist

        # Parse master namelist to identify required model namelists
        master_nml_data = f90nml.reads(str(self._master_namelist.namelist))
        self._required_models = dict(masternml.iter_model_name_filepath(master_nml_data))

        # Build mapping of available model namelists
        self._model_namelists = {}
        namelist_by_name = {nml.name: nml for nml in self.namelists}

        for model_name, model_path in self._required_models.items():
            # Look for the namelist file by filename
            model_filename = model_path.name
            if model_filename in namelist_by_name:
                self._model_namelists[model_name] = namelist_by_name[model_filename]
            elif not model_path.is_absolute():
                # For relative paths, require the namelist to be provided
                msg = f"Missing model namelist for model '{model_name}': expected file '{model_filename}' not found in provided namelists"
                raise ValueError(msg)
            # For absolute paths, the file is expected to exist on the target system
            # We don't validate this here as it will be handled by aiida-icon

        if self.wrapper_script is not None:
            self.wrapper_script = self._validate_wrapper_script(self.wrapper_script, self.config_rootdir)

    @property
    def master_namelist(self) -> NamelistFile:
        return self._master_namelist

    @property
    def model_namelists(self) -> dict[str, NamelistFile]:
        """Return mapping of model names to their namelist files."""
        return self._model_namelists.copy()

    @property
    def required_models(self) -> dict[str, Path]:
        """Return mapping of model names to their expected file paths from master namelist."""
        return self._required_models.copy()

    @property
    def is_restart(self) -> bool:
        """Check if the icon task starts from the restart file."""
        # restart port must be present and nonempty
        return bool(self.inputs.get(self._AIIDA_ICON_RESTART_FILE_PORT_NAME, False))

    def update_icon_namelists_from_workflow(self):
        if not isinstance(self.cycle_point, DateCyclePoint):
            msg = "ICON task must have a DateCyclePoint"
            raise TypeError(msg)

        # Update master namelist
        self.master_namelist.update_from_specs(
            {
                "master_time_control_nml": {
                    "experimentStartDate": self.cycle_point.start_date.isoformat() + "Z",
                    "experimentStopDate": self.cycle_point.stop_date.isoformat() + "Z",
                    "restarttimeintval": str(self.cycle_point.period),
                },
                "master_nml": {"lrestart": self.is_restart, "read_restart_namelists": self.is_restart},
            }
        )

    def dump_namelists(self, directory: Path):
        if not directory.exists():
            msg = f"Dumping path {directory} does not exist."
            raise OSError(msg)
        if not directory.is_dir():
            msg = f"Dumping path {directory} is not directory."
            raise OSError(msg)

        # Dump all namelists with coordinate suffix
        for namelist in self.namelists:
            suffix = ("_".join([str(p) for p in self.coordinates.values()])).replace(" ", "_")
            filename = namelist.name + "_" + suffix
            namelist.dump(directory / filename)

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
