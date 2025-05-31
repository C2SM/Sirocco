from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Self

from sirocco.core.graph_items import Task
from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    from pathlib import Path


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

    def update_icon_namelists_from_workflow(self):
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
