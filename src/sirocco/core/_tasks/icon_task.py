from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Self

import f90nml

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(kw_only=True)
class Namelist(models.ConfigNamelistSpec):
    name: str = field(init=False)

    def __post_init__(self):
        self.update_from_path(self.path)
        self.name = self.path.name

    @property
    def content(self) -> f90nml.Namelist:
        return self._content

    def update_from_path(self, path: Path) -> None:
        """Updates the internal content from a namelist in path."""
        self._content = f90nml.read(path)
        self.path = path

    def update_from_specs(self, specs: dict[str, Any]) -> None:
        """Updates the internal content from the specs."""
        for section, params in specs.items():
            section_name, k = self.section_index(section)
            # Create section if non-existent
            if section_name not in self.content:
                # NOTE: f90nml will automatially create the corresponding nested f90nml.Namelist
                #       objects, no need to explicitly use the f90nml.Namelist class constructor
                self.content[section_name] = {} if k is None else [{}]
            # Update namelist with user input
            # NOTE: unlike FORTRAN convention, user index starts at 0 as in Python
            if k == len(self.content[section_name]) + 1:
                # Create additional section if required
                self.content[section_name][k] = f90nml.Namelist()
            nml_section = self.content[section_name] if k is None else self.content[section_name][k]
            nml_section.update(params)

    def dump_content(self, path: Path):
        import io

        with io.StringIO() as buffer:
            self.content.write(buffer)
            path.write_text(buffer.getvalue())

    @staticmethod
    def section_index(section_name) -> tuple[str, int | None]:
        """Check for single vs multiple namelist section

        Check if the user specified a section name that ends with digits
        between brackets, for example:

        section_index("section[123]") -> ("section", 123)
        section_index("section123") -> ("section123", None)

        This is the convention chosen to indicate multiple
        sections with the same name, typically `output_nml` for multiple
        output streams."""
        multi_section_pattern = re.compile(r"(.*)\[([0-9]+)\]$")
        if m := multi_section_pattern.match(section_name):
            return m.group(1), int(m.group(2)) - 1
        return section_name, None


@dataclass(kw_only=True)
class IconTask(models.ConfigIconTaskSpecs, Task):
    _MASTER_NAMELIST_NAME: ClassVar[str] = field(default="icon_master.namelist", repr=False)
    _AIIDA_ICON_RESTART_FILE_PORT_NAME: ClassVar[str] = field(default="restart_file", repr=False)
    namelists: list[Namelist]

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
        if (master_model_nml := self._master_namelist.content.get("master_model_nml", None)) is None:
            msg = "No model filename specified in master namelist: Could not find section '&master_model_nml'"
            raise ValueError(msg)
        if (model_namelist_filename := master_model_nml.get("model_namelist_filename")) is None:
            msg = "No model filename specified in master namelist: Could not find entry 'model_namelist_filename' under section '&master_model_nml'"
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
    def master_namelist(self) -> Namelist:
        return self._master_namelist

    @property
    def model_namelist(self) -> Namelist:
        return self._model_namelist

    @property
    def is_restart(self) -> bool:
        """Check if the icon task starts from the restart file."""
        return self._AIIDA_ICON_RESTART_FILE_PORT_NAME in self.inputs

    def update_namelists_from_workflow(self):
        if not isinstance(self.cycle_point, DateCyclePoint):
            msg = "ICON task must have a DateCyclePoint"
            raise TypeError(msg)
        self.master_namelist.update_from_specs(
            {
                "master_time_control_nml": {
                    "experimentStartDate": self.cycle_point.start_date.isoformat() + "Z",
                    "experimentStopDate": self.cycle_point.stop_date.isoformat() + "Z",
                },
                "master_nml": {"lrestart": self.is_restart, "read_restart_namelists": self.is_restart},
            }
        )

    def dump_namelists(self, path: Path):
        if not path.exists():
            msg = f"Dumping path {path} does not exist."
            raise OSError(msg)
        if not path.is_dir():
            msg = f"Dumping path {path} is not directory."
            raise OSError(msg)

        for namelist in self.namelists:
            suffix = ("_".join([str(p) for p in self.coordinates.values()])).replace(" ", "_")
            filename = namelist.name + "_" + suffix
            namelist.dump_content(path / filename)

    @classmethod
    def build_from_config(cls: type[Self], config: models.ConfigTask, **kwargs: Any) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        # The following check is here for type checkers.
        # We don't want to narrow the type in the signature, as that would break liskov substitution.
        # We guarantee elsewhere this is called with the correct type at runtime
        if not isinstance(config, models.ConfigIconTask):
            raise TypeError

        def validate_config_namelist_path(config_namelist_path: Path, rootdir: Path) -> Path:
            namelist_path = (
                rootdir / config_namelist_path if config_namelist_path.is_relative_to(".") else config_namelist_path
            )
            if not namelist_path.exists():
                msg = f"Namelist in path {namelist_path} does not exist."
                raise FileNotFoundError(msg)
            if not namelist_path.is_file():
                msg = f"Namelist in path {namelist_path} is not a file."
                raise OSError(msg)
            return namelist_path

        config_rootdir = kwargs["config_rootdir"]
        namelists = []
        for config_namelist in config_kwargs["namelists"]:
            namelist_path = validate_config_namelist_path(config_namelist.path, config_rootdir)
            namelist = Namelist(path=namelist_path)
            namelist.update_from_specs(config_namelist.specs)
            namelists.append(namelist)
        config_kwargs["namelists"] = namelists

        return cls(
            **kwargs,
            **config_kwargs,
        )
