import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

import f90nml

from sirocco.parsing import yaml_data_models as models


@dataclass(kw_only=True)
class NamelistFile(models.ConfigNamelistFileSpec):
    """A wrapper Class around f90nml.namelist.Namelist

    - adds a name and a path
    - adds the update_from_specs method"""

    name: str = field(init=False)
    path: Path = field(repr=False)
    namelist: f90nml.Namelist = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.name = self.path.name
        self.namelist = f90nml.read(self.path)

    def __getitem__(self, k) -> Any:
        return self.namelist.__getitem__(k)

    def __setitem__(self, k, v) -> None:
        self.namelist.__setitem__(k, v)

    def get(self, key: str, default: Any = None) -> Any:
        return self.namelist.get(key, default=default)

    def iter_nml(self, key: str) -> Iterator[f90nml.Namelist]:
        match nmls := self.get(key):
            case f90nml.Namelist():
                yield nmls
            case f90nml.namelist.Cogroup():
                yield from nmls
            case _:
                yield

    @classmethod
    def from_config(cls: type[Self], config: models.ConfigNamelistFile, config_rootdir: Path) -> Self:
        path = cls._validate_namelist_path(config.path, config_rootdir)
        self = cls(path=path)
        self.update_from_specs(config.specs)
        return self

    def update_from_specs(self, specs: dict[str, Any]) -> None:
        """Updates the internal namelist from the specs."""
        for section, params in specs.items():
            section_name, k = self.section_index(section)
            # Create section if non-existent
            if section_name not in self.namelist:
                # NOTE: f90nml will automatially create the corresponding nested f90nml.Namelist
                #       objects, no need to explicitly use the f90nml.Namelist class constructor
                self.namelist[section_name] = {} if k is None else [{}]
            # Update namelist with user input
            # NOTE: unlike FORTRAN convention, user index starts at 0 as in Python
            if k == len(self.namelist[section_name]) + 1:
                # Create additional section if required
                self.namelist[section_name][k] = f90nml.Namelist()
            nml_section = self.namelist[section_name] if k is None else self.namelist[section_name][k]
            nml_section.update(params)

    def dump(self, path: Path) -> None:
        if path.is_file():
            path.unlink()
        if path.is_dir():
            msg = f"Cannot write namelist {path.name} to path {path.name} already exists."
            raise OSError(msg)
        self.namelist.write(path)

    @staticmethod
    def _validate_namelist_path(config_namelist_path: Path, config_rootdir: Path) -> Path:
        if config_namelist_path.is_absolute():
            msg = f"Namelist path {config_namelist_path} must be relative with respect to config file."
            raise ValueError(msg)
        namelist_path = config_rootdir / config_namelist_path
        if not namelist_path.exists():
            msg = f"Namelist in path {namelist_path} does not exist."
            raise FileNotFoundError(msg)
        if not namelist_path.is_file():
            msg = f"Namelist in path {namelist_path} is not a file."
            raise OSError(msg)
        return namelist_path

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
