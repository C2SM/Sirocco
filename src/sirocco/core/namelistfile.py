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

    def __contains__(self, k) -> bool:
        return k in self.namelist

    def get(self, key: str, default: Any = None) -> Any:
        return self.namelist.get(key, default=default)

    def iter_nml(self, key: str) -> Iterator[f90nml.Namelist]:
        match nmls := self.get(key):
            case f90nml.Namelist():
                yield nmls
            case f90nml.namelist.Cogroup():
                yield from nmls

    @classmethod
    def from_config(cls: type[Self], config: models.ConfigNamelistFile, config_rootdir: Path) -> Self:
        path = cls._validate_namelist_path(config.path, config_rootdir)
        self = cls(path=path)
        self.update_from_specs(config.specs)
        return self

    def update_from_specs(self, specs: dict[str, Any]) -> None:
        """Updates the internal namelist from the specs.

        A section name ending with an index between brackets, e.g. `output_nml[2]`,
        targets a single occurrence of a potentially repeated section. The index
        starts at 1, follows the order of the namelist file and can go past the last
        occurrence in order to append new ones, as long as no index is skipped.
        Without index, all the occurrences of the section are updated.

        The result does not depend on the order of the specs: new occurrences are
        appended first, then specs without index are applied and finally the indexed
        ones, which thus take precedence."""

        # NOTE: k is the 0-based index corresponding to the 1-based user index
        indexed_specs: list[tuple[str, int, str, dict[str, Any]]] = []
        global_specs: list[tuple[str, dict[str, Any]]] = []
        for section, params in specs.items():
            section_name, k = self.section_index(section)
            if k is None:
                global_specs.append((section_name, params))
            else:
                indexed_specs.append((section_name, k, section, params))
        indexed_specs.sort(key=lambda spec: spec[1])

        # Append new occurrences of indexed sections
        for section_name, k, section, _ in indexed_specs:
            n_occurrences = len([*self.iter_nml(section_name)])
            if k < 0 or k > n_occurrences:
                msg = (
                    f"{self.name}: cannot update {section!r}, index must be between 1 and {n_occurrences + 1} "
                    f"(number of {section_name!r} sections + 1). New sections cannot skip an index."
                )
                raise ValueError(msg)
            if k == n_occurrences:
                self.namelist.add_cogroup(section_name, f90nml.Namelist())

        # Update all the occurrences of sections specified without index
        for section_name, params in global_specs:
            # Create section if non-existent
            if section_name not in self.namelist:
                # NOTE: f90nml will automatially create the corresponding nested f90nml.Namelist
                #       object, no need to explicitly use the f90nml.Namelist class constructor
                self.namelist[section_name] = {}
            self.namelist[section_name].update(params)

        # Update single occurrences
        for section_name, k, _, params in indexed_specs:
            [*self.iter_nml(section_name)][k].update(params)

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
