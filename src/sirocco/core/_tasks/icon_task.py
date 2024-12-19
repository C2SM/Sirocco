from __future__ import annotations

import re
from dataclasses import dataclass, field

import f90nml

from sirocco.core.graph_items import Task
from sirocco.parsing._yaml_data_models import ConfigIconTaskSpecs


@dataclass
class IconTask(ConfigIconTaskSpecs, Task):
    core_namelists: dict[str, f90nml.Namelist] = field(default_factory=dict)

    def init_namelists(self):
        """Read in or create namelists"""
        self.core_namelists = {}
        for name, cfg_nml in self.namelists.items():
            if (nml_path := self.config_root / cfg_nml.path).exists():
                self.core_namelists[name] = f90nml.read(nml_path)
            else:
                self.core_namelists[name] = f90nml.Namelist()

    def update_nml_from_config(self):
        """Update namelists from user input"""

        # TODO: implement format for users to reference parameters and date in their specs
        for name, cfg_nml in self.namelists.items():
            core_nml = self.core_namelists[name]
            for section, params in cfg_nml.specs.items():
                section_name, k = self.section_index(section)
                # Create section if non existant
                # NOTE: f90nml will automatially create the corresponding nested f90nml.Namelist
                #       objects, no need to explicitly use the f90nml.Namelist class constructor
                if section_name not in core_nml:
                    core_nml[section_name] = {} if k is None else [{}]
                # Update namelist with user input
                # NOTE: unlike FORTRAN convention, user index starts at 0 as in Python
                if k == len(core_nml[section_name]) + 1:
                    core_nml[section_name][k] = f90nml.Namelist
                nml_section = core_nml[section_name] if k is None else core_nml[section_name][k]
                nml_section.update(params)

    def update_nml_from_workflow(self):
        self.core_namelists["icon_master.namelist"]["master_time_control_nml"].update(
            {"experimentStartDate": self.start_date, "experimentStopDate": self.end_date}
        )
        self.core_namelists["icon_master.namelist"]["master_nml"]["lrestart"] = any(
            in_data.type == "icon_restart" for in_data in self.inputs
        )

    @staticmethod
    def section_index(section_name):
        multi_section_pattern = re.compile(r"(.*)\[([0-9]+)\]$")
        if m := multi_section_pattern.match(section_name):
            return m.group(1), int(m.group(2)) - 1
        return section_name, None
