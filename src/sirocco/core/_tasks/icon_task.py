from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from aiida.orm import SinglefileData
import f90nml

from sirocco.core.graph_items import Task, Data
from sirocco.parsing._yaml_data_models import ConfigIconTaskSpecs, ConfigNamelist


@dataclass
class IconTask(ConfigIconTaskSpecs, Task):
    # To avoid non-default argument is followed after default one we overwrite
    # here the code attribute with a default one, since it required on the level
    # of the yaml file it does not limit the checks on user config input
    code: str = ""
    core_namelists: dict[str, f90nml.Namelist] = field(default_factory=dict)

    # TODO cannot do just super, because inputs are split between the two classes
    #      but why doies this work without any init constructor, need to think through
    #      this __init_subclass__ magic.
    #      We move the check to the temporary for now the draft of the PR
    #def __init__(self, *args, **kwargs):
    #    #super().__init__(*args, **kwargs)
    #    dict_keys(['config_rootdir', 'coordinates', 'start_date', 'end_date', 'inputs', 'outputs', 'namelists', 'computer', 'host', 'account', 'uenv', 'nodes', 'walltime', 'name'])
    # 
    #    if len(self.outputs) > 1:
    #        raise ValueError("Icon task received more than one but only one (icon restart file) is valid.")

    @property
    def master_namelist(self) -> ConfigNamelist:
        return self.namelists["icon_master.namelist"]

    @property
    def core_master_namelist(self) -> f90nml.Namelist:
        return self.core_namelists["icon_master.namelist"]

    @property
    def model_namelist(self) -> ConfigNamelist:
        model_namelist_name = self.core_master_namelist['master_model_nml']['model_namelist_filename']
        return self.namelists[model_namelist_name]

    @property
    def core_model_namelist(self) -> f90nml.Namelist:
        model_namelist_name = self.core_master_namelist['master_model_nml']['model_namelist_filename']
        return self.core_namelists[model_namelist_name]

    @property
    def restart_file(self) -> Data | None:
        # TODO move out of property
        if len(self.outputs) > 1:
            raise ValueError("Icon task received more than one but only one (icon restart file) is valid.")
        return self.outputs[0] if len(self.outputs) > 0 else None


    def init_core_namelists(self):
        """Read in or create namelists"""
        self.core_namelists = {}
        for name, cfg_nml in self.namelists.items():
            if (nml_path := self.config_rootdir / cfg_nml.path).exists():
                self.core_namelists[name] = f90nml.read(nml_path)
            else:
                # If namelist does not exist, build it from the users given specs
                self.core_namelists[name] = f90nml.Namelist()

    def update_core_namelists_from_config(self):
        """Update the core namelists from namelists provided by the user in the config yaml file."""

        # TODO: implement format for users to reference parameters and date in their specs
        for name, cfg_nml in self.namelists.items():
            core_nml = self.core_namelists[name]
            if cfg_nml.specs is None:
                continue
            for section, params in cfg_nml.specs.items():
                section_name, k = self.section_index(section)
                # Create section if non-existent
                if section_name not in core_nml:
                    # NOTE: f90nml will automatially create the corresponding nested f90nml.Namelist
                    #       objects, no need to explicitly use the f90nml.Namelist class constructor
                    core_nml[section_name] = {} if k is None else [{}]
                # Update namelist with user input
                # NOTE: unlike FORTRAN convention, user index starts at 0 as in Python
                if k == len(core_nml[section_name]) + 1:
                    # Create additional section if required
                    core_nml[section_name][k] = f90nml.Namelist()
                nml_section = core_nml[section_name] if k is None else core_nml[section_name][k]
                nml_section.update(params)

    def update_core_namelists_from_workflow(self):
        self.core_namelists["icon_master.namelist"]["master_time_control_nml"].update(
            {
                "experimentStartDate": self.start_date.isoformat() + "Z",
                "experimentStopDate": self.end_date.isoformat() + "Z",
            }
        )
        self.core_namelists["icon_master.namelist"]["master_nml"].update(
            {
                "lrestart": True,
            }
        )
        self.core_namelists["icon_master.namelist"]["master_nml"]["lrestart"] = any(
            # NOTE: in_data[0] contains the actual data node and in_data[1] the port name
            in_data[1] == "restart"
            for in_data in self.inputs
        )

    def dump_core_namelists(self, folder=None):
        if folder is not None:
            folder = Path(folder)
            folder.mkdir(parents=True, exist_ok=True)
        for name, cfg_nml in self.namelists.items():
            if folder is None:
                folder = (self.config_rootdir / cfg_nml.path).parent
            suffix = ("_".join([str(p) for p in self.coordinates.values()])).replace(" ", "_")
            self.core_namelists[name].write(folder / (name + "_" + suffix), force=True)

    def create_workflow_namelists(self, folder=None):
        self.init_core_namelists()
        self.update_core_namelists_from_config()
        self.update_core_namelists_from_workflow()
        self.dump_core_namelists(folder=folder)

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
