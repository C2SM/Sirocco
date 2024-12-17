from __future__ import annotations

from dataclasses import dataclass

from sirocco.core.graph_items import Task
from sirocco.parsing._yaml_data_models import ConfigIconTaskSpecs


@dataclass
class IconTask(ConfigIconTaskSpecs, Task):
    def update_nml_from_config(self):
        pass
