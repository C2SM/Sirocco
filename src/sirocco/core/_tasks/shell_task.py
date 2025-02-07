from __future__ import annotations

from dataclasses import dataclass

from sirocco.core.graph_items import Task
from sirocco.parsing.yaml_data_models import ConfigShellTaskSpecs


@dataclass(kw_only=True)
class ShellTask(ConfigShellTaskSpecs, Task):
    pass
