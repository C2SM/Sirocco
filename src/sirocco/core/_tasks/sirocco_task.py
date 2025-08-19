from dataclasses import dataclass

from sirocco.parsing import yaml_data_models as models
from sirocco.core.graph_items import Task


# NOTE: SiroccoTask is an subclass of graphitem for implementation simplicity
# reasons eventhough it's not allowed to be part of the graph
@dataclass(kw_only=True)
class SiroccoTask(models.ConfigSiroccoTaskSpecs, Task):
    """Special Task for orchestrating the workflow"""

    name: str = "Sirocco"
    computer: str = "dummy"

    def __post_init__(self) -> None:
        self.label = "Sirocco"
        self.run_dir = self.config_rootdir
