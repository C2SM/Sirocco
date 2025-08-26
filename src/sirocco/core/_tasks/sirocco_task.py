from dataclasses import dataclass, field
from typing import ClassVar

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models


# NOTE: SiroccoTask is an subclass of graphitem for implementation simplicity
# reasons eventhough it's not allowed to be part of the graph
@dataclass(kw_only=True)
class SiroccoContinueTask(models.ConfigSiroccoTaskSpecs, Task):
    """Special Task for orchestrating the workflow"""

    SUBMIT_FILENAME: ClassVar[str] = field(default="sirocco_run_script.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="sirocco.log", repr=False)

    name: str = "SIROCCO"
    computer: str = "dummy"
    rank: int = 0
    config_filename: str

    def __post_init__(self) -> None:
        self.label = "Sirocco"
        self.run_dir = self.config_rootdir
        self.jobid_path = self.run_dir / self.JOBID_FILENAME
        self.rank_path = self.run_dir / self.RANK_FILENAME

    def resolve_output_data_paths(self) -> None:
        pass

    def prepare_for_submission(self) -> None:
        pass

    def runscript_lines(self) -> list[str]:
        script_lines = []
        if self.venv:
            script_lines.append(f"source {self.venv}")
        script_lines.append(f"sirocco continue --from_wf {self.config_rootdir}/{self.config_filename}")
        return script_lines
