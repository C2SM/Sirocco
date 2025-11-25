from dataclasses import dataclass, field
from typing import ClassVar

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models


# NOTE: SiroccoTask is an subclass of graphitem for implementation simplicity
# reasons eventhough it's not allowed to be part of the graph
@dataclass(kw_only=True)
class SiroccoContinueTask(models.ConfigSiroccoTaskSpecs, Task):
    """Special Sirocco Task for continuing the workflow"""

    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_continue.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="sirocco.log", repr=False)
    CLEAN_UP_BEFORE_SUBMIT: ClassVar[bool] = field(default=False, repr=False)  # Clean up directory when submitting
    LOCK_FILE_NAME: ClassVar[str] = field(default=".sirocco.lock", repr=False)

    name: str = "SIROCCO"
    computer: str | None = "dummy"
    rank: int = 0
    config_filename: str

    def __post_init__(self) -> None:
        self.run_dir = self.config_rootdir
        self.label = "Sirocco_continue"

    def resolve_output_data_paths(self) -> None:
        pass

    def prepare_for_submission(self) -> None:
        pass

    def runscript_lines(self) -> list[str]:
        script_lines: list[str] = []
        if self.venv:
            script_lines.append(f"source {self.venv}")
        script_lines.append(f"sirocco continue --from_wf {self.config_filename} || exit")
        return script_lines
