"""The special task that resubmits `sirocco continue`.

It only exists for the standalone engine, so it lives here and not in core.
It still subclasses `core.Task` so that the scheduler can submit it like any
other task.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from sirocco.core.graph_items import Task
from sirocco.engines.standalone.handlers import prepare_for_submission, runscript_lines
from sirocco.parsing import yaml_data_models as models


# NOTE: SiroccoTask is a subclass of `GraphItem` for implementation simplicity
# reasons (submission) even though it's not allowed to be part of the graph
@dataclass(kw_only=True)
class SiroccoContinueTask(models.ConfigSiroccoTaskSpecs, Task):
    """Special Sirocco Task for continuing the workflow"""

    plugin: ClassVar[str] = "sirocco_continue"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_continue.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="sirocco.log", repr=False)
    CLEAN_UP_BEFORE_SUBMIT: ClassVar[bool] = field(default=False, repr=False)  # Clean up directory when submitting
    LOCK_FILE_NAME: ClassVar[str] = field(default=".sirocco.lock", repr=False)

    name: str = "SIROCCO"
    computer: str = "dummy"
    config_rootdir: Path

    def __post_init__(self) -> None:
        self.run_dir = self.config_rootdir
        self.label = "Sirocco_continue"

    def resolve_output_data_paths(self) -> None:
        pass


@prepare_for_submission.register
def prepare_for_submission_sirocco_continue(task: SiroccoContinueTask) -> None:
    pass


@runscript_lines.register
def runscript_lines_sirocco_continue(task: SiroccoContinueTask) -> list[str]:
    lines: list[str] = []
    if task.venv is not None:
        lines.append(f"source {task.venv}/bin/activate")
    cmd: list[str] = ["sirocco"]
    if os.environ.get("NO_COLOR") == "1":
        cmd.append("--no-ansi")
    cmd.append(f"continue --from_wf {task.config_rootdir} || exit")
    lines.append(" ".join(cmd))
    return lines
