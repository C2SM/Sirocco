"""The special task that resubmits `sirocco continue`.

It only exists for the standalone engine, so it lives here and not in core.
It still subclasses `core.Task` so that the scheduler can submit it like any
other task.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from sirocco.core.graph_items import Task
from sirocco.engines.standalone.plugin_handlers.plugin_handler import PluginHandler
from sirocco.parsing import yaml_data_models as models

if TYPE_CHECKING:
    from pathlib import Path


# NOTE: SiroccoTask is a subclass of `GraphItem` for implementation simplicity
# reasons (submission) even though it's not allowed to be part of the graph
@dataclass(kw_only=True)
class SiroccoContinueTask(models.ConfigSiroccoTaskSpecs, Task):
    """Special Sirocco Task for continuing the workflow"""

    plugin: ClassVar[str] = "sirocco_continue"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_continue.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="sirocco.log", repr=False)
    LOCK_FILE_NAME: ClassVar[str] = field(default=".sirocco.lock", repr=False)

    name: str = "SIROCCO"
    computer: str = "dummy"
    config_rootdir: Path

    def __post_init__(self) -> None:
        self.label = "Sirocco_continue"


class SiroccoTaskHandler(PluginHandler[SiroccoContinueTask]):
    submit_filename = SiroccoContinueTask.SUBMIT_FILENAME
    stdouterr_filename = SiroccoContinueTask.STDOUTERR_FILENAME
    clean_up_rundir = False  # The run directory is the config directory, never wipe it

    def get_rundir(self) -> Path:
        # Runs from the config directory, where it keeps its log
        return self.task.config_rootdir

    def resolve_output_paths(self) -> None:
        pass  # No outputs

    def prepare_rundir_content(self) -> None:
        pass

    def get_runscript_lines(self) -> list[str]:
        lines: list[str] = []
        if self.task.venv is not None:
            lines.append(f"source {self.task.venv}/bin/activate")
        cmd: list[str] = ["sirocco"]
        if os.environ.get("NO_COLOR") == "1":
            cmd.append("--no-ansi")
        cmd.append(f"continue --from_wf {self.task.config_rootdir} || exit")
        lines.append(" ".join(cmd))
        return lines
