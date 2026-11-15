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
from sirocco.engines.standalone.handlers import (
    SubmissionSettings,
    prepare_for_submission,
    resolve_output_paths,
    run_dir,
    runscript_lines,
    submission_settings,
)
from sirocco.parsing import yaml_data_models as models

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.engines.standalone.path_resolver import PathResolver


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


@submission_settings.register(SiroccoContinueTask)
def submission_settings_sirocco_continue(task: SiroccoContinueTask) -> SubmissionSettings:
    # The run directory is the config directory, never wipe it
    return SubmissionSettings(
        submit_filename=task.SUBMIT_FILENAME,
        stdouterr_filename=task.STDOUTERR_FILENAME,
        clean_up_run_dir=False,
    )


@run_dir.register(SiroccoContinueTask)
def run_dir_sirocco_continue(task: SiroccoContinueTask, path_resolver: PathResolver) -> Path:  # noqa: ARG001
    # Runs from the config directory, where it keeps its log
    return task.config_rootdir


@resolve_output_paths.register(SiroccoContinueTask)
def resolve_output_paths_sirocco_continue(task: SiroccoContinueTask, path_resolver: PathResolver) -> None:
    pass  # No outputs


@prepare_for_submission.register(SiroccoContinueTask)
def prepare_for_submission_sirocco_continue(task: SiroccoContinueTask, path_resolver: PathResolver) -> None:
    pass


@runscript_lines.register(SiroccoContinueTask)
def runscript_lines_sirocco_continue(task: SiroccoContinueTask, path_resolver: PathResolver) -> list[str]:  # noqa: ARG001
    lines: list[str] = []
    if task.venv is not None:
        lines.append(f"source {task.venv}/bin/activate")
    cmd: list[str] = ["sirocco"]
    if os.environ.get("NO_COLOR") == "1":
        cmd.append("--no-ansi")
    cmd.append(f"continue --from_wf {task.config_rootdir} || exit")
    lines.append(" ".join(cmd))
    return lines
