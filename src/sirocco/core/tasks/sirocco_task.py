from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Literal, Self

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import OneOffPoint

if TYPE_CHECKING:
    from pathlib import Path


# NOTE: SiroccoTask is a subclass of `GraphItem` for implementation simplicity
# reasons (submission) even though it's not allowed to be part of the graph


@dataclass(kw_only=True)
class SiroccoTask(models.ConfigSiroccoTaskSpecs, Task):
    plugin: ClassVar[str] = "sirocco_undefined"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_undefined.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="sirocco.log", repr=False)
    CLEAN_UP_BEFORE_SUBMIT: ClassVar[bool] = field(default=False, repr=False)  # Clean up directory when submitting
    LOCK_FILE_NAME: ClassVar[str] = field(default=".sirocco.lock", repr=False)

    name: str = "SIROCCO"
    computer: str = "dummy"
    rank: int = 0
    config_rootdir: Path

    def __post_init__(self) -> None:
        self.run_dir = self.config_rootdir

    def resolve_output_data_paths(self) -> None:
        pass

    def prepare_for_submission(self) -> None:
        pass

    @classmethod
    def from_config_sirocco_task(
        cls: type[Self],
        config_sirocco_task: models.ConfigSiroccoTask | None,
        config_rootdir: Path,
        action: Literal["start", "continue", "stop", "restart"],
    ) -> SiroccoTask:
        config_kwargs = dict(config_sirocco_task) if config_sirocco_task else {}
        if config_kwargs:
            del config_kwargs["parameters"]
        match action:
            case "start":
                return SiroccoStartTask(
                    coordinates={},
                    cycle_point=OneOffPoint(),
                    config_rootdir=config_rootdir,
                    parents=[],
                    **config_kwargs,
                )
            case "continue":
                return SiroccoContinueTask(
                    coordinates={},
                    cycle_point=OneOffPoint(),
                    config_rootdir=config_rootdir,
                    parents=[],
                    **config_kwargs,
                )
            case _:
                msg = f"unsupported action {action}"
                raise ValueError(msg)


@dataclass(kw_only=True)
class SiroccoContinueTask(SiroccoTask):
    """Special Sirocco Task for continuing the workflow"""

    plugin: ClassVar[str] = "sirocco_continue"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_continue.sh", repr=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.label = "Sirocco_continue"

    def runscript_lines(self) -> list[str]:
        lines: list[str] = []
        if self.venv is not None:
            lines.append(f"source {self.venv}/bin/activate")
        cmd: list[str] = ["sirocco"]
        if os.environ.get("NO_COLOR") == "1":
            cmd.append("--no-ansi")
        cmd.append(f"continue --from_wf {self.config_rootdir} || exit")
        lines.append(" ".join(cmd))
        return lines


@dataclass(kw_only=True)
class SiroccoStartTask(SiroccoTask):
    """Special Sirocco Task for continuing the workflow"""

    plugin: ClassVar[str] = "sirocco_start"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_start.sh", repr=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.label = "Sirocco_start"

    def runscript_lines(self) -> list[str]:
        lines: list[str] = []
        if self.venv is not None:
            lines.append(f"source {self.venv}/bin/activate")
        cmd: list[str] = ["sirocco"]
        if os.environ.get("NO_COLOR") == "1":
            cmd.append("--no-ansi")
        cmd.append(f"continue start {self.config_rootdir} || exit")
        lines.append(" ".join(cmd))
        return lines
