from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

from pydantic import TypeAdapter
from ruamel.yaml import YAML

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import OneOffPoint

if TYPE_CHECKING:
    from pathlib import Path

type ACTION_T = Literal["start", "continue", "stop", "restart"]


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
        action: ACTION_T,
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

    @classmethod
    def from_config_file(cls, action: ACTION_T, config_file_path: Path, config_rootdir: Path) -> SiroccoTask:
        # Extract relevant config parts and build config sirocco task specs
        config_dict: dict[str, Any] = YAML(typ="safe", pure=True).load(config_file_path)
        config_tasks_list: list[dict[str, Any]] = config_dict["tasks"]
        config_root_task_dict: dict[str, Any] = {}
        config_sirocco_task_dict: dict[str, Any] = {}

        for config_task in config_tasks_list:
            match next(iter(config_task)):
                case "ROOT":
                    config_root_task_dict = config_task
                case "SIROCCO":
                    config_sirocco_task_dict = config_task
                    config_sirocco_task_dict.update(config_root_task_dict)

        # Build sirocco task
        return cls.from_config_sirocco_task(
            config_sirocco_task=TypeAdapter(models.ConfigSiroccoTask).validate_python(config_sirocco_task_dict),
            config_rootdir=config_rootdir,
            action=action,
        )


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
