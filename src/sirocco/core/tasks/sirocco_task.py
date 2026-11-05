from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Self

from pydantic import TypeAdapter
from ruamel.yaml import YAML

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import OneOffPoint

if TYPE_CHECKING:
    from pathlib import Path


# NOTE: SiroccoTask is a subclass of `GraphItem` for implementation simplicity
# reasons (submission) even though it's not allowed to be part of the graph
@dataclass(kw_only=True)
class SiroccoTask(models.ConfigSiroccoTaskSpecs, Task):
    plugin: ClassVar[str] = "sirocco_base"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_undefined.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="sirocco.log", repr=False)
    CLEAN_UP_BEFORE_SUBMIT: ClassVar[bool] = field(default=False, repr=False)  # Clean up directory when submitting
    LOCK_FILE_NAME: ClassVar[str] = field(default=".sirocco.lock", repr=False)
    SIROCCO_CMD: ClassVar[str] = field(default="__none__", repr=False)

    name: str = "SIROCCO"
    computer: str = "dummy"
    rank: int = 0
    sirocco_cmd_opts: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        self.run_dir = self.config_rootdir
        self.label = self.plugin

    def resolve_output_data_paths(self) -> None:
        pass

    def prepare_for_submission(self) -> None:
        pass

    def runscript_lines(self) -> list[str]:
        lines: list[str] = []
        if self.venv is not None:
            lines.append(f"source {self.venv}/bin/activate")
        cmd: list[str] = ["sirocco"]
        if os.environ.get("NO_COLOR") == "1":
            cmd.append("--no-ansi")
        cmd.append(f"{self.SIROCCO_CMD}")
        cmd.extend(self.sirocco_cmd_opts)
        cmd.append(f"{self.config_rootdir} || exit")
        lines.append(" ".join(cmd))
        return lines

    @classmethod
    def from_config_sirocco_task(
        cls: type[Self],
        config_sirocco_task: models.ConfigSiroccoTask | None,
        config_rootdir: Path,
        action: str,
        base_env: dict[str, str],
        **kwargs,
    ) -> SiroccoTask:
        if (action_cls := Task.plugin_classes.get(f"sirocco_{action}")) is None:
            msg = f"Sirocco action '{action}' not supported."
            raise ValueError(msg)
        if not issubclass(action_cls, SiroccoTask):
            msg = f"factory must return an instance of s SiroccoTask subclass, got {action_cls}"
            raise TypeError(msg)

        config_kwargs = dict(config_sirocco_task) if config_sirocco_task else {}
        config_kwargs.pop("parameters", None)

        return action_cls(
            coordinates={},
            cycle_point=OneOffPoint(),
            config_rootdir=config_rootdir,
            parents=[],
            base_env=base_env,
            **config_kwargs,
            **kwargs,
        )

    @classmethod
    def from_config_file(
        cls,
        action: str,
        config_file_path: Path,
        config_rootdir: Path,
        template_context: dict[str, Any] | None = None,
        **kwargs,
    ) -> SiroccoTask:
        # apply templating
        _, content = models.ConfigWorkflow.apply_template(
            content=config_file_path.read_text(), context=template_context if template_context else {}
        )

        # Extract relevant config parts and build config sirocco task specs
        config_dict: dict[str, Any] = YAML(typ="safe", pure=True).load(content)
        config_tasks_list: list[dict[str, dict[str, Any]]] = config_dict["tasks"]
        config_root_task_spec: dict[str, Any] = {}
        config_sirocco_task_spec: dict[str, Any] = {}
        for config_task in config_tasks_list:
            name, spec = next(iter(config_task.items()))
            match name:
                case "ROOT":
                    config_root_task_spec = spec
                case "SIROCCO":
                    config_sirocco_task_spec = spec
        config_sirocco_task_dict: dict[str, Any] = {"name": "SIROCCO"}
        config_sirocco_task_dict.update(config_root_task_spec)
        config_sirocco_task_dict.update(config_sirocco_task_spec)

        # Build sirocco task
        return cls.from_config_sirocco_task(
            config_sirocco_task=TypeAdapter(models.ConfigSiroccoTask).validate_python(config_sirocco_task_dict),
            config_rootdir=config_rootdir,
            action=action,
            base_env={},
            **kwargs,
        )


@dataclass(kw_only=True)
class SiroccoContinueTask(SiroccoTask):
    plugin: ClassVar[str] = "sirocco_continue"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_continue.sh", repr=False)
    SIROCCO_CMD: ClassVar[str] = field(default="continue --from_wf", repr=False)


@dataclass(kw_only=True)
class SiroccoStartTask(SiroccoTask):
    plugin: ClassVar[str] = "sirocco_start"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_start.sh", repr=False)
    SIROCCO_CMD: ClassVar[str] = field(default="start", repr=False)
