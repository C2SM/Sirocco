"""The special tasks running sirocco commands as jobs (`sirocco start`, `sirocco continue`).

They only exist for the standalone engine, so they live here and not in core.
They still subclass `core.Task` so that the scheduler can submit them like any
other task.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Self

from pydantic import TypeAdapter
from ruamel.yaml import YAML

from sirocco.core.graph_items import Task
from sirocco.engines.standalone.plugin_handlers.plugin_handler import PluginHandler
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import OneOffPoint

if TYPE_CHECKING:
    from pathlib import Path


# NOTE: SiroccoTask is a subclass of `GraphItem` for implementation simplicity
# reasons (submission) even though it's not allowed to be part of the graph
@dataclass(kw_only=True)
class SiroccoTask(models.ConfigSiroccoTaskSpecs, Task):
    """Special task running the sirocco command `SIROCCO_CMD` on the workflow in `config_rootdir`"""

    plugin: ClassVar[str] = "sirocco_base"
    SUBMIT_FILENAME: ClassVar[str] = field(default=".sirocco_undefined.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="sirocco.log", repr=False)
    LOCK_FILE_NAME: ClassVar[str] = field(default=".sirocco.lock", repr=False)
    SIROCCO_CMD: ClassVar[str] = field(default="__none__", repr=False)

    name: str = "SIROCCO"
    computer: str = "dummy"
    config_rootdir: Path
    sirocco_cmd_opts: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        self.label = self.plugin.capitalize()

    @classmethod
    def from_config_sirocco_task(
        cls,
        config_sirocco_task: models.ConfigSiroccoTask | None,
        config_rootdir: Path,
        **kwargs: Any,
    ) -> Self:
        config_kwargs = dict(config_sirocco_task) if config_sirocco_task else {}
        config_kwargs.pop("parameters", None)
        return cls(
            coordinates={},
            cycle_point=OneOffPoint(),
            config_rootdir=config_rootdir,
            parents=[],
            **config_kwargs,
            **kwargs,
        )

    @classmethod
    def from_config_file(
        cls,
        config_file_path: Path,
        config_rootdir: Path,
        template_file_path: Path | None = None,
        **kwargs: Any,
    ) -> Self:
        """Build the task from the ROOT and SIROCCO task specs of a config file, without building the workflow

        `config_rootdir` is where the task runs, which can differ from the directory of `config_file_path`,
        e.g. for a config pushed to a remote machine.
        """
        yaml_handler = YAML(typ="safe", pure=True)

        # apply templating
        _, content = models.ConfigWorkflow.apply_template(
            content=config_file_path.read_text(),
            context=yaml_handler.load(template_file_path.read_text()) if template_file_path is not None else {},
        )

        # Extract relevant config parts and build config sirocco task specs
        config_dict: dict[str, Any] = yaml_handler.load(content)
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


class SiroccoTaskHandler[T: SiroccoTask](PluginHandler[T]):
    """Common handler of the Sirocco tasks, registered by its subclasses for each of them"""

    stdouterr_filename = SiroccoTask.STDOUTERR_FILENAME
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
        cmd.append(self.task.SIROCCO_CMD)
        cmd.extend(self.task.sirocco_cmd_opts)
        cmd.append(f"{self.task.config_rootdir} || exit")
        lines.append(" ".join(cmd))
        return lines


class SiroccoContinueTaskHandler(SiroccoTaskHandler[SiroccoContinueTask]):
    submit_filename = SiroccoContinueTask.SUBMIT_FILENAME


class SiroccoStartTaskHandler(SiroccoTaskHandler[SiroccoStartTask]):
    submit_filename = SiroccoStartTask.SUBMIT_FILENAME
