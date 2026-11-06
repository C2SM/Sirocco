from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Self

from ruamel.yaml import YAML

from sirocco.core.graph_items import GeneratedData, Task
from sirocco.parsing import yaml_data_models as models

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core.graph_items import Data
    from sirocco.parsing.cycling import CyclePoint


@dataclass(kw_only=True)
class ShellTask(models.ConfigShellTaskSpecs, Task):
    CONTEXT_FILENAME: ClassVar[str] = field(default="sirocco_context.yaml", repr=False)

    @classmethod
    def build_from_config(
        cls: type[Self], config: models.ConfigTask, config_rootdir: Path, cycle_point: CyclePoint, **kwargs: Any
    ) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        del config_kwargs["src"]
        # The following check is here for type checkers.
        # We don't want to narrow the type in the signature, as that would break liskov substitution.
        # We guarantee elsewhere this is called with the correct type at runtime
        if not isinstance(config, models.ConfigShellTask):
            raise TypeError

        self = cls(
            config_rootdir=config_rootdir,
            cycle_point=cycle_point,
            **kwargs,
            **config_kwargs,
        )
        if config.src is not None:
            self.src = self._validate_path(config.src, config_rootdir)
        return self

    @property
    def inputs(self) -> dict[str, list[Data]]:
        if (len(self.components) > 1) or (
            next(iter(self.components.keys())) != models.ConfigCycleTask.__SINGLE_COMPONENT_NAME__
        ):
            msg = "Only single component taks can unambiguously define inputs"
            raise ValueError(msg)
        return next(iter(self.components.values())).inputs

    @property
    def outputs(self) -> dict[str, list[GeneratedData]]:
        if (len(self.components) > 1) or (
            next(iter(self.components.keys())) != models.ConfigCycleTask.__SINGLE_COMPONENT_NAME__
        ):
            msg = "Only single component taks can unambiguously define outputs"
            raise ValueError(msg)
        return next(iter(self.components.values())).outputs

    @staticmethod
    def _validate_path(path: Path, config_rootdir: Path) -> Path:
        path = config_rootdir / path
        if not path.exists():
            msg = f"Script in path {path} does not exist."
            raise FileNotFoundError(msg)
        return path

    def runscript_lines(self) -> list[str]:
        return [
            self.resolve_ports(
                {
                    port: [str(data.resolved_path) for data in input_data]
                    for port, input_data in (self.outputs | self.inputs).items()
                }
            )
        ]

    @staticmethod
    def _serialize_coordinates(coordinates: dict[str, Any]) -> dict[str, Any]:
        return {key: value.isoformat() if isinstance(value, datetime) else value for key, value in coordinates.items()}

    def build_context_file(self) -> None:
        """Write the context file that describes the shell task's resolved inputs, outputs and dependencies"""

        context_dict = {
            "task": {
                "name": self.name,
                "coordinates": self._serialize_coordinates(self.coordinates),
                "run_dir": str(self.run_dir),
            },
            "inputs": {
                port: [
                    {
                        "name": data.name,
                        "path": str(data.resolved_path),
                        "coordinates": self._serialize_coordinates(data.coordinates),
                    }
                    for data in input_data
                ]
                for port, input_data in self.inputs.items()
            },
            "outputs": {
                port: [{"name": data.name, "path": str(data.resolved_path)} for data in output_data]
                for port, output_data in self.outputs.items()
            },
            "wait_on": [
                {
                    "name": task.name,
                    "coordinates": self._serialize_coordinates(task.coordinates),
                    "run_dir": str(task.run_dir),
                }
                for task in self.wait_on
            ],
        }

        yaml = YAML(typ="safe")
        yaml.default_flow_style = False
        with (self.run_dir / self.CONTEXT_FILENAME).open("w", encoding="utf-8") as context_file:
            yaml.dump(context_dict, context_file)

    def prepare_for_submission(self) -> None:
        if self.src is not None:
            if self.src.is_dir():
                shutil.copytree(self.src, self.run_dir / self.src.name)
            else:
                shutil.copy(self.src, self.run_dir / self.src.name)
        self.build_context_file()

    def resolve_output_data_paths(self) -> None:
        for data in self.output_data_nodes():
            if data.path is None:
                msg = "shell task output data must specify a path"
                raise ValueError(msg)
            if isinstance(data, GeneratedData):
                if data.path.is_absolute():
                    data.resolved_path = data.path
                else:
                    data.resolved_path = self.run_dir / data.path
