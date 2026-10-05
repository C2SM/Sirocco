from __future__ import annotations

import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from sirocco.core.graph_items import GeneratedData, Task
from sirocco.parsing import yaml_data_models as models

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core.graph_items import Data


@dataclass(kw_only=True)
class ShellTask(models.ConfigShellTaskSpecs, Task):
    @classmethod
    def build_from_config(cls: type[Self], config: models.ConfigTask, config_rootdir: Path, **kwargs: Any) -> Self:
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
            **kwargs,
            **config_kwargs,
        )
        return self

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.src is None:
            return
        self.src = self.config_rootdir / self.src
        if not self.src.exists():
            msg = f"{self.label}: src not found at {self.src}, must be a path relative to the config dir."
            raise FileNotFoundError(msg)

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

    def runscript_lines(self) -> list[str]:
        return [
            self.resolve_ports(
                {
                    port: [str(data.resolved_path) for data in input_data]
                    for port, input_data in (self.outputs | self.inputs).items()
                }
            )
        ]

    def prepare_for_submission(self) -> None:
        if self.src is not None:
            if self.src.is_dir():
                shutil.copytree(self.src, self.run_dir / self.src.name)
            else:
                shutil.copy(self.src, self.run_dir / self.src.name)

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
