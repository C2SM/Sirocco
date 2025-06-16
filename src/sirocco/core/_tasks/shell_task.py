from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(kw_only=True)
class ShellTask(models.ConfigShellTaskSpecs, Task):
    @classmethod
    def build_from_config(cls: type[Self], config: models.ConfigTask, config_rootdir: Path, **kwargs: Any) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
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
        if self.src is not None:
            self.src = self._validate_src(self.src, config_rootdir)
        return self

    @staticmethod
    def _validate_src(config_src: Path, config_rootdir: Path) -> Path:
        if config_src.is_absolute():
            msg = f"Namelist path {config_src} must be relative with respect to config file."
        src = config_rootdir / config_src
        if not src.exists():
            msg = f"Script in path {src} does not exist."
            raise FileNotFoundError(msg)
        if not src.is_file():
            msg = f"Script in path {src} is not a file."
            raise OSError(msg)
        return src
