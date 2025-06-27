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
        del config_kwargs["path"]
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
        if config.path is not None:
            self.path = self._validate_path(config.path, config_rootdir)
        return self

    @staticmethod
    def _validate_path(path: Path, config_rootdir: Path) -> Path:
        if path.is_absolute():
            msg = f"Script path {path} must be relative with respect to config file."
        path = config_rootdir / path
        if not path.exists():
            msg = f"Script in path {path} does not exist."
            raise FileNotFoundError(msg)
        if not path.is_file():
            msg = f"Script in path {path} is not a file."
            raise OSError(msg)
        return path
