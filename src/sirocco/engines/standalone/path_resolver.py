"""Locations on disk used by the standalone engine: task run directories and data paths.

Where a task runs and where its outputs end up is decided by the engine, not by the
core graph. Paths follow from the config only, so they are recomputed by every sirocco
command and never stored in the state file.

Generated data paths are resolved on demand: asking for the path of an output that is
not known yet asks its origin task to resolve all of its outputs (see
`handlers.resolve_output_paths`), so that callers never need to care about the order
in which tasks are processed.
"""

from __future__ import annotations

from functools import singledispatchmethod
from typing import TYPE_CHECKING

from sirocco.core.graph_items import AvailableData, GeneratedData
from sirocco.engines.standalone import handlers

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core.graph_items import Data, Task


class PathResolver:
    _RUN_ROOT_NAME = "run"

    def __init__(self, config_rootdir: Path) -> None:
        self.config_rootdir = config_rootdir
        self._data_paths: dict[str, Path] = {}

    @property
    def run_root(self) -> Path:
        """Root of all task run directories"""
        return self.config_rootdir / self._RUN_ROOT_NAME

    def run_dir(self, task: Task) -> Path:
        return (self.run_root / task.label).resolve()

    # =========== Data paths ===========


    @singledispatchmethod
    def resolve_data_path(self, data: Data) -> None:
        """Set the resolved path of a data node with `path_resolver.set_path`"""
        msg = f"standalone engine: no resolve_data_path implementation for {type(data).__name__}"
        raise NotImplementedError(msg)


    @resolve_data_path.register(AvailableData)
    def resolve_data_path_available(self, data: AvailableData) -> None:
        self.set_path(data, data.path)


    @resolve_data_path.register(GeneratedData)
    def resolve_data_path_generated(self, data: GeneratedData) -> None:
        if data.path is not None and data.path.is_absolute():
            self.set_path(data, data.path)
        else:
            handlers.resolve_output_paths(data.origin_task, self)

    def get_path(self, data: Data) -> Path:
        """Resolved path of a data node"""
        if (path := self._data_paths.get(data.label)) is None:
            self.resolve_data_path(data)
            if (path := self._data_paths.get(data.label)) is None:
                msg = f"path of data {data.label} could not be resolved"
                raise ValueError(msg)
        return path

    def set_path(self, data: Data, path: Path) -> None:
        """Set the resolved path of a data node

        Setting it again is allowed, e.g. ICON output ports are applied both when resolving
        output paths and when preparing the task, but only to the same path: a conflicting
        resolution would otherwise silently override the path already used elsewhere.
        """
        if not path.is_absolute():
            msg = "resolved path must be absolute"
            raise ValueError(msg)
        if (current := self._data_paths.get(data.label)) is not None and current != path:
            msg = f"data {data.label}: resolved to {path} but already resolved to {current}"
            raise ValueError(msg)
        self._data_paths[data.label] = path
