"""Tables of the standalone engine keyed by graph items, filled on demand

- `TaskHandlerTable`: the handler of each task (see `plugin_handlers/`),
- `DataPathTable`: the resolved path of each data node.

The two depend on each other: handlers write and read resolved paths, and resolving the
path of a generated data node asks the handler of its origin task. Both are created and
owned by the engine: the handler table gets the data path table at creation and hands it to
each handler it creates, and the engine then sets `DataPathTable.task_handlers`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sirocco.core.graph_items import AvailableData, GeneratedData
from sirocco.engines.standalone.plugin_handlers import PluginHandler

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco import core


@dataclass(kw_only=True)
class TaskHandlerTable:
    """Handler of each task, keyed by the task label and created on first access"""

    run_root: Path
    data_paths: DataPathTable = field(repr=False)
    _task_handlers: dict[str, PluginHandler] = field(default_factory=dict, init=False)

    def __getitem__(self, task: core.Task) -> PluginHandler:
        if (task_handler := self._task_handlers.get(task.label)) is None:
            task_handler = self._task_handlers[task.label] = PluginHandler.from_core_task(
                task, run_root=self.run_root, data_paths=self.data_paths
            )
        return task_handler


@dataclass(kw_only=True)
class DataPathTable:
    """Resolved path of each data node, keyed by the data label

    Paths follow from the config only, so they are recomputed by every sirocco command
    and never stored in the state file.

    Generated data paths are resolved on demand: asking for the path of an output that is
    not known yet asks the handler of its origin task to resolve all of its outputs (see
    `PluginHandler.resolve_output_paths`), so that callers never need to care about the
    order in which tasks are processed.
    """

    # Set by the engine once the handler table, which needs this one at creation, exists
    task_handlers: TaskHandlerTable = field(init=False, repr=False)
    _paths: dict[str, Path] = field(default_factory=dict, init=False)

    def __getitem__(self, data: core.Data) -> Path:
        if data.label not in self._paths:
            self._resolve(data)
            if data.label not in self._paths:
                msg = f"path of data {data.label} could not be resolved"
                raise ValueError(msg)
        return self._paths[data.label]

    def __setitem__(self, data: core.Data, path: Path) -> None:
        """Set the resolved path of a data node

        Setting it again is allowed, e.g. ICON output ports are applied both when resolving
        output paths and when preparing the task, but only to the same path: a conflicting
        resolution would otherwise silently override the path already used elsewhere.
        """
        if not path.is_absolute():
            msg = "resolved path must be absolute"
            raise ValueError(msg)
        if (current := self._paths.get(data.label)) is not None and current != path:
            msg = f"data {data.label}: resolved to {path} but already resolved to {current}"
            raise ValueError(msg)
        self._paths[data.label] = path

    def _resolve(self, data: core.Data) -> None:
        match data:
            case AvailableData():
                self[data] = data.path
            case GeneratedData() if data.path is not None and data.path.is_absolute():
                self[data] = data.path
            case GeneratedData():
                self.task_handlers[data.origin_task].resolve_output_paths()
            case _:
                msg = f"standalone engine: cannot resolve the path of {type(data).__name__}"
                raise NotImplementedError(msg)
