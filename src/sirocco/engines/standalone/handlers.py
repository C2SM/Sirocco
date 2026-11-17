"""Task handlers: what the standalone engine does with each kind of task.

A `PluginHandler` subclass implements the plugin specific parts (abstract methods) and
may override the generic ones (normal methods and class variables). Each subclass sets
the task type it handles with `task_type`, which registers it at class creation. The
built-in handlers are in `tasks/`, imported by `sirocco.engines.standalone`.

Handlers are created per task, on first access to `StandaloneEngine.handlers[task]`. The
engine also holds what all of them share: the run root and the resolved data paths. Paths
follow from the config only, so they are recomputed by every sirocco command and never
stored in the state file.

Generated data paths are resolved on demand: asking for the path of an output that is
not known yet asks the handler of its origin task to resolve all of its outputs (see
`PluginHandler.resolve_output_paths`), so that callers never need to care about the
order in which tasks are processed.
"""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

from sirocco.core.graph_items import AvailableData, GeneratedData
from sirocco.parsing import cycling

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core.graph_items import Data, Task
    from sirocco.engines.standalone.engine import StandaloneEngine


class PluginHandler[T: Task](ABC):
    """Standalone engine behaviour for one task"""

    handler_classes: ClassVar[dict[type[Any], type[PluginHandler]]] = {}
    task_type: ClassVar[type[Task]]

    # Submission settings
    submit_filename: ClassVar[str] = "run_script.sh"
    stdouterr_filename: ClassVar[str] = "run_script.%j.o"
    clean_up_rundir: ClassVar[bool] = True  # Wipe the run directory before preparing it
    scheduler_uenv: ClassVar[bool] = True  # Let the scheduler start the job in the task uenv and view

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "task_type" not in cls.__dict__:
            return  # Intermediate or test class, handles no task type of its own
        if cls.task_type in PluginHandler.handler_classes:
            msg = f"standalone engine: handler for {cls.task_type.__name__} already registered"
            raise ValueError(msg)
        PluginHandler.handler_classes[cls.task_type] = cls

    @staticmethod
    def handler_class(task: Task) -> type[PluginHandler]:
        """Handler class registered for the type of `task`"""
        if (handler_class := PluginHandler.handler_classes.get(type(task))) is None:
            msg = f"standalone engine: no handler for plugin {task.plugin!r}"
            raise NotImplementedError(msg)
        return handler_class

    def __init__(self, task: T, engine: StandaloneEngine) -> None:
        self.task = task
        self.engine = engine

    # =========== Plugin specific ===========

    @abstractmethod
    def resolve_output_paths(self) -> None:
        """Set the paths of all the task outputs with `set_data_path`"""

    @abstractmethod
    def prepare_rundir_content(self) -> None:
        """Generate or copy any file required at runtime to the run directory"""

    @abstractmethod
    def get_runscript_lines(self) -> list[str]:
        """Plugin specific lines of the run script"""

    # =========== Paths ===========

    def get_rundir(self) -> Path:
        """Directory in which the task runs"""
        return (self.engine.run_root / self.task.label).resolve()

    def get_runscript_path(self) -> Path:
        return self.get_rundir() / self.submit_filename

    def get_data_path(self, data: Data) -> Path:
        """Resolved path of a data node"""
        if (path := self.engine.data_paths.get(data.label)) is None:
            self.resolve_data_path(data)
            if (path := self.engine.data_paths.get(data.label)) is None:
                msg = f"path of data {data.label} could not be resolved"
                raise ValueError(msg)
        return path

    def set_data_path(self, data: Data, path: Path) -> None:
        """Set the resolved path of a data node

        Setting it again is allowed, e.g. ICON output ports are applied both when resolving
        output paths and when preparing the task, but only to the same path: a conflicting
        resolution would otherwise silently override the path already used elsewhere.
        """
        if not path.is_absolute():
            msg = "resolved path must be absolute"
            raise ValueError(msg)
        if (current := self.engine.data_paths.get(data.label)) is not None and current != path:
            msg = f"data {data.label}: resolved to {path} but already resolved to {current}"
            raise ValueError(msg)
        self.engine.data_paths[data.label] = path

    def resolve_data_path(self, data: Data) -> None:
        match data:
            case AvailableData():
                self.set_data_path(data, data.path)
            case GeneratedData() if data.path is not None and data.path.is_absolute():
                self.set_data_path(data, data.path)
            case GeneratedData():
                self.engine.handlers[data.origin_task].resolve_output_paths()
            case _:
                msg = f"standalone engine: cannot resolve the path of {type(data).__name__}"
                raise NotImplementedError(msg)

    # =========== Submission ===========

    def prepare_rundir(self) -> None:
        """Create (or wipe) the run directory and fill it"""
        rundir = self.get_rundir()
        if self.clean_up_rundir:
            shutil.rmtree(rundir, ignore_errors=True)
        rundir.mkdir(parents=True, exist_ok=True)
        self.prepare_rundir_content()

    def get_sirocco_environment_lines(self) -> list[str]:
        # TODO: Add parameters
        env_list: list[str] = []
        if isinstance(self.task.cycle_point, cycling.DateCyclePoint):
            env_list.append(f"export SIROCCO_START_DATE={self.task.cycle_point.chunk_start_date.isoformat()}")
            env_list.append(f"export SIROCCO_STOP_DATE={self.task.cycle_point.chunk_stop_date.isoformat()}")
        return env_list

    def get_link_lines(self) -> list[str]:
        link_list: list[str] = []
        for component in self.task.components.values():
            if "link" in component.inputs:
                link_list.extend([f"ln -s {self.get_data_path(data)} ." for data in component.inputs["link"]])
            if "link_content" in component.inputs:
                link_list.extend(
                    [
                        f"for item in {self.get_data_path(data)}/*; do ln -s ${{item}} .; done"
                        for data in component.inputs["link_content"]
                    ]
                )
        return link_list
