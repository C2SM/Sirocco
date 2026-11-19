"""Task handlers: what the standalone engine does with each kind of task.

A `PluginHandler` subclass implements the plugin specific parts (abstract methods) and
may override the generic ones (normal methods and class variables). Each subclass sets
the task type it handles with `task_type`, which registers it at class creation. The
built-in handlers are the other modules of this package, imported by
`sirocco.engines.standalone`.

Handlers are created per task, on first access to `StandaloneEngine.task_handlers[task]`. The
engine also holds what all of them share: the run root and the resolved data paths
(`StandaloneEngine.data_paths[data]`).
"""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

from sirocco.parsing import cycling

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core.graph_items import Task
    from sirocco.engines.standalone.engine import StandaloneEngine


class PluginHandler(ABC):
    """Standalone engine behaviour for one task"""

    handler_classes: ClassVar[dict[type[Any], type[PluginHandler]]] = {}
    task_type: ClassVar[type[Task] | None] = None  # Set by subclasses handling a task type

    # Submission settings
    submit_filename: ClassVar[str] = "run_script.sh"
    stdouterr_filename: ClassVar[str] = "run_script.%j.o"
    clean_up_rundir: ClassVar[bool] = True  # Wipe the run directory before preparing it
    scheduler_uenv: ClassVar[bool] = True  # Let the scheduler start the job in the task uenv and view

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if (task_type := cls.task_type) is None:
            return  # Intermediate or test class, handles no task type of its own
        if task_type in PluginHandler.handler_classes:
            msg = f"standalone engine: handler for {task_type.__name__} already registered"
            raise ValueError(msg)
        PluginHandler.handler_classes[task_type] = cls

    @staticmethod
    def handler_class(task: Task) -> type[PluginHandler]:
        """Handler class registered for the type of `task`"""
        if (handler_class := PluginHandler.handler_classes.get(type(task))) is None:
            msg = f"standalone engine: no handler for plugin {task.plugin!r}"
            raise NotImplementedError(msg)
        return handler_class

    def __init__(self, task: Task, engine: StandaloneEngine) -> None:
        self.task = task
        self.engine = engine

    # =========== Plugin specific ===========

    @abstractmethod
    def resolve_output_paths(self) -> None:
        """Set the paths of all the task outputs in `engine.data_paths`"""

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
                link_list.extend([f"ln -s {self.engine.data_paths[data]} ." for data in component.inputs["link"]])
            if "link_content" in component.inputs:
                link_list.extend(
                    [
                        f"for item in {self.engine.data_paths[data]}/*; do ln -s ${{item}} .; done"
                        for data in component.inputs["link_content"]
                    ]
                )
        return link_list
