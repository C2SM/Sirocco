"""Task handlers: what the standalone engine does with each kind of task.

A `PluginHandler` subclass implements the plugin specific parts (abstract methods) and
may override the generic ones (normal methods and class variables). A subclass
parametrizes `PluginHandler` with the task class it handles, e.g.
`ShellHandler(PluginHandler[ShellTask])`, which types its `task` attribute and registers it
at class creation for the plugin of that task class. The
built-in handlers are the other modules of this package, imported by
`sirocco.engines.standalone`.

Handlers are created per task, on first access to `StandaloneEngine.task_handlers[task]`. The
engine also holds what all of them share: the run root and the resolved data paths
(`StandaloneEngine.data_paths[data]`).
"""

from __future__ import annotations

import shutil
import types
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, get_args, get_origin

from sirocco import core
from sirocco.parsing import cycling

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.engines.standalone.engine import StandaloneEngine


@dataclass(kw_only=True)
class PluginHandler[T: core.Task](ABC):
    """Standalone engine behaviour for one task of type `T`"""

    handler_classes: ClassVar[dict[str, type[PluginHandler[Any]]]] = {}

    # Submission settings
    submit_filename: ClassVar[str] = "run_script.sh"
    stdouterr_filename: ClassVar[str] = "run_script.%j.o"
    clean_up_rundir: ClassVar[bool] = True  # Wipe the run directory before preparing it
    scheduler_uenv: ClassVar[bool] = True  # Let the scheduler start the job in the task uenv and view

    task: T
    engine: StandaloneEngine

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Register handler in `handler_classes`"""

        super().__init_subclass__(**kwargs)
        # Introspection to access the base class
        task_class = next(
            (get_args(base)[0] for base in types.get_original_bases(cls) if get_origin(base) is PluginHandler),
            None,
        )
        if task_class is None or isinstance(task_class, TypeVar):
            return  # Task class not given here (inherited from a parent handler, or left generic): nothing to register
        if not (isinstance(task_class, type) and issubclass(task_class, core.Task)):
            msg = f"{cls.__name__}: `PluginHandler` must be parametrized with a `Task` subclass, got {task_class!r}"
            raise TypeError(msg)
        plugin = task_class.plugin
        if plugin in PluginHandler.handler_classes:
            msg = f"standalone engine: handler for plugin {plugin!r} already registered"
            raise ValueError(msg)
        PluginHandler.handler_classes[plugin] = cls

    @staticmethod
    def from_core_task(task: core.Task, engine: StandaloneEngine) -> PluginHandler[Any]:
        """Handler of `task`, of the class registered for its plugin"""
        if (handler_class := PluginHandler.handler_classes.get(task.plugin)) is None:
            msg = f"standalone engine: no handler for plugin {task.plugin!r}"
            raise NotImplementedError(msg)
        return handler_class(task=task, engine=engine)

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
