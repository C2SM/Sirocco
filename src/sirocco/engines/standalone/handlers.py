"""Per-plugin behaviour of the standalone engine.

Each function decorated with `@singledispatch` below is a generic function: its
implementation is picked from the class of its first argument, walking up the MRO like a
method lookup, the decorated function being the fallback. Plugin modules add theirs with:

```python
@prepare_for_submission.register(ShellTask)
def prepare_for_submission_shell(
    task: ShellTask, path_resolver: PathResolver
) -> None: ...
```

- Implementations are named `<generic function>_<task type>`, as in `pretty_print.py`, to
  tell them apart in tracebacks.
- The class is passed explicitly to `register`. Reading it from the type annotation would
  need the class at runtime, which linters cannot see: they would move its import under
  `TYPE_CHECKING`.
- Registration happens when the registering module is imported, which
  `sirocco.engines.standalone` does for all built-in plugins (see `tasks/`).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatch
from typing import TYPE_CHECKING

from sirocco.core.graph_items import AvailableData, GeneratedData
from sirocco.parsing import cycling

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core.graph_items import Data, Task
    from sirocco.engines.standalone.path_resolver import PathResolver


@dataclass(frozen=True)
class SubmissionSettings:
    """How a task is submitted: run script file names and run directory handling"""

    submit_filename: str = "run_script.sh"
    stdouterr_filename: str = "run_script.%j.o"
    clean_up_run_dir: bool = True  # Wipe the run directory before submitting
    scheduler_uenv: bool = True  # Let the scheduler start the job in the task uenv and view


@singledispatch
def submission_settings(task: Task) -> SubmissionSettings:  # noqa: ARG001  # default for all tasks
    return SubmissionSettings()


@singledispatch
def run_dir(task: Task, path_resolver: PathResolver) -> Path:
    """Directory in which the task runs"""
    return (path_resolver.run_root / task.label).resolve()


@singledispatch
def resolve_output_paths(task: Task, path_resolver: PathResolver) -> None:
    """Set the resolved paths of all the task outputs with `path_resolver.set_path`"""
    msg = f"standalone engine: plugin {task.plugin!r} has no resolve_output_paths implementation"
    raise NotImplementedError(msg)


@singledispatch
def prepare_for_submission(task: Task, path_resolver: PathResolver) -> None:
    """Generate or copy any file required at runtime to the task run directory"""
    msg = f"standalone engine: plugin {task.plugin!r} has no prepare_for_submission implementation"
    raise NotImplementedError(msg)


@singledispatch
def runscript_lines(task: Task, path_resolver: PathResolver) -> list[str]:
    """Plugin specific lines of the run script"""
    msg = f"standalone engine: plugin {task.plugin!r} has no runscript_lines implementation"
    raise NotImplementedError(msg)


# =========== Data paths ===========


@singledispatch
def resolve_data_path(data: Data, path_resolver: PathResolver) -> None:
    """Set the resolved path of a data node with `path_resolver.set_path`"""
    msg = f"standalone engine: no resolve_data_path implementation for {type(data).__name__}"
    raise NotImplementedError(msg)


@resolve_data_path.register(AvailableData)
def resolve_data_path_available(data: AvailableData, path_resolver: PathResolver) -> None:
    path_resolver.set_path(data, data.path)


@resolve_data_path.register(GeneratedData)
def resolve_data_path_generated(data: GeneratedData, path_resolver: PathResolver) -> None:
    if data.path is not None and data.path.is_absolute():
        path_resolver.set_path(data, data.path)
    else:
        resolve_output_paths(data.origin_task, path_resolver)


# =========== Plugin independent parts of the run script ===========


def sirocco_environment_lines(task: Task) -> list[str]:
    # TODO: Add parameters
    env_list: list[str] = []
    if isinstance(task.cycle_point, cycling.DateCyclePoint):
        env_list.append(f"export SIROCCO_START_DATE={task.cycle_point.chunk_start_date.isoformat()}")
        env_list.append(f"export SIROCCO_STOP_DATE={task.cycle_point.chunk_stop_date.isoformat()}")
    return env_list


def link_lines(task: Task, path_resolver: PathResolver) -> list[str]:
    link_list: list[str] = []
    for component in task.components.values():
        if "link" in component.inputs:
            link_list.extend([f"ln -s {path_resolver.path(data)} ." for data in component.inputs["link"]])
        if "link_content" in component.inputs:
            link_list.extend(
                [
                    f"for item in {path_resolver.path(data)}/*; do ln -s ${{item}} .; done"
                    for data in component.inputs["link_content"]
                ]
            )
    return link_list
