"""Per-plugin behaviour of the standalone engine.

Each function below is a `functools.singledispatch` generic function: a
function whose implementation is picked at call time from the *class of its
first argument*. The base implementation (decorated with `@singledispatch`)
is the fallback, used when no more specific implementation is registered.

Plugin modules register their own implementation with:

```python
@prepare_for_submission.register(ShellTask)
def prepare_for_submission_shell(task: ShellTask) -> None: ...
```

Registered implementations are named `<generic function>_<task type>`, as in
`pretty_print.py`, so that they can be told apart in tracebacks.

The class is passed explicitly to `register` rather than read from the type
annotation of the first parameter: an annotation needs the class at runtime,
which linters cannot see, so they would move its import under `TYPE_CHECKING`.

`register(ShellTask)` stores the function in a `{class: implementation}` table attached to
`prepare_for_submission`. A call `prepare_for_submission(task)` then looks
up `type(task)` in that table, walking up the MRO, so a subclass of
`ShellTask` would reuse the `ShellTask` implementation unless it registers
its own. This is the same lookup as for methods, but the table lives in the
engine instead of in the core classes.

Registration happens when the module containing the `register` call is
imported. `sirocco.engines.standalone.tasks` imports all built-in plugin
modules, and `sirocco.engines.standalone` imports that package, so using the
engine at all guarantees every built-in plugin is registered.
"""

from __future__ import annotations

from functools import singledispatch
from typing import TYPE_CHECKING

from sirocco.parsing import cycling

if TYPE_CHECKING:
    from sirocco.core.graph_items import Task


@singledispatch
def prepare_for_submission(task: Task) -> None:
    """Generate or copy any file required at runtime to the task run_dir"""
    msg = f"standalone engine: plugin {task.plugin!r} has no prepare_for_submission implementation"
    raise NotImplementedError(msg)


@singledispatch
def runscript_lines(task: Task) -> list[str]:
    """Plugin specific lines of the run script"""
    msg = f"standalone engine: plugin {task.plugin!r} has no runscript_lines implementation"
    raise NotImplementedError(msg)


# =========== Plugin independent parts of the run script ===========


def sirocco_environment_lines(task: Task) -> list[str]:
    # TODO: Add parameters
    env_list: list[str] = []
    if isinstance(task.cycle_point, cycling.DateCyclePoint):
        env_list.append(f"export SIROCCO_START_DATE={task.cycle_point.chunk_start_date.isoformat()}")
        env_list.append(f"export SIROCCO_STOP_DATE={task.cycle_point.chunk_stop_date.isoformat()}")
    return env_list


def link_lines(task: Task) -> list[str]:
    link_list: list[str] = []
    for component in task.components.values():
        if "link" in component.inputs:
            link_list.extend([f"ln -s {data.resolved_path} ." for data in component.inputs["link"]])
        if "link_content" in component.inputs:
            link_list.extend(
                [
                    f"for item in {data.resolved_path}/*; do ln -s ${{item}} .; done"
                    for data in component.inputs["link_content"]
                ]
            )
    return link_list
