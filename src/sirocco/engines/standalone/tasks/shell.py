"""Standalone engine support for `core.ShellTask`"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

from sirocco.core.tasks.shell_task import ShellTask
from sirocco.engines.standalone.handlers import prepare_for_submission, resolve_output_paths, runscript_lines

if TYPE_CHECKING:
    from sirocco.engines.standalone.path_resolver import PathResolver


@resolve_output_paths.register(ShellTask)
def resolve_output_paths_shell(task: ShellTask, path_resolver: PathResolver) -> None:
    for data in task.output_data_nodes():
        if data.path is None:
            msg = "shell task output data must specify a path"
            raise ValueError(msg)
        path_resolver.set_path(data, data.path if data.path.is_absolute() else path_resolver.run_dir(task) / data.path)


@prepare_for_submission.register(ShellTask)
def prepare_for_submission_shell(task: ShellTask, path_resolver: PathResolver) -> None:
    if task.src is not None:
        if task.src.is_dir():
            shutil.copytree(task.src, path_resolver.run_dir(task) / task.src.name)
        else:
            shutil.copy(task.src, path_resolver.run_dir(task) / task.src.name)


@runscript_lines.register(ShellTask)
def runscript_lines_shell(task: ShellTask, path_resolver: PathResolver) -> list[str]:
    return [
        task.resolve_ports(
            {
                port: [str(path_resolver.path(data)) for data in input_data]
                for port, input_data in (task.outputs | task.inputs).items()
            }
        )
    ]
