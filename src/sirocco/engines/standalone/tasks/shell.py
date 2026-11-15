"""Standalone engine support for `core.ShellTask`"""

from __future__ import annotations

import shutil

from sirocco.core.tasks.shell_task import ShellTask
from sirocco.engines.standalone.handlers import prepare_for_submission, runscript_lines


@prepare_for_submission.register(ShellTask)
def prepare_for_submission_shell(task: ShellTask) -> None:
    if task.src is not None:
        if task.src.is_dir():
            shutil.copytree(task.src, task.run_dir / task.src.name)
        else:
            shutil.copy(task.src, task.run_dir / task.src.name)


@runscript_lines.register(ShellTask)
def runscript_lines_shell(task: ShellTask) -> list[str]:
    return [
        task.resolve_ports(
            {
                port: [str(data.resolved_path) for data in input_data]
                for port, input_data in (task.outputs | task.inputs).items()
            }
        )
    ]
