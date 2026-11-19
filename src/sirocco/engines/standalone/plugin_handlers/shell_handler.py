"""Standalone engine support for `core.ShellTask`"""

from __future__ import annotations

import shutil

from sirocco.core.tasks.shell_task import ShellTask
from sirocco.engines.standalone.plugin_handlers.plugin_handler import PluginHandler


class ShellHandler(PluginHandler):
    task_type = ShellTask
    task: ShellTask

    def resolve_output_paths(self) -> None:
        for data in self.task.output_data_nodes():
            if data.path is None:
                msg = "shell task output data must specify a path"
                raise ValueError(msg)
            self.engine.data_paths[data] = data.path if data.path.is_absolute() else self.get_rundir() / data.path

    def prepare_rundir_content(self) -> None:
        if (src := self.task.src) is not None:
            if src.is_dir():
                shutil.copytree(src, self.get_rundir() / src.name)
            else:
                shutil.copy(src, self.get_rundir() / src.name)

    def get_runscript_lines(self) -> list[str]:
        return [
            self.task.resolve_ports(
                {
                    port: [str(self.engine.data_paths[data]) for data in input_data]
                    for port, input_data in (self.task.outputs | self.task.inputs).items()
                }
            )
        ]
