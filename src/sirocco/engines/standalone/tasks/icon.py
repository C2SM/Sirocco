"""Standalone engine support for `core.IconTask`"""

from __future__ import annotations

import shlex
import shutil
from itertools import chain
from typing import TYPE_CHECKING

from sirocco.core.tasks.icon_task import IconTask, launch
from sirocco.engines.standalone.handlers import (
    SubmissionSettings,
    prepare_for_submission,
    resolve_output_paths,
    runscript_lines,
    submission_settings,
)
from sirocco.engines.standalone.scheduler import UENV_MACHINES
from sirocco.engines.standalone.tasks.icon_ports import apply_port

if TYPE_CHECKING:
    from sirocco.engines.standalone.path_resolver import PathResolver


@submission_settings.register(IconTask)
def submission_settings_icon(task: IconTask) -> SubmissionSettings:  # noqa: ARG001
    # The uenv is not set by the scheduler but passed to the runtime scripts (ICON_UENV),
    # together with the mount points of the squashed images
    return SubmissionSettings(scheduler_uenv=False)


@resolve_output_paths.register(IconTask)
def resolve_output_paths_icon(task: IconTask, path_resolver: PathResolver) -> None:
    run_dir = path_resolver.run_dir(task)
    for model in task.models.values():
        for port in model.outputs:
            apply_port(port, model, run_dir, path_resolver)


@prepare_for_submission.register(IconTask)
def prepare_for_submission_icon(task: IconTask, path_resolver: PathResolver) -> None:
    run_dir = path_resolver.run_dir(task)

    launch.check_machine(task)

    # Check uenv and create the mount points of the squashed images
    if task.computer in UENV_MACHINES:
        if task.uenv is None:
            msg = f"{task.label}: uenv is required for ICON tasks on {task.computer}"
            raise ValueError(msg)
        for _, mount_point in task.get_squash_mounts(run_dir):
            mount_point.mkdir(parents=True, exist_ok=True)

    # Link ICON binaries
    if task.exe.cpu is not None:
        (run_dir / launch.ICON_CPU_LINK).symlink_to(task.exe.cpu.path)
    if task.exe.gpu is not None:
        (run_dir / launch.ICON_GPU_LINK).symlink_to(task.exe.gpu.path)

    # Take input/output ports specifications into account:
    # - adapt namelist parameters
    # - resolve data path
    # - link data
    for model in task.models.values():
        for port in chain(model.inputs, model.outputs):
            apply_port(port, model, run_dir, path_resolver)

    # Dump ICON config files
    task.dump_namelists(directory=run_dir, filename_mode="raw")
    if task.is_coupled() and task.yac_coupling is not None:
        shutil.copy(
            task.config_rootdir / task.yac_coupling,
            run_dir / task.yac_coupling.name,
        )

    # Runtime scripts and the files they expect
    shutil.copytree(launch.runtime_dir(task), run_dir, dirs_exist_ok=True)
    (run_dir / launch.HOSTFILE).write_text(launch.hostfile_content(task))
    if task.target == "hybrid":
        (run_dir / launch.MULTI_PROG_CONFIG).write_text(launch.multi_prog_content(task))


@runscript_lines.register(IconTask)
def runscript_lines_icon(task: IconTask, path_resolver: PathResolver) -> list[str]:
    run_dir = path_resolver.run_dir(task)
    squash_mounts = [(path_resolver.path(image), mount) for image, mount in task.get_squash_mounts(run_dir)]
    env = launch.runtime_environment(task, squash_mounts)
    return [*(f"export {name}={shlex.quote(value)}" for name, value in env.items()), f"source ./{launch.RUNTIME_MAIN}"]
