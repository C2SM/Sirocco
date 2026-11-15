"""Standalone engine support for `core.IconTask`"""

from __future__ import annotations

import shlex
import shutil
from itertools import chain
from pathlib import Path

from sirocco.core.tasks.icon_task import IconTask
from sirocco.core.tasks.icon_task.ports import PortHandler
from sirocco.engines.standalone.handlers import prepare_for_submission, runscript_lines
from sirocco.engines.standalone.scheduler import UENV_MACHINES

RUNTIME_ROOT = Path(__file__).parents[3] / "core" / "tasks" / "icon_task" / "runtime"


@prepare_for_submission.register
def _(task: IconTask) -> None:
    # Check supported machine
    if task.computer not in task.SUPPORTED_MACHINES:
        msg = f"{task.label}: machine {task.computer} not suportted for icon task. Supported machines are {task.SUPPORTED_MACHINES}"
        raise ValueError(msg)

    # Handle uenv and squashed images
    # - reset task.uenv and task.view to None so that they doesn't get taken into account by the scheduler
    # - store them in task.icon_uenv and task.icon_view to expose them with `export ICON_UENV=XYZ` in the run script
    # - append squashed images paths and mount points to icon_uenv
    if task.computer in UENV_MACHINES:
        if task.uenv is None:
            msg = f"{task.label}: uenv is required for ICON tasks on {task.computer}"
            raise ValueError(msg)
        uenv_list = [task.uenv]
        uenv_list.extend((f"{squash}:{mount}" for squash, mount in task.get_squash_paths()))
        task.icon_uenv = ",".join(uenv_list)
        task.icon_view = task.view
    task.uenv = None
    task.view = None

    # Link ICON binaries
    if task.exe.cpu is not None:
        (task.run_dir / "icon_cpu").symlink_to(task.exe.cpu.path)
    if task.exe.gpu is not None:
        (task.run_dir / "icon_gpu").symlink_to(task.exe.gpu.path)

    # Take input/output ports specifications into account:
    # - adapt namelist parameters
    # - resolve data path
    # - link data
    for model in task.models.values():
        for port in chain(model.inputs, model.outputs):
            PortHandler.handle(port, model)

    # Dump ICON config files
    task.dump_namelists(directory=task.run_dir, filename_mode="raw")
    if task.is_coupled() and task.yac_coupling is not None:
        shutil.copy(
            task.config_rootdir / task.yac_coupling,
            task.run_dir / task.yac_coupling.name,
        )

    # Copy required runtime files
    if task.runtime is None:
        runtime_dir = RUNTIME_ROOT / task.computer
        if not runtime_dir.exists():
            msg = f"{task.label}: runtime not defined for computer {task.computer}"
            raise ValueError(msg)
    else:
        runtime_dir = task.config_rootdir / task.runtime
        if not runtime_dir.is_dir():
            msg = f"{task.label}: 'runtime' directory not found at {runtime_dir}"
            raise ValueError(msg)
        if not (runtime_dir / task.MAIN).is_file():
            msg = f"{task.label}: {task.MAIN} not found in runtime at {runtime_dir}"
            raise ValueError(msg)
    shutil.copytree(runtime_dir, task.run_dir, dirs_exist_ok=True)

    # Hostfile and multi-prog config file
    task.distribute_ranks()
    if task.target == "hybrid":
        task.generate_multi_prog_config()


@runscript_lines.register
def _(task: IconTask) -> list[str]:
    lines: list[str] = []
    if task.icon_uenv is not None:
        lines.append(f"export ICON_UENV={shlex.quote(task.icon_uenv)}")
        if task.icon_view is not None:
            lines.append(f"export ICON_VIEW={task.icon_view}")
    lines.append(f"export N_PROCS={task.n_procs}")
    lines.append(f"export SIROCCO_TARGET={task.target}")
    if task.exe.gpu:
        if task.exe.gpu.icon4py_venv:
            lines.append(f"export ICON4PY_VENV={task.exe.gpu.icon4py_venv}")
        if task.exe.gpu.gt4py_build_cache_dir:
            lines.append(f"export GT4PY_BUILD_CACHE_DIR={task.exe.gpu.gt4py_build_cache_dir}")
    lines.append(f"source ./{task.MAIN}")
    return lines
