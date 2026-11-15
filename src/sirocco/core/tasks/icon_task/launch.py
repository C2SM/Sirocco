"""How ICON tasks are launched by the runtime scripts, for all engines.

The runtime scripts (`runtime/<machine>/`, or the task's own `runtime` directory) start
ICON from the job working directory. They expect there:

- the runtime scripts themselves, the entry point being `RUNTIME_MAIN`,
- the ICON executables linked as `ICON_CPU_LINK` and/or `ICON_GPU_LINK`,
- the rank layout in `HOSTFILE`, with placeholder node names (see `hostfile_content`),
- for hybrid runs, the srun multi-prog configuration in `MULTI_PROG_CONFIG`
  (see `multi_prog_content`),

and the environment variables returned by `runtime_environment`. Engines only
materialize this (files, links, environment) in their own way and source `RUNTIME_MAIN`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sirocco.core.tasks.icon_task.icon_task import IconTask

RUNTIME_ROOT = Path(__file__).parent / "runtime"
# Entry point of the runtime scripts, to be sourced by the job
RUNTIME_MAIN = "main.sh"
# Machines with runtime scripts and rank distribution
SUPPORTED_MACHINES = ["santis"]

ICON_CPU_LINK = "icon_cpu"
ICON_GPU_LINK = "icon_gpu"
HOSTFILE = "hostfile-sirocco"
MULTI_PROG_CONFIG = "multi-prog.conf"


def check_machine(task: IconTask) -> None:
    if task.computer not in SUPPORTED_MACHINES:
        msg = f"{task.label}: machine {task.computer} not suportted for icon task. Supported machines are {SUPPORTED_MACHINES}"
        raise ValueError(msg)


def runtime_dir(task: IconTask) -> Path:
    """Directory of the runtime scripts to copy to the job working directory"""
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
        if not (runtime_dir / RUNTIME_MAIN).is_file():
            msg = f"{task.label}: {RUNTIME_MAIN} not found in runtime at {runtime_dir}"
            raise ValueError(msg)
    return runtime_dir


def runtime_environment(task: IconTask, squash_mounts: Iterable[tuple[Path, Path]] = ()) -> dict[str, str]:
    """Environment variables expected by the runtime scripts

    `squash_mounts` are the resolved paths of the squashed images with their mount points,
    both depending on where the engine runs the job.
    """
    env: dict[str, str] = {}
    if task.uenv is not None:
        env["ICON_UENV"] = ",".join([task.uenv, *(f"{image}:{mount}" for image, mount in squash_mounts)])
        if task.view is not None:
            env["ICON_VIEW"] = task.view
    env["N_PROCS"] = str(task.n_procs)
    env["SIROCCO_TARGET"] = task.target
    if task.exe.gpu:
        if task.exe.gpu.icon4py_venv:
            env["ICON4PY_VENV"] = str(task.exe.gpu.icon4py_venv)
        if task.exe.gpu.gt4py_build_cache_dir:
            env["GT4PY_BUILD_CACHE_DIR"] = str(task.exe.gpu.gt4py_build_cache_dir)
    return env


def hostfile_content(task: IconTask) -> str:
    """Rank layout with placeholder node names, completed by the runtime scripts on the allocated nodes"""
    task.distribute_ranks()
    nid_length = len(str(task.nodes - 1))
    return "\n".join(
        "sirocco_nid{node_id:0{nid_length}} {numa_node} {pe_type} {target} {model}".format(
            **task.ranks_info[k], nid_length=nid_length
        )
        for k in range(task.n_procs)
    )


def multi_prog_content(task: IconTask) -> str:
    """srun multi-prog configuration for hybrid runs"""
    match task.computer:
        case "santis":
            lines: list[str] = []
            if task.exe.gpu:
                lines.append(f"{task.get_exe_ranks_str(task.exe.gpu)} ./santis_icon_wrapper.sh ./{ICON_GPU_LINK}")
            if task.exe.cpu:
                lines.append(f"{task.get_exe_ranks_str(task.exe.cpu)} ./santis_icon_wrapper.sh ./{ICON_CPU_LINK}")
            if task.exe.hiopy:
                lines.append(f"{task.exe.hiopy.min_rank}-{task.exe.hiopy.max_rank} ./santis_hiopy_wrapper.sh")
            return "\n".join(lines)
        case _:
            msg = f"{task.label}: multi-prog configuration not implemented for machine {task.computer}"
            raise NotImplementedError(msg)
