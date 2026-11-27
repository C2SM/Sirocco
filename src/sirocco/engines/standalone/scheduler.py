"""Job scheduler interface of the standalone engine.

The scheduler is stateless with respect to tasks: it receives job ids from the
engine for parents, jobs to cancel or query and returns the new job id for
submitted tasks. Only the engine reads and writes the state table.

The job scripts of Sirocco's own jobs (`sirocco continue`, later the remote control
commands) export `SIROCCO_JOBID` and `SIROCCO_SCHEDULER`. Only processes started by
these scripts count as running in a job (`Scheduler.current_jobid()`): a command typed
in an interactive allocation inherits the allocation's job id, but other commands may
share it and the allocation may end before the command.
"""

from __future__ import annotations

import os
import subprocess
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, assert_never

from sirocco.engines.standalone.state import TaskStatus

if TYPE_CHECKING:
    from sirocco.engines.standalone.plugin_handlers import PluginHandler

UENV_MACHINES: list[str] = ["santis"]

type STDOUTERR_MODE_T = Literal["overwrite", "append"]
type DEPENDENCY_TYPE_T = Literal["ALL_COMPLETED", "ANY", "NONE"]


JOBID_ENV_VAR = "SIROCCO_JOBID"
SCHEDULER_ENV_VAR = "SIROCCO_SCHEDULER"


class SchedulerCommandError(RuntimeError): ...


class UnknownJobError(SchedulerCommandError):
    """The scheduler doesn't know the job (anymore)"""


class Scheduler(ABC):
    scheduler_classes: ClassVar[dict[str, type[Self]]] = {}
    name: ClassVar[str | None] = None  # Set by subclasses implementing a scheduler, name used in the config
    jobid_variable: ClassVar[str]  # Environment variable holding the job id in a job

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if (name := cls.name) is None:
            return  # Intermediate or test class, registers no scheduler of its own
        if name in Scheduler.scheduler_classes:
            msg = f"Scheduler {name} already set"
            raise ValueError(msg)
        Scheduler.scheduler_classes[name] = cls

    def submit(
        self,
        task_handler: PluginHandler,
        parent_ids: list[str],
        env: dict[str, str],
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
        """Submit a task from environment `env` and return its job id"""
        # Prepare for submission (create or wipe the run directory, link/copy necessary files/dirs, ...)
        task_handler.prepare_rundir()
        self.generate_run_script(task_handler, stdouterr_mode=stdouterr_mode)
        return self.submit_to_scheduler(task_handler, parent_ids=parent_ids, env=env, dependency_type=dependency_type)

    def generate_run_script(self, task_handler: PluginHandler, stdouterr_mode: STDOUTERR_MODE_T) -> None:
        task = task_handler.task

        # Shebang
        script_lines: list[str] = ["#!/bin/bash -l", ""]

        # Scheduler header
        script_lines.extend(self.header_lines(task_handler, stdouterr_mode=stdouterr_mode))

        # Some MPI environment variables for potential usage by the user provided script
        script_lines.append("")
        if task.nodes is not None:
            script_lines.append(f"export N_NODES={task.nodes}")
        if task.procs_per_node is not None:
            script_lines.append(f"export PROCS_PER_NODE={task.procs_per_node}")
        if task.cores_per_proc is not None:
            script_lines.append(f"export CORES_PER_PROC={task.cores_per_proc}")

        # Sirocco context
        script_lines.append("")
        if task_handler.sirocco_job:
            script_lines.extend(self.sirocco_job_lines())
        script_lines.extend(task_handler.get_sirocco_environment_lines())

        # Linked input
        script_lines.extend(task_handler.get_link_lines())

        # Task runscript "content"
        script_lines.append("")
        script_lines.extend(task_handler.get_runscript_lines())

        # Write to runscript file
        script_path = task_handler.get_runscript_path()
        script_path.write_text("\n".join(script_lines))
        script_path.chmod(0o755)

    def sirocco_job_lines(self) -> list[str]:
        """Lines exporting the variables that make a process started by the script a Sirocco job"""
        return [f"export {SCHEDULER_ENV_VAR}={self.name}", f"export {JOBID_ENV_VAR}=${{{self.jobid_variable}}}"]

    @staticmethod
    def current_jobid() -> str | None:
        """Job id of the current process if started by a Sirocco job script, `None` otherwise"""
        return os.environ.get(JOBID_ENV_VAR)

    @staticmethod
    def current_scheduler() -> str | None:
        """Name of the scheduler running the current process if started by a Sirocco job script"""
        return os.environ.get(SCHEDULER_ENV_VAR)

    def cluster(self) -> str | None:
        """Name of the cluster of the scheduler, `None` if the scheduler can't tell"""
        return None

    @abstractmethod
    def header_lines(
        self,
        task_handler: PluginHandler,
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
    ) -> list[str]:
        pass

    @abstractmethod
    def submit_to_scheduler(
        self,
        task_handler: PluginHandler,
        parent_ids: list[str],
        env: dict[str, str],
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
        pass

    @abstractmethod
    def get_status(self, jobid: str) -> TaskStatus:
        """Status of job `jobid`, raises `UnknownJobError` if the scheduler doesn't know it"""

    @abstractmethod
    def cancel(self, jobid: str) -> None:
        pass

    @staticmethod
    def run_command(cmd: list[str], **subprocess_kw) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(cmd, capture_output=True, check=True, text=True, **subprocess_kw)
        except subprocess.CalledProcessError as e:
            msg = f"Command {cmd} failed with the following error:\n{e.stderr}"
            raise SchedulerCommandError(msg) from e


class Slurm(Scheduler):
    name = "slurm"
    jobid_variable = "SLURM_JOB_ID"

    def cluster(self) -> str | None:
        if (cluster := os.environ.get("SLURM_CLUSTER_NAME")) is not None:
            return cluster
        try:
            config = self.run_command(["scontrol", "show", "config"]).stdout
        except (SchedulerCommandError, OSError):
            return None
        for line in config.splitlines():
            key, _, value = line.partition("=")
            if key.strip() == "ClusterName":
                return value.strip()
        return None

    def header_lines(
        self,
        task_handler: PluginHandler,
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
    ) -> list[str]:
        task = task_handler.task
        header: list[str] = [
            f"#SBATCH --output={task_handler.stdouterr_filename}",
            f"#SBATCH --error={task_handler.stdouterr_filename}",
            f"#SBATCH --job-name={task.label}",
        ]
        if account := task.account:
            header.append(f"#SBATCH --account={account}")
        if time := task.walltime:
            header.append(f"#SBATCH --time={time}")
        if partition := task.partition:
            header.append(f"#SBATCH --partition={partition}")
        if nodes := task.nodes:
            header.append(f"#SBATCH --nodes={nodes}")
        if gpus_per_node := task.gpus_per_node:
            header.append(f"#SBATCH --gpus-per-node={gpus_per_node}")
        if gres_flags := task.gres_flags:
            header.append(f"#SBATCH --gres-flags={gres_flags}")
        if gres := task.gres:
            header.append(f"#SBATCH --gres={gres}")
        if task.computer in UENV_MACHINES and task_handler.scheduler_uenv:
            if uenv := task.uenv:
                header.append(f"#SBATCH --uenv={uenv}")
            if view := task.view:
                header.append(f"#SBATCH --view={view}")
        if stdouterr_mode == "append":
            header.append("#SBATCH --open-mode=append")
        return header

    def submit_to_scheduler(
        self,
        task_handler: PluginHandler,
        parent_ids: list[str],
        env: dict[str, str],
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
        task = task_handler.task
        submit_cmd: list[str] = ["sbatch", "--parsable"]
        if parent_ids:
            match dependency_type:
                case "ALL_COMPLETED":
                    submit_cmd.append("--dependency=afterok:" + ":".join(parent_ids))
                case "ANY":
                    submit_cmd.append("--dependency=afterany:" + "?afterany:".join(parent_ids))
                case "NONE":
                    pass
                case _:
                    assert_never(dependency_type)
        submit_cmd.append(task_handler.submit_filename)

        if not env:
            msg = f"{task.label}: submission environment is empty"
            raise ValueError(msg)
        result = self.run_command(submit_cmd, cwd=task_handler.get_rundir(), env=env)
        return result.stdout.strip()

    def cancel(self, jobid: str) -> None:
        self.run_command(["scancel", jobid])

    def get_status(self, jobid: str) -> TaskStatus:
        """Infer task status using sacct"""
        result = self.run_command(
            [
                "sacct",
                "-n",
                "-X",
                "--format=state",
                "--parsable2",
                "--delimiter=''",
                "-j",
                jobid,
            ]
        )
        status_str = result.stdout.strip()
        if not status_str:
            msg = f"job {jobid} unknown to slurm"
            raise UnknownJobError(msg)
        # NOTE: For a complete list of SLURM state codes, see
        #       https://slurm.schedmd.com/job_state_codes.html
        match status_str:
            case s if s.startswith("RUNNING"):
                status = TaskStatus.RUNNING
            case s if s.startswith(("PENDING", "SUSPENDED", "PREEMPTED")):
                status = TaskStatus.WAITING
            case s if s.startswith("COMPLETED"):
                status = TaskStatus.COMPLETED
            case s if s.startswith(("FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT", "CANCELLED")):
                status = TaskStatus.FAILED
            case _:
                msg = f"unexpected status reported for job {jobid}: {status_str}"
                raise ValueError(msg)
        return status
