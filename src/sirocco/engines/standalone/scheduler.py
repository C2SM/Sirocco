"""Job scheduler interface of the standalone engine.

The scheduler is stateless with respect to tasks: it receives job ids from the
engine (parents, task to cancel or query) and returns the new job id on
submission. Only the engine reads and writes the state table.
"""

import logging
import os
import shutil
import subprocess
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, assert_never

from sirocco.core.graph_items import Task, TaskStatus
from sirocco.engines.standalone import handlers

LOGGER = logging.getLogger(__name__)
UENV_MACHINES: list[str] = ["santis"]

type STDOUTERR_MODE_T = Literal["overwrite", "append"]
type DEPENDENCY_TYPE_T = Literal["ALL_COMPLETED", "ANY", "NONE"]


class SchedulerCommandError(RuntimeError): ...


@contextmanager
def ignore_env(*args: str):
    ignored_vars: dict[str, str] = {var: os.environ.pop(var) for var in args if var in os.environ}
    try:
        yield
    finally:
        os.environ.update(ignored_vars)


@dataclass(kw_only=True)
class Scheduler(ABC):
    def submit(
        self,
        task: Task,
        parent_ids: list[str],
        env: dict[str, str],
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
        """Submit a task from environment ``env`` and return its job id"""

        # Prepare for submission ( create rundir, wipe rundir, link/copy necessary files/dirs, ...)
        # ======================
        if task.CLEAN_UP_BEFORE_SUBMIT:
            shutil.rmtree(task.run_dir, ignore_errors=True)
        task.run_dir.mkdir(parents=True, exist_ok=True)
        handlers.prepare_for_submission(task)

        # Generate runscript
        # ==================
        self.generate_run_script(task=task, stdouterr_mode=stdouterr_mode)

        # Submit runscript
        # ================
        return self.submit_to_scheduler(task, parent_ids=parent_ids, env=env, dependency_type=dependency_type)

    def generate_run_script(
        self, task: Task, stdouterr_mode: STDOUTERR_MODE_T, script_path: Path | None = None
    ) -> None:
        # Shebang
        script_lines: list[str] = ["#!/bin/bash -l", ""]

        # Scheduler header
        script_lines.extend(self.header_lines(task, stdouterr_mode=stdouterr_mode))

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
        script_lines.extend(handlers.sirocco_environment_lines(task))

        # Linked input
        script_lines.extend(handlers.link_lines(task))

        # Task runscript "content"
        script_lines.append("")
        script_lines.extend(handlers.runscript_lines(task))

        # Write to runscript file
        if script_path is None:
            script_path = task.run_dir / task.SUBMIT_FILENAME
        script_path.write_text("\n".join(script_lines))
        script_path.chmod(0o755)

    @abstractmethod
    def header_lines(
        self,
        task: Task,
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
    ) -> list[str]:
        pass

    @abstractmethod
    def submit_to_scheduler(
        self,
        task: Task,
        parent_ids: list[str],
        env: dict[str, str],
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
        pass

    @abstractmethod
    def get_status(self, task: Task, jobid: str) -> TaskStatus:
        pass

    @abstractmethod
    def cancel(self, task: Task, jobid: str) -> None:
        pass

    @classmethod
    def create(cls, key: str) -> "Scheduler":
        if key == "slurm":
            return Slurm()
        msg = f"Scheduler {key} not implemented"
        raise NotImplementedError(msg)

    @staticmethod
    def run_command(cmd: list[str], **subprocess_kw) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(cmd, capture_output=True, check=True, text=True, **subprocess_kw)
        except subprocess.CalledProcessError as e:
            msg = f"Command {cmd} failed with the following error:\n{e.stderr}"
            raise SchedulerCommandError(msg) from e


@dataclass(kw_only=True)
class Slurm(Scheduler):
    def header_lines(
        self,
        task: Task,
        stdouterr_mode: STDOUTERR_MODE_T = "overwrite",
    ) -> list[str]:
        header: list[str] = [
            f"#SBATCH --output={task.STDOUTERR_FILENAME}",
            f"#SBATCH --error={task.STDOUTERR_FILENAME}",
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
        if task.computer in UENV_MACHINES:
            if uenv := task.uenv:
                header.append(f"#SBATCH --uenv={uenv}")
            if view := task.view:
                header.append(f"#SBATCH --view={view}")
        if stdouterr_mode == "append":
            header.append("#SBATCH --open-mode=append")
        return header

    def submit_to_scheduler(
        self,
        task: Task,
        parent_ids: list[str],
        env: dict[str, str],
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
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
        submit_cmd.append(task.SUBMIT_FILENAME)

        if not env:
            msg = f"{task.label}: submission environment is empty"
            raise ValueError(msg)
        result = self.run_command(submit_cmd, cwd=task.run_dir, env=env)
        return result.stdout.strip()

    def cancel(self, task: Task, jobid: str):
        """Cancel a submitted task"""

        if jobid == "_NO_ID_":
            msg = f"task {task.label} cannot be canceled as it does not have a jobid"
            raise ValueError(msg)
        self.run_command(["scancel", jobid])

    def get_status(self, task: Task, jobid: str) -> TaskStatus:
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
                msg = f"unexpected status reported for task {task.label}: {status_str}"
                raise ValueError(msg)
        return status
