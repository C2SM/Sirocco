import logging
import shutil
import subprocess
from dataclasses import dataclass
from typing import Literal, assert_never

from sirocco.core.graph_items import Task, TaskStatus

LOGGER = logging.getLogger(__name__)


class SchedulerCommandError(RuntimeError):
    ...

@dataclass(kw_only=True)
class Scheduler:
    def submit(
        self,
        task: Task,
        output_mode: Literal["overwrite", "append"] = "overwrite",
        dependency_type: Literal["ALL_COMPLETED", "ANY"] = "ALL_COMPLETED",
    ):
        """Submit a task"""

        # Prepare for submission ( create rundir, wipe rundir, link/copy necessary files/dirs, ...)
        # ======================
        if task.CLEAN_UP_BEFORE_SUBMIT:
            shutil.rmtree(task.run_dir, ignore_errors=True)
        task.run_dir.mkdir(parents=True, exist_ok=True)
        task.prepare_for_submission()

        # Build runscript
        # ===============
        # Shebang
        script_lines: list[str] = ["#!/bin/bash -l", ""]

        # Scheduler header
        script_lines.extend(self.header_lines(task, output_mode=output_mode, dependency_type=dependency_type))

        # Some MPI environment variables for potential usage by the user defined runscript content
        script_lines.append("")
        if task.nodes is not None:
            script_lines.append(f"N_NODES={task.nodes}")
        if task.ntasks_per_node is not None:
            script_lines.append(f"N_TASKS_PER_NODE={task.ntasks_per_node}")
        if task.cpus_per_task is not None:
            script_lines.append(f"CPUS_PER_TASK={task.cpus_per_task}")

        # Sirocco context
        script_lines.append("")
        script_lines.extend(task.sirocco_environemnt())

        # Linked input
        script_lines.extend(self.add_links(task))

        # Task runscript "content"
        script_lines.append("")
        script_lines.extend(task.runscript_lines())

        # Submit runscript
        # ================
        (task.run_dir / task.SUBMIT_FILENAME).write_text("\n".join(script_lines))
        task.jobid = self.submit_to_scheduler(task)

    def add_links(self, task: Task) -> list[str]:
        link_list: list[str] = []
        if "link" in task.inputs:
            link_list.extend([f"ln -s {data.resolved_path} ." for data in task.inputs["link"]])
        if "link_content" in task.inputs:
            link_list.extend(
                [
                    f"for item in {data.resolved_path}/*; do ln -s ${{item}} .; done"
                    for data in task.inputs["link_content"]
                ]
            )
        return link_list

    def header_lines(
        self,
        task: Task,
        output_mode: Literal["overwrite", "append"] = "overwrite",
        dependency_type: Literal["ALL_COMPLETED", "ANY", "NONE"] = "ALL_COMPLETED",
    ) -> list[str]:
        raise NotImplementedError

    def submit_to_scheduler(self, task: Task) -> str:
        raise NotImplementedError

    def get_status(self, task: Task) -> TaskStatus:
        raise NotImplementedError

    def cancel(self, task: Task) -> None:
        raise NotImplementedError

    @classmethod
    def create(cls, key: str) -> "Scheduler":
        if key == "slurm":
            return Slurm()
        msg = f"Scheduler {key} not implemented"
        raise NotImplementedError(msg)

    @staticmethod
    def run_command(cmd: list[str], **subprocess_kw) -> subprocess.CompletedProcess:
        try:
            result = subprocess.run(cmd, capture_output=True, check=True, **subprocess_kw)
            return result
        except subprocess.CalledProcessError as e:
            msg = f"Command {cmd} failed with the following error:\n{e.stderr.decode()}"
            raise SchedulerCommandError(msg)


@dataclass(kw_only=True)
class Slurm(Scheduler):

    def __post_init__(self):
        self.sbatch = shutil.which("sbatch")

    def header_lines(
        self,
        task: Task,
        output_mode: Literal["overwrite", "append"] = "overwrite",
        dependency_type: Literal["ALL_COMPLETED", "ANY", "NONE"] = "ALL_COMPLETED",
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
        if nodes := task.nodes:
            header.append(f"#SBATCH --nodes={nodes}")
        if ntasks_per_node := task.ntasks_per_node:
            header.append(f"#SBATCH --ntasks-per-node={ntasks_per_node}")
        if output_mode == "append":
            header.append("#SBATCH --open-mode=append")
        if parent_ids := [parent.jobid for parent in task.parents if parent.rank >= 0]:
            # NOTE: We can safely remove tasks with rank -1 from the parents list
            #       in order to avoid depending on old tasks for which the scheduler
            #       has no info anymore when restarting after a long time.
            match dependency_type:
                case "ALL_COMPLETED":
                    header.append("#SBATCH --dependency=afterok:" + ":".join(parent_ids))
                case "ANY":
                    header.append("#SBATCH --dependency=afterany:" + "?afterany:".join(parent_ids))
                case "NONE":
                    pass
                case _:
                    assert_never(dependency_type)
        return header

    def submit_to_scheduler(self, task: Task) -> str:
        submit_cmd: list[str] = [self.sbatch, "--parsable"]
        if uenv := task.uenv:
            submit_cmd.append(f"--uenv={uenv}")
        if view := task.view:
            submit_cmd.append(f"--view={view}")
        submit_cmd.append(task.SUBMIT_FILENAME)

        result = self.run_command(submit_cmd, cwd=task.run_dir)
        return result.stdout.decode().strip()

    def cancel(self, task: Task):
        """Cancel a submitted task"""

        if task.jobid == "_NO_ID_":
            msg = f"task {task.label} cannot be canceled as it does not have a jobid"
            raise ValueError(msg)
        self.run_command(["scancel", task.jobid])

    def get_status(self, task: Task) -> TaskStatus:
        """Infer task status using sacct"""

        result = self.run_command(["sacct", "-o", "state", "-p", "-j", task.jobid])
        status_str = result.stdout.decode().strip().split("\n")[1][:-1]
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
