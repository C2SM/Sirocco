import shutil
import subprocess
from dataclasses import dataclass
from typing import Literal, assert_never

from sirocco.core._tasks.sirocco_task import SiroccoContinueTask
from sirocco.core.graph_items import Task, TaskStatus


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
        # TODO: Make shebang a config file entry defaulting to "#!/bin/bash -l"
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

        # Task runscript "content"
        script_lines.append("")
        script_lines.extend(task.runscript_lines())

        # Accounting
        if not isinstance(task, SiroccoContinueTask):
            script_lines.append(
                "sacct -j ${SLURM_JOB_ID} --format='User,JobID,Jobname,partition,state,time,start,end,elapsed,nnodes,ncpus'"
            )

        # Submit runscript
        # ================
        (task.run_dir / task.SUBMIT_FILENAME).write_text("\n".join(script_lines))
        task.jobid = self.submit_to_scheduler(task)

        # Dump task jobid and rank
        # ========================
        task.dump_jobid_and_rank()

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


@dataclass(kw_only=True)
class Slurm(Scheduler):
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
        if uenv := task.uenv:
            header.append(f"#SBATCH --uenv={uenv}")
        if view := task.view:
            header.append(f"#SBATCH --view={view}")
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
        result = subprocess.run(
            ["sbatch", "--parsable", task.SUBMIT_FILENAME],  # noqa: S607
            capture_output=True,
            cwd=task.run_dir,
            check=True,
        )
        return result.stdout.decode().strip()

    def cancel(self, task: Task):
        """Cancel a submitted task"""

        if task.jobid == "_NO_ID_":
            msg = f"task {task.label} cannot be canceled as it does not have a jobid"
            raise ValueError(msg)
        subprocess.run(["scancel", task.jobid], check=True)  # noqa: S607

    def get_status(self, task: Task) -> TaskStatus:
        """Infer task status using sacct"""

        result = subprocess.run(
            ["sacct", "-o", "state", "-p", "-j", task.jobid],  # noqa: S607
            capture_output=True,
            check=True,
        )
        status_str = result.stdout.decode().strip().split("\n")[1][:-1]
        # NOTE: For a complete list of SLURM state codes, see
        #       https://slurm.schedmd.com/job_state_codes.html
        match status_str:
            case s if s.startswith(("RUNNING", "PENDING", "SUSPENDED")):
                status = TaskStatus.ONGOING
            case s if s.startswith("COMPLETED"):
                status = TaskStatus.COMPLETED
            case s if s.startswith(("FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT", "CANCELLED")):
                status = TaskStatus.FAILED
            case _:
                msg = f"unexpected status reported for task {task.label}: {status_str}"
                raise ValueError(msg)
        return status
