import subprocess
from dataclasses import dataclass
from typing import Literal, assert_never

from sirocco.core.graph_items import Status, Task


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
        task.prepare_for_submission()

        # Shebang
        submit_script: list[str] = ["#!/bin/bash -l"]

        # SLURM header
        submit_script.extend(self.header_lines(task, output_mode=output_mode, dependency_type=dependency_type))

        # Rest of the script
        submit_script.append("")
        submit_script.extend(task.runscript_lines())

        # Submit
        (task.run_dir / task.SUBMIT_FILENAME).write_text("\n".join(submit_script))
        result = subprocess.run(
            ["sbatch", "--parsable", task.SUBMIT_FILENAME],  # noqa: S607
            capture_output=True,
            cwd=task.run_dir,
            check=True,
        )
        task.jobid = result.stdout.decode().strip()

        # Serialize required information
        task.jobid_path.write_text(f"{task.jobid}")
        task.rank_path.write_text(f"{task.rank}")

    def header_lines(
        self,
        task: Task,
        output_mode: Literal["overwrite", "append"] = "overwrite",
        dependency_type: Literal["ALL_COMPLETED", "ANY", "NONE"] = "ALL_COMPLETED",
    ) -> list[str]:
        raise NotImplementedError

    def get_status(self, task: Task) -> Status:
        raise NotImplementedError

    def cancel(self, task: Task) -> None:
        raise NotImplementedError

    @classmethod
    def create(cls, key: str) -> "Scheduler":
        if key == "slurm":
            return Slurm()
        msg = "Scheduler not implemented for {key}"
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
        if task.parents:
            # NOTE: We can safely remove tasks with rank -1 from the parents list
            #       in order to avoid depending on old tasks for which the scheduler
            #       has no info anymore when restarting after a long time.
            # parent_ids: list[str] = [parent.jobid for parent in task.parents if parent.rank > 0]
            parent_ids: list[str] = [parent.jobid for parent in task.parents]
            if parent_ids:
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

    def cancel(self, task: Task):
        """Cancel a submitted task"""

        if task.jobid == "_NO_ID_":
            msg = f"task {task.label} cannot be canceled as it does not have a jobid"
            raise ValueError(msg)
        subprocess.run(["scancel", task.jobid], check=True)  # noqa: S607

    def get_status(self, task: Task) -> Status:
        """Infer task status using sacct"""

        result = subprocess.run(
            ["sacct", "-o", "state", "-p", "-j", task.jobid],  # noqa: S607
            capture_output=True,
            check=True,
        )
        status_str = result.stdout.decode().strip().split("\n")[-1][:-1]
        # NOTE: For a complete list of SLURM state codes, see
        #       https://slurm.schedmd.com/job_state_codes.html
        match status_str:
            case "RUNNING" | "PENDING" | "SUSPENDED":
                status = Status.ONGOING
            case "COMPLETED":
                status = Status.COMPLETED
            case "FAILED" | "NODE_FAIL" | "OUT_OF_MEMORY" | "TIMEOUT" | "CANCELLED":
                status = Status.FAILED
            case _:
                msg = f"unexpected status reported for task {task.label}: {status_str}"
                raise ValueError(msg)
        return status
