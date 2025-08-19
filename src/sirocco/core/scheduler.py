import subprocess
from dataclasses import dataclass
from typing import Literal

from sirocco.core.graph_items import Status, Task


@dataclass(kw_only=True)
class Scheduler:
    def submit_task(self, task: Task, dependency_type: Literal["ALL_COMPLETED", "ANY"] = "ALL_COMPLETED") -> None:
        raise NotImplementedError

    def update_status(self, task: Task) -> Status:
        raise NotImplementedError

    def cancel_task(self, task: Task) -> None:
        raise NotImplementedError

    @classmethod
    def create(cls, key: str) -> "Scheduler":
        if key == "slurm":
            return Slurm()
        msg = "Scheduler not implemented for {key}"
        raise NotImplementedError(msg)


@dataclass(kw_only=True)
class Slurm(Scheduler):
    def submit_task(self, task: Task, dependency_type: Literal["ALL_COMPLETED", "ANY"] = "ALL_COMPLETED"):
        """Submit a task"""

        # Shebang
        submit_script: list[str] = ["#!/bin/bash -l"]

        # SLURM header
        submit_script.append("")
        if account := task.account:
            submit_script.append(f"#SBATCH --account={account}")
        if time := task.walltime:
            submit_script.append(f"#SBATCH --time={time}")
        if nodes := task.nodes:
            submit_script.append(f"#SBATCH --nodes={nodes}")
        if ntasks_per_node := task.ntasks_per_node:
            submit_script.append(f"#SBATCH --ntasks-per-node={ntasks_per_node}")
        if uenv := task.uenv:
            submit_script.append(f"#SBATCH --uenv={uenv}")
        if view := task.view:
            submit_script.append(f"#SBATCH --view={view}")
        if task.parents:
            match dependency_type:
                case "ALL_COMPLETED":
                    submit_script.append(
                        "#SBATCH --dependency=afterok:" + ":".join([parent.jobid for parent in task.parents])
                    )
                case "ANY":
                    submit_script.append(
                        "#SBATCH --dependency=afterany:" + "?afterany:".join([parent.jobid for parent in task.parents])
                    )
                case _:
                    msg = "only ALL_COMPLETED and ANY are valid dependecy types"
                    raise ValueError(msg)

        # Rest of the script
        submit_script.append("")
        submit_script.extend(task.run_script_lines())

        # Submit
        (task.run_dir / task.submit_filename).write_text("\n".join(submit_script))
        result = subprocess.run(
            ["sbatch", "--parsable", task.submit_filename],  # noqa: S607
            capture_output=True,
            cwd=task.run_dir,
            check=False,
        )
        task.jobid = result.stdout.decode().strip()

    def cancel_task(self, task: Task):
        """Cancel a submitted task"""

        if task.jobid == "_NO_ID_":
            msg = f"task {task.label} cannot be canceled as it does not have a jobid"
            raise ValueError(msg)
        subprocess.run(["scancel", task.jobid], check=False)  # noqa: S607

    def update_status(self, task: Task) -> Status:
        raise NotImplementedError()
