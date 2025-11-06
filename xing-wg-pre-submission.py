"""
This is an example of using aiida-workgraph to launch AiiDA CalcJob tasks with SLURM job dependencies.
The workgraph consists of four AddTasks (A, B, C, D) with the following dependencies:
Task A -> Task B, Task C -> Task D

Here is shot of the SLURM job queue showing the dependencies in action:
$ squeue
    JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
127025    normal aiida-19  wang_x3 PD       0:00      1 (Dependency)   <-- Task C
127024    normal aiida-19  wang_x3 PD       0:00      1 (Dependency)   <-- Task B
127023    normal aiida-19  wang_x3  R       0:19      1 thor3          <-- Task A

"""

from aiida_workgraph import task, namespace, dynamic, group
from aiida import load_profile, orm
from typing import Annotated
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation

load_profile()


# Define a custom CalcJob to allow passing parent_folder as input for test purposes
class CustomArithmeticAddCalculation(ArithmeticAddCalculation):
    """Custom ArithmeticAddCalculation to allow passing parent_folders as input."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        # In the "prepare_for_submission" method, use symlink the parent folder instead of copying the contents
        spec.input_namespace(
            "parent_folders", valid_type=orm.RemoteData, required=False
        )

    # def prepare_for_submission(self, folder):
    # In the "prepare_for_submission" method, use symlink the parent folder instead of copying the contents
    # This is important! Because if you copy the contents directly, the files will not be available.


# Transfer the Calcjob to a task
AddTask = task(CustomArithmeticAddCalculation)


@task(outputs=namespace(job_id=int, remote_folder=int))
async def get_job_data(workgraph_name, task_name, interval=2, timeout=600):
    """Monitor the CalcJob task in a workgraph by polling its status.
    Once the CalcJob task has a job_id, return the job_id and remote_folder.
    """
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine
    from aiida.orm.utils.serialize import AiiDALoader
    import yaml
    import time
    import asyncio

    # Query the WorkGraph node by its name
    # One can give a unique name to a workgraph when building it (in the "main_graph" function below)
    builder = orm.QueryBuilder()
    builder.append(
        WorkGraphEngine,
        filters={"attributes.process_label": {"==": f"WorkGraph<{workgraph_name}>"}},
        tag="process",
    )
    start_time = time.time()
    while True:
        if builder.count() > 0:
            # Get the last node in the workgraph
            workgraph_node = builder.all()[-1][0]
            # Load the AiiDA process node for the specified task
            node = yaml.load(
                workgraph_node.task_processes.get(task_name, ""), Loader=AiiDALoader
            )
            if node:
                job_id = node.get_job_id()
                if job_id is not None:
                    return {
                        "job_id": int(job_id),
                        "remote_folder": node.outputs.remote_folder.pk,
                    }
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timeout waiting for job_id for task {task_name}")
        await asyncio.sleep(interval)


@task.graph(outputs=AddTask.outputs)
def launch_task_with_dependency(
    x: int,
    y: int,
    code: orm.Code,
    parent_folders: Annotated[
        dict, dynamic(int)
    ] = None,  # dynamic inputs, allow passing multiple RemoteData nodes as parent_folders
    job_ids: Annotated[
        dict, dynamic(int)
    ] = None,  # dynamic inputs, so multiple job IDs can be passed in a dictionary
):
    """Add slurm dependency to a task using custom_scheduler_commands."""
    # Normalize job_ids into a colon-separated string
    if job_ids:
        dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        custom_cmd = f"#SBATCH --dependency=afterok:{dep_str}"
    else:
        custom_cmd = ""  # no dependency
    # load the RemoteData nodes from their pks, and pass them as parent_folders input
    # This is necessary for keeping the data dependencies in AiiDA
    parent_folders = (
        {key: orm.load_node(val.value) for key, val in parent_folders.items()}
        if parent_folders
        else None
    )
    out = AddTask(
        x=x,
        y=y,
        code=code,
        parent_folders=parent_folders,
        metadata={
            "call_link_label": "AddTask",
            "options": {"custom_scheduler_commands": custom_cmd, "sleep": 20},
        },
    )
    return out


@task.graph
def main_graph(x, y, code) -> Annotated[dict, namespace(sum=int)]:
    """Main graph to run AddTasks with slurm dependency.

    Task A -> Task B, Task C -> Task D
    """
    # First launch Task A without dependency, job_ids is None
    # Set a unique name ("launch_A") to the workgraph for querying later
    launch_task_with_dependency(
        x=x, y=y, code=code, metadata={"call_link_label": "launch_A"}
    )
    # monitor Task A to get its job_id and remote_folder when available
    A_data = get_job_data(workgraph_name="launch_A", task_name="AddTask")
    # use A_data to set dependency and parent_folders for the Tasks B, C
    launch_task_with_dependency(
        x=x,
        y=y,
        code=code,
        job_ids={"A": A_data.job_id},
        parent_folders={"A": A_data.remote_folder},
        metadata={"call_link_label": "launch_B"},
    )
    B_data = get_job_data(workgraph_name="launch_B", task_name="AddTask")
    launch_task_with_dependency(
        x=x,
        y=y,
        code=code,
        parent_folders={"A": A_data.remote_folder},
        job_ids={"A": A_data.job_id},
        metadata={"call_link_label": "launch_C"},
    )
    C_data = get_job_data(workgraph_name="launch_C", task_name="AddTask")
    # Start monitor task B and C only when task A has its job id.
    # This is not strictly necessary
    A_data >> group(B_data, C_data)
    # use both B_data and C_data to set dependency for Task D
    task_D_out = launch_task_with_dependency(
        x=x,
        y=y,
        code=code,
        parent_folders={"B": B_data.remote_folder, "C": C_data.remote_folder},
        metadata={"call_link_label": "launch_D"},
        job_ids={"B": B_data.job_id, "C": C_data.job_id},
    )
    return {"sum": task_D_out.sum}


# ------------------------------------------------
# use thor cluster for testing
code = orm.load_code("add@thor")
x = 3
y = 4

wg = main_graph.build(x=x, y=y, code=code)
wg.run()
print(f"Final result: {wg.outputs.sum}")
