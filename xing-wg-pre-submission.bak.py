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

from aiida_workgraph import task, namespace, dynamic
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida import load_profile, orm
from typing import Annotated

load_profile()

# Define the AddTask using ArithmeticAddCalculation CalcJob
AddTask = task(ArithmeticAddCalculation)


@task
async def get_job_id(workgraph_name, task_name, interval=2, timeout=600):
    """Get the job_id of a CalcJob task in a workgraph by polling the workgraph node."""
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine
    from aiida.orm.utils.serialize import AiiDALoader
    import yaml
    import time
    import asyncio

    # Query the WorkGraph node by its name
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
                    return job_id
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timeout waiting for job_id for task {task_name}")
        await asyncio.sleep(interval)


@task.graph(outputs=AddTask.outputs)
def launch_task_with_dependency(
    x: int,
    y: int,
    code: orm.Code,
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
    out = AddTask(
        x=x,
        y=y,
        code=code,
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
    A_out = launch_task_with_dependency(
        x=x, y=y, code=code, metadata={"call_link_label": "launch_A"}
    )
    # monitor Task A to get its job_id when available
    A_id = get_job_id(workgraph_name="launch_A", task_name="AddTask").result
    # use A_id to set dependency for the Tasks B, C
    B_out = launch_task_with_dependency(
        x=x,
        y=y,
        code=code,
        job_ids={"A": A_id},
        metadata={"call_link_label": "launch_B"},
    )
    B_id = get_job_id(workgraph_name="launch_B", task_name="AddTask").result
    C_out = launch_task_with_dependency(
        x=x,
        y=y,
        code=code,
        job_ids={"A": A_id},
        metadata={"call_link_label": "launch_C"},
    )
    C_id = get_job_id(workgraph_name="launch_C", task_name="AddTask").result
    # B_out wait for A_id, so B_id should also wait for A_id
    A_id >> B_id
    A_id >> C_id
    # use both B_id and task_C_id to set dependency for Task D
    task_D_out = launch_task_with_dependency(
        x=x,
        y=y,
        code=code,
        metadata={"call_link_label": "launch_D"},
        job_ids={"B": B_id, "C": C_id},
    )
    return {"sum": task_D_out.sum}


# ------------------------------------------------

code = orm.load_code("add@santis-async-ssh")
x = 3
y = 4

wg = main_graph.build(x=x, y=y, code=code)
wg.submit()
print(f"Final result: {wg.outputs.sum}")
