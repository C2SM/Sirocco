"""Job monitoring - Async polling for CalcJob completion."""

from __future__ import annotations

import asyncio
import logging

import yaml
from aiida.orm.utils.serialize import AiiDALoader
from aiida_workgraph import namespace, task

__all__ = ["get_job_data"]

logger = logging.getLogger(__name__)


async def _poll_for_job_data(workgraph_name: str, task_name: str, interval: int = 10) -> dict:
    """Poll for job data until available.

    Args:
        workgraph_name: Name of the WorkGraph to monitor
        task_name: Name of the task within the WorkGraph
        interval: Polling interval in seconds

    Returns:
        Dict with job_id and remote_folder PK
    """
    from aiida import orm
    from aiida_workgraph.engine.workgraph import WorkGraphEngine

    while True:
        # Query for WorkGraphs
        builder = orm.QueryBuilder()
        builder.append(
            WorkGraphEngine,
            filters={
                "attributes.process_label": {"==": f"WorkGraph<{workgraph_name}>"},
            },
        )

        if builder.count() == 0:
            await asyncio.sleep(interval)
            continue

        workgraph_node = builder.all()[-1][0]
        node_data = workgraph_node.task_processes.get(task_name)
        if not node_data:
            await asyncio.sleep(interval)
            continue

        node = yaml.load(node_data, Loader=AiiDALoader)  # noqa: S506
        if not node:
            await asyncio.sleep(interval)
            continue

        job_id = node.get_job_id()
        if job_id is None:
            await asyncio.sleep(interval)
            continue

        # SUCCESS — return early
        remote_pk = node.outputs.remote_folder.pk
        return {"job_id": int(job_id), "remote_folder": remote_pk}


@task(outputs=namespace(job_id=int, remote_folder=int))
async def get_job_data(
    workgraph_name: str,
    task_name: str,
    timeout_seconds: int,
    interval: int = 10,
):
    """Monitor CalcJob and return job_id and remote_folder PK when available.

    Args:
        workgraph_name: Name of the WorkGraph to monitor
        task_name: Name of the task within the WorkGraph
        timeout_seconds: Timeout in seconds (based on task walltime + buffer)
        interval: Polling interval in seconds

    Returns:
        Dict with job_id and remote_folder PK

    Raises:
        TimeoutError: If job data is not available within timeout period
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            return await _poll_for_job_data(workgraph_name, task_name, interval)
    except TimeoutError as err:
        msg = f"Timeout waiting for job_id for task {task_name} after {timeout_seconds}s"
        raise TimeoutError(msg) from err
