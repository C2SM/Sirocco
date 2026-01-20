"""Patch AiiDA to handle SLURM job dependency problems gracefully.

Sirocco uses pre-submission with SLURM dependencies, which means CalcJobs are
submitted with --dependency=afterok:JOBID directives. When parent jobs complete
very quickly (or there's API propagation delay), SLURM may purge them from its
database before the dependent job is submitted, causing "Job dependency problem"
errors.

This patch adds two workarounds:
1. Retry submission without dependencies when SLURM reports dependency problems
2. Handle job polling delays (jobs not immediately visible after submission)

This is Sirocco-specific and not intended for upstream aiida-core.
"""

# TODO: Investigate why this appears again (seems weird that SLURM purges it form its db),
# and convert into a proper error handler

import logging

logger = logging.getLogger(__name__)


def patch_slurm_dependency_handling():
    """Apply monkey-patches for SLURM dependency handling.

    This patches:
    1. execmanager.submit_calculation - catch and handle dependency errors
    2. tasks.task_update_job - handle API propagation delays in job polling
    """
    from aiida.engine.daemon import execmanager
    from aiida.engine.processes.calcjobs import tasks
    from aiida.orm import CalcJobNode
    from aiida.orm.utils.log import get_dblogger_extra
    from aiida.schedulers.datastructures import JobState
    from aiida.schedulers.scheduler import SchedulerError
    from aiida.transports.transport import Transport

    # Store references to original methods
    original_submit_calculation = execmanager.submit_calculation
    original_task_update_job = tasks.task_update_job

    def _retry_submit_without_dependencies(
        calculation: CalcJobNode,
        scheduler,
        transport: Transport,
        workdir: str,
        submit_script_filename: str,
    ) -> str:
        """Retry job submission after filtering out finished dependencies.

        This is called when SLURM rejects a job due to dependency problems, typically
        because some dependency jobs have already completed and been purged from the
        scheduler's database. We query SLURM to determine which dependencies still
        exist and only keep those, allowing already-satisfied dependencies to be removed.

        :param calculation: the CalcJobNode to submit
        :param scheduler: the scheduler instance
        :param transport: the transport instance
        :param workdir: the remote working directory (as string)
        :param submit_script_filename: name of the submit script file
        :return: the job id from successful submission
        """
        import re
        import tempfile
        from pathlib import Path, PurePosixPath

        from aiida.common.log import AIIDA_LOGGER
        logger_extra = get_dblogger_extra(calculation)

        script_path = str(PurePosixPath(workdir) / submit_script_filename)

        # Read the submit script - handle both sync and async transports
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sh', delete=False) as tmpfile:
            tmp_path = Path(tmpfile.name)
            try:
                # Download the script
                transport.getfile(script_path, tmp_path)
                script_content = tmp_path.read_text()

                # Find and parse the dependency directive
                # Match lines like: #SBATCH --dependency=afterok:123:456
                dependency_match = re.search(
                    r'^#SBATCH\s+--dependency=(\w+):([0-9:]+)$',
                    script_content,
                    flags=re.MULTILINE
                )

                if not dependency_match:
                    # No dependency directive found - something else is wrong
                    raise SchedulerError(
                        'Job dependency problem reported but no dependency directive found in submit script'
                    )

                dependency_type = dependency_match.group(1)  # e.g., 'afterok'
                job_ids_str = dependency_match.group(2)  # e.g., '123:456:789'
                job_ids = job_ids_str.split(':')

                AIIDA_LOGGER.info(
                    f'Job submission failed due to dependency problem. Original dependencies: {dependency_type}:{job_ids_str}',
                    extra=logger_extra
                )

                # Query scheduler to see which jobs still exist
                try:
                    scheduler.get_jobs(jobs=job_ids)
                    existing_jobs = []
                    for job_id in job_ids:
                        try:
                            # Try to get info for each job individually
                            job_info = scheduler.get_jobs(jobs=[job_id])
                            if job_info:
                                existing_jobs.append(job_id)
                                AIIDA_LOGGER.debug(
                                    f'Dependency job {job_id} still exists in scheduler',
                                    extra=logger_extra
                                )
                        except Exception:
                            AIIDA_LOGGER.debug(
                                f'Dependency job {job_id} no longer in scheduler (finished)',
                                extra=logger_extra
                            )
                except Exception as e:
                    AIIDA_LOGGER.warning(
                        f'Failed to query scheduler for job status: {e}. Will remove all dependencies.',
                        extra=logger_extra
                    )
                    existing_jobs = []

                # Rebuild the dependency directive with only existing jobs
                original_line = dependency_match.group(0)
                if existing_jobs:
                    new_dependency = f"#SBATCH --dependency={dependency_type}:{':'.join(existing_jobs)}"
                    new_line = f"{new_dependency}  # Filtered from: {job_ids_str}"
                    AIIDA_LOGGER.info(
                        f'Keeping dependencies on still-running jobs: {":".join(existing_jobs)}',
                        extra=logger_extra
                    )
                else:
                    new_line = f"# DEPENDENCY REMOVED: all dependencies satisfied ({job_ids_str})"
                    AIIDA_LOGGER.info(
                        'All dependencies satisfied, removing dependency directive',
                        extra=logger_extra
                    )

                script_content = script_content.replace(original_line, new_line)

                # Write modified content to temp file
                tmp_path.write_text(script_content)

                # Upload the modified script back
                transport.putfile(tmp_path, script_path)

            finally:
                # Clean up temp file
                tmp_path.unlink(missing_ok=True)

        # Retry submission
        AIIDA_LOGGER.info(
            'Resubmitting job with updated dependencies',
            extra=logger_extra
        )

        return scheduler.submit_job(workdir, submit_script_filename)

    def patched_submit_calculation(calculation: CalcJobNode, transport: Transport):
        """Submit calculation with SLURM dependency error handling.

        This patched version wraps the original submit_calculation and catches
        SLURM "Job dependency problem" errors, retrying without dependencies.
        """
        from aiida.common.log import AIIDA_LOGGER

        job_id = calculation.get_job_id()

        # If already submitted, return existing job_id
        if job_id is not None:
            return job_id

        scheduler = calculation.computer.get_scheduler()
        scheduler.set_transport(transport)

        submit_script_filename = calculation.get_option('submit_script_filename')
        workdir = calculation.get_remote_workdir()

        try:
            result = scheduler.submit_job(workdir, submit_script_filename)
        except SchedulerError as exc:
            # Handle SLURM job dependency problems
            if 'Job dependency problem' in str(exc):
                logger_extra = get_dblogger_extra(calculation)
                AIIDA_LOGGER.warning(
                    'SLURM job dependency problem detected - dependencies may have already completed. '
                    'Stripping dependencies and resubmitting.',
                    extra=logger_extra
                )

                # Strip dependency directive from submit script and retry
                result = _retry_submit_without_dependencies(
                    calculation, scheduler, transport, workdir, submit_script_filename
                )
            else:
                # Re-raise other scheduler errors
                raise

        if isinstance(result, str):
            calculation.set_job_id(result)

        return result

    async def patched_task_update_job(node, job_manager, cancellable):
        """Update job state with handling for API propagation delays.

        This patched version handles cases where a job has been submitted but
        isn't yet visible in the scheduler (common with FirecREST and other
        REST APIs that have propagation delays).
        """
        from aiida.engine.processes.calcjobs.tasks import TransportTaskException

        # Delegate to original implementation for the actual job polling
        # We just wrap it to handle the case where job_info is None
        job_id = node.get_job_id()

        if job_id is None:
            logger.warning(f'job for node<{node.pk}> does not have a job id')
            return True

        # Call the original task_update_job to get job info
        # This handles all the async job manager logic correctly
        try:
            job_done = await original_task_update_job(node, job_manager, cancellable)
            return job_done
        except Exception as e:
            # Check if this is because the job isn't visible yet
            # If we've never successfully polled this job before, it might just be API delay
            last_job_info = node.get_last_job_info()

            if last_job_info is None and "not found" in str(e).lower():
                # Never successfully polled this job - might be API propagation delay
                # Raise TransportTaskException to trigger retry with exponential backoff
                raise TransportTaskException(
                    f'Job<{job_id}> not yet visible in scheduler (possible API propagation delay). Will retry.'
                ) from e
            else:
                # Some other error, or job was polled before - re-raise
                raise

    # Apply the patches
    execmanager.submit_calculation = patched_submit_calculation
    tasks.task_update_job = patched_task_update_job

    logger.info(
        "Applied SLURM dependency handling patches for Sirocco pre-submission workflows"
    )
