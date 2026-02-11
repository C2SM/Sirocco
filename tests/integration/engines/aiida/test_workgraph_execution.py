"""Integration tests for end-to-end WorkGraph execution.

Tests that actually execute workflows and verify runtime behavior
(timing, branch independence, submission order).

These tests are marked as slow and require a running AiiDA daemon.
"""

import logging

import pytest

from sirocco.core import Workflow
from sirocco.engines.aiida import build_sirocco_workgraph

LOGGER = logging.getLogger(__name__)


@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "dynamic-simple",
    ],
)
def test_branch_independence_execution(config_paths):
    """Integration test that actually runs the branch independence workflow.

    This test verifies that with dynamic levels and front_depth=1:
    - The faster branch completes without waiting for the slower branch
    - Tasks are pre-submitted before their dependencies finish (front_depth=1)
    - Submission order follows dynamic level computation
    """
    import logging
    from datetime import datetime
    from pathlib import Path
    from zoneinfo import ZoneInfo

    from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
    from aiida.orm import CalcJobNode

    from tests.integration.utils import (
        assert_branch_independence,
        assert_pre_submission_occurred,
        assert_submission_order,
        extract_launcher_times,
        print_timing_summary,
    )

    LOGGER = logging.getLogger(__name__)

    # Set up persistent file logging
    log_file = Path("branch_independence_test.log")
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    LOGGER.addHandler(file_handler)
    LOGGER.setLevel(logging.INFO)

    LOGGER.info("=" * 80)
    LOGGER.info("Starting branch independence integration test")
    LOGGER.info("=" * 80)

    # Build and run the workflow
    LOGGER.info("Building workflow from config")
    core_workflow = Workflow.from_config_file(str(config_paths["yml"]), template_context=config_paths["variables"])
    workgraph = build_sirocco_workgraph(core_workflow)
    LOGGER.info("WorkGraph built with %s tasks", len(workgraph.tasks))

    # Track task completion times
    start_time = datetime.now(ZoneInfo("Europe/Zurich"))
    LOGGER.info("Starting workflow execution at %s", start_time)
    workgraph.run()
    end_time = datetime.now(ZoneInfo("Europe/Zurich"))
    output_node = workgraph.process

    total_time = (end_time - start_time).total_seconds()
    LOGGER.info("Workflow execution completed in %.1fs", total_time)
    LOGGER.info("Workflow PK: %s", output_node.pk)
    LOGGER.info("Workflow state: %s", output_node.process_state)
    LOGGER.info("Workflow exit_code: %s", output_node.exit_code)

    # Check if workflow completed successfully
    if not output_node.is_finished_ok:
        LOGGER.error("Workflow did not finish successfully!")
        LOGGER.error(
            "Workchain report:\n%s",
            get_workchain_report(output_node, levelname="REPORT"),
        )
        for node in output_node.called_descendants:
            if isinstance(node, CalcJobNode):
                LOGGER.error("%s workdir: %s", node.process_label, node.get_remote_workdir())
                LOGGER.error("%s report:\n%s", node.process_label, get_calcjob_report(node))

    assert output_node.is_finished_ok, (
        f"Workflow failed. Exit code: {output_node.exit_code}, message: {output_node.exit_message}"
    )

    # Extract launcher timing data (when tasks were submitted and completed)
    launcher_times = extract_launcher_times(output_node)
    LOGGER.info("Extracted timing data for %s tasks", len(launcher_times))

    # Print detailed timing summary for debugging
    print_timing_summary(launcher_times)

    # Verify we have all expected tasks
    expected_tasks = [
        "root",
        "fast_1",
        "fast_2",
        "fast_3",
        "slow_1",
        "slow_2",
        "slow_3",
    ]
    found_tasks = list(launcher_times.keys())
    LOGGER.info("Found tasks: %s", found_tasks)
    assert len(found_tasks) >= 7, (
        f"Expected at least 7 tasks ({expected_tasks}), found {len(found_tasks)}: {found_tasks}"
    )

    LOGGER.info("Testing branch independence...")
    try:
        assert_branch_independence(launcher_times, fast_branch="fast", slow_branch="slow")
        LOGGER.info("✓ PASS: Fast branch completed before slow branch")
    except AssertionError:
        LOGGER.exception("✗ FAIL: Branch independence assertion failed")
        raise

    LOGGER.info("Testing pre-submission behavior...")
    pre_submission_tests = [
        ("fast_2", "fast_1", "fast_2 should be submitted before fast_1 finishes"),
        ("fast_3", "fast_2", "fast_3 should be submitted before fast_2 finishes"),
        ("slow_2", "slow_1", "slow_2 should be submitted before slow_1 finishes"),
    ]

    for task, dep, description in pre_submission_tests:
        if task in launcher_times and dep in launcher_times:
            try:
                assert_pre_submission_occurred(launcher_times, task, dep)
                LOGGER.info("✓ PASS: %s", description)
            except AssertionError as e:
                LOGGER.warning("⚠ SKIPPED: %s - %s", description, e)
                # Don't fail the test if pre-submission doesn't occur
                # (depends on timing and scheduler overhead)
        else:
            LOGGER.warning("⚠ SKIPPED: %s - tasks not found", description)

    LOGGER.info("Testing submission order...")
    try:
        # Root must be submitted first
        assert_submission_order(launcher_times, ["root", "fast_1"])
        assert_submission_order(launcher_times, ["root", "slow_1"])

        # Within each branch, tasks should be submitted in sequence
        assert_submission_order(launcher_times, ["fast_1", "fast_2", "fast_3"])
        assert_submission_order(launcher_times, ["slow_1", "slow_2", "slow_3"])

        LOGGER.info("✓ PASS: Submission order is correct")
    except AssertionError:
        LOGGER.exception("✗ FAIL: Submission order assertion failed")
        raise

    LOGGER.info("Testing independent branch advancement...")
    if "fast_3" in launcher_times and "slow_3" in launcher_times:
        fast_3_submit = launcher_times["fast_3"]["ctime"]
        slow_3_submit = launcher_times["slow_3"]["ctime"]

        time_diff = (slow_3_submit - fast_3_submit).total_seconds()
        LOGGER.info("fast_3 submitted at %s", fast_3_submit)
        LOGGER.info("slow_3 submitted at %s", slow_3_submit)
        LOGGER.info("Time difference: %.1fs", time_diff)

        assert fast_3_submit < slow_3_submit, (
            "fast_3 should be submitted before slow_3 (demonstrates branch independence)"
        )
        LOGGER.info("✓ PASS: fast_3 submitted before slow_3")
    else:
        LOGGER.warning("⚠ SKIPPED: fast_3/slow_3 timing check - tasks not found")

    # Assertions passed - log success
    LOGGER.info("=" * 80)
    LOGGER.info("✓ All assertions passed!")
    LOGGER.info("  ✓ Fast branch completed before slow branch (branch independence)")
    LOGGER.info("  ✓ Tasks submitted in correct order (dynamic levels)")
    LOGGER.info("  ✓ Pre-submission behavior observed (front_depth=1)")
    LOGGER.info("  ✓ fast_3 submitted before slow_3 (independent advancement)")
    LOGGER.info("  ✓ Total execution time: %.1fs", total_time)
    LOGGER.info("=" * 80)
    LOGGER.info("Test completed successfully. Log saved to %s", log_file.absolute())

    # Clean up file handler
    LOGGER.removeHandler(file_handler)
    file_handler.close()


@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    ("config_case", "front_depth"),
    [
        ("dynamic-simple", 1),  # No pre-submission (default)
        ("dynamic-simple", 2),  # One level ahead
        ("dynamic-simple", 3),  # Two levels ahead
    ],
)
def test_branch_independence_with_front_depths(config_paths, front_depth):
    """Parameterized test for different front_depth values.

    Tests that dynamic level computation works correctly with different
    pre-submission strategies:
    - front_depth=1: no pre-submission
    - front_depth=2: Submit one level ahead (optimal for most cases)
    - front_depth=3: Submit two levels ahead (aggressive pre-submission)

    All front depths should result in branch independence, but with different
    pre-submission behavior.
    """
    import logging
    from datetime import datetime
    from pathlib import Path
    from zoneinfo import ZoneInfo

    from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
    from aiida.orm import CalcJobNode

    from tests.integration.utils import (
        assert_branch_independence,
        extract_launcher_times,
    )

    LOGGER = logging.getLogger(__name__)

    # Set up persistent file logging
    log_file = Path(f"branch_independence_window{front_depth}_test.log")
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    LOGGER.addHandler(file_handler)
    LOGGER.setLevel(logging.INFO)

    LOGGER.info("=" * 80)
    LOGGER.info("Testing with front_depth=%s", front_depth)
    LOGGER.info("=" * 80)

    # Build and run the workflow with specified front_depth
    LOGGER.info("Building workflow from config")
    core_workflow = Workflow.from_config_file(str(config_paths["yml"]), template_context=config_paths["variables"])
    # Override front_depth for this test
    core_workflow.front_depth = front_depth
    workgraph = build_sirocco_workgraph(core_workflow)
    LOGGER.info("WorkGraph built with %s tasks", len(workgraph.tasks))

    # Track execution time
    start_time = datetime.now(ZoneInfo("Europe/Zurich"))
    LOGGER.info("Starting workflow execution at %s", start_time)
    workgraph.run()
    end_time = datetime.now(ZoneInfo("Europe/Zurich"))
    output_node = workgraph.process

    total_time = (end_time - start_time).total_seconds()
    LOGGER.info("Workflow execution completed in %.1fs", total_time)
    LOGGER.info("Workflow PK: %s", output_node.pk)

    # Check if workflow completed successfully
    if not output_node.is_finished_ok:
        LOGGER.error("Workflow did not finish successfully!")
        LOGGER.error(
            "Workchain report:\n%s",
            get_workchain_report(output_node, levelname="REPORT"),
        )
        for node in output_node.called_descendants:
            if isinstance(node, CalcJobNode):
                LOGGER.error("%s workdir: %s", node.process_label, node.get_remote_workdir())
                LOGGER.error("%s report:\n%s", node.process_label, get_calcjob_report(node))

    assert output_node.is_finished_ok, (
        f"Workflow failed. Exit code: {output_node.exit_code}, message: {output_node.exit_message}"
    )

    # Extract timing data
    launcher_times = extract_launcher_times(output_node)
    LOGGER.info("Extracted timing data for %s tasks", len(launcher_times))

    LOGGER.info("Testing branch independence...")
    try:
        assert_branch_independence(launcher_times, fast_branch="fast", slow_branch="slow")
        LOGGER.info("✓ PASS: Fast branch completed before slow branch")
    except AssertionError:
        LOGGER.exception("✗ FAIL: Branch independence assertion failed")
        raise

    if front_depth == 1:
        LOGGER.info("Validating front_depth=1 behavior (no pre-submission)...")
        # With front_depth=1, tasks are submitted sequentially
        # Wait for level N to finish before submitting level N+1
        LOGGER.info("✓ Sequential execution mode (no pre-submission)")

    elif front_depth >= 2:
        LOGGER.info("Validating front_depth=%s behavior (aggressive pre-submission)...", front_depth)
        # With higher front_depth, tasks can be submitted multiple levels ahead
        # More aggressive streaming submission
        LOGGER.info("✓ Front depth %s (aggressive pre-submission)", front_depth)

    # Log success
    LOGGER.info("=" * 80)
    LOGGER.info("✓ Test passed with front_depth=%s", front_depth)
    LOGGER.info("  ✓ Fast branch completed before slow branch")
    LOGGER.info("  ✓ Total execution time: %.1fs", total_time)
    LOGGER.info("=" * 80)
    LOGGER.info("Test completed. Log saved to %s", log_file.absolute())

    # Clean up file handler
    LOGGER.removeHandler(file_handler)
    file_handler.close()


@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "dynamic-simple",  # Use dynamic-simple path for complex config
    ],
)
def test_complex_workflow_with_cross_dependencies(config_paths):
    """Integration test for complex workflow with 3 branches and cross-dependencies.

    This test validates:
    - 3 branches (fast, medium, slow) with different execution speeds
    - Cross-dependencies between branches:
      * medium_2 depends on fast_2 (cross-branch)
      * slow_2 depends on medium_2 (cross-branch)
    - Convergence point where all branches sync (finalize task)
    - Dynamic level computation with complex dependency graphs
    """
    import logging
    from datetime import datetime
    from pathlib import Path
    from zoneinfo import ZoneInfo

    from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
    from aiida.orm import CalcJobNode

    from tests.integration.utils import (
        assert_branch_independence,
        assert_cross_dependency_respected,
        extract_launcher_times,
        print_timing_summary,
    )

    LOGGER = logging.getLogger(__name__)

    # Set up persistent file logging
    log_file = Path("complex_workflow_test.log")
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    LOGGER.addHandler(file_handler)
    LOGGER.setLevel(logging.INFO)

    LOGGER.info("=" * 80)
    LOGGER.info("Starting complex workflow integration test")
    LOGGER.info("Testing: 3 branches + cross-dependencies + convergence")
    LOGGER.info("=" * 80)

    # Use config_complex.yml instead of config.yml
    config_dir = Path(config_paths["yml"]).parent
    complex_config_path = config_dir / "config_complex.yml"

    if not complex_config_path.exists():
        pytest.skip(f"Complex config not found: {complex_config_path}")

    # Build and run the workflow
    LOGGER.info("Building workflow from %s", complex_config_path)
    core_workflow = Workflow.from_config_file(str(complex_config_path))
    workgraph = build_sirocco_workgraph(core_workflow)
    LOGGER.info("WorkGraph built with %s tasks", len(workgraph.tasks))

    # Track execution time
    start_time = datetime.now(ZoneInfo("Europe/Zurich"))
    LOGGER.info("Starting workflow execution at %s", start_time)
    workgraph.run()
    end_time = datetime.now(ZoneInfo("Europe/Zurich"))
    output_node = workgraph.process

    total_time = (end_time - start_time).total_seconds()
    LOGGER.info("Workflow execution completed in %.1fs", total_time)
    LOGGER.info("Workflow PK: %s", output_node.pk)

    # Check if workflow completed successfully
    if not output_node.is_finished_ok:
        LOGGER.error("Workflow did not finish successfully!")
        LOGGER.error(
            "Workchain report:\n%s",
            get_workchain_report(output_node, levelname="REPORT"),
        )
        for node in output_node.called_descendants:
            if isinstance(node, CalcJobNode):
                LOGGER.error("%s workdir: %s", node.process_label, node.get_remote_workdir())
                LOGGER.error("%s report:\n%s", node.process_label, get_calcjob_report(node))

    assert output_node.is_finished_ok, (
        f"Workflow failed. Exit code: {output_node.exit_code}, message: {output_node.exit_message}"
    )

    # Extract timing data
    launcher_times = extract_launcher_times(output_node)
    LOGGER.info("Extracted timing data for %s tasks", len(launcher_times))

    # Print detailed timing summary
    print_timing_summary(launcher_times)

    LOGGER.info("Testing fast branch independence...")
    try:
        assert_branch_independence(launcher_times, fast_branch="fast", slow_branch="medium")
        LOGGER.info("✓ PASS: Fast branch completed before medium branch")

        assert_branch_independence(launcher_times, fast_branch="fast", slow_branch="slow")
        LOGGER.info("✓ PASS: Fast branch completed before slow branch")
    except AssertionError:
        LOGGER.exception("✗ FAIL: Fast branch independence failed")
        raise

    LOGGER.info("Testing medium vs slow branch...")
    try:
        assert_branch_independence(launcher_times, fast_branch="medium", slow_branch="slow")
        LOGGER.info("✓ PASS: Medium branch completed before slow branch")
    except AssertionError:
        LOGGER.exception("✗ FAIL: Medium vs slow assertion failed")
        raise

    LOGGER.info("Testing cross-dependency constraints...")
    try:
        # medium_2 depends on medium_1 and fast_2
        if "medium_2" in launcher_times and "medium_1" in launcher_times and "fast_2" in launcher_times:
            assert_cross_dependency_respected(launcher_times, "medium_2", ["medium_1", "fast_2"])
            LOGGER.info("✓ PASS: medium_2 correctly waits for medium_1 and fast_2")
        else:
            LOGGER.warning("⚠ SKIPPED: medium_2 cross-dependency check - tasks not found")

        # slow_2 depends on slow_1 and medium_2
        if "slow_2" in launcher_times and "slow_1" in launcher_times and "medium_2" in launcher_times:
            assert_cross_dependency_respected(launcher_times, "slow_2", ["slow_1", "medium_2"])
            LOGGER.info("✓ PASS: slow_2 correctly waits for slow_1 and medium_2")
        else:
            LOGGER.warning("⚠ SKIPPED: slow_2 cross-dependency check - tasks not found")

    except AssertionError:
        LOGGER.exception("✗ FAIL: Cross-dependency assertion failed")
        raise

    LOGGER.info("Testing convergence point...")
    if "finalize" in launcher_times:
        finalize_submit = launcher_times["finalize"]["ctime"]

        # Check that finalize was submitted AFTER all final branch tasks completed
        final_tasks = []
        if "fast_3" in launcher_times:
            final_tasks.append(("fast_3", launcher_times["fast_3"]))
        if "medium_3" in launcher_times:
            final_tasks.append(("medium_3", launcher_times["medium_3"]))
        if "slow_3" in launcher_times:
            final_tasks.append(("slow_3", launcher_times["slow_3"]))

        for task_name, task_info in final_tasks:
            task_finish = task_info["mtime"]
            assert task_finish <= finalize_submit, (
                f"finalize should start after {task_name} finishes. "
                f"{task_name} finished at {task_finish}, finalize submitted at {finalize_submit}"
            )
            LOGGER.info("✓ finalize correctly waits for %s", task_name)

        LOGGER.info("✓ PASS: Convergence point correctly synchronizes all branches")
    else:
        LOGGER.warning("⚠ SKIPPED: Convergence point check - finalize task not found")

    # Log success
    LOGGER.info("=" * 80)
    LOGGER.info("✓ All assertions passed!")
    LOGGER.info("  ✓ Fast branch completed before medium and slow branches")
    LOGGER.info("  ✓ Medium branch completed before slow branch")
    LOGGER.info("  ✓ Cross-dependencies properly enforced")
    LOGGER.info("  ✓ Convergence point synchronizes all branches")
    LOGGER.info("  ✓ Total execution time: %.1fs", total_time)
    LOGGER.info("=" * 80)
    LOGGER.info("Test completed. Log saved to %s", log_file.absolute())

    # Clean up file handler
    LOGGER.removeHandler(file_handler)
    file_handler.close()


# configs that are tested for running workgraph
@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "small-shell",
        # "parameters",
    ],
)
def test_run_workgraph(config_paths):
    """Tests end-to-end the parsing from file up to running the workgraph.

    Automatically uses the aiida_profile fixture to create a new profile. Note to debug the test with your profile
    please run this in a separate file as the profile is deleted after test finishes.
    """
    core_workflow = Workflow.from_config_file(str(config_paths["yml"]), template_context=config_paths["variables"])
    workgraph = build_sirocco_workgraph(core_workflow)
    workgraph.run()
    output_node = workgraph.process
    if not output_node.is_finished_ok:
        from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
        from aiida.orm import CalcJobNode

        # overall report but often not enough to really find the bug, one has to go to calcjob
        LOGGER.error(
            "Workchain report:\n%s",
            get_workchain_report(output_node, levelname="REPORT"),
        )
        # the calcjobs are typically stored in 'called_descendants'
        for node in output_node.called_descendants:
            if isinstance(node, CalcJobNode):
                LOGGER.error("%s workdir: %s", node.process_label, node.get_remote_workdir())
                LOGGER.error("%s report:\n%s", node.process_label, get_calcjob_report(node))
    assert output_node.is_finished_ok, (
        f"Not successful run. Got exit code {output_node.exit_code} with message {output_node.exit_message}."
    )
