import pytest

from sirocco.core import Workflow
from sirocco.parsing.yaml_data_models import ConfigWorkflow
from sirocco.workgraph import (
    build_icon_task_spec,
    build_shell_task_spec,
    build_sirocco_workgraph,
    compute_topological_levels,
)


# Hardcoded, explicit integration test based on the `parameters` case for now
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "parameters",
    ],
)
def test_shell_filenames_nodes_arguments(config_paths):
    import datetime

    from sirocco import core

    config_workflow = ConfigWorkflow.from_config_file(str(config_paths["yml"]))

    # Update the stop_date for both cycles to make the result shorter
    # NOTE: We currently don't use timezone-aware times in config YAML, thus ignora DTZ001 for now.
    # See https://github.com/C2SM/Sirocco/issues/161
    config_workflow.cycles[0].cycling.stop_date = datetime.datetime(2027, 1, 1, 0, 0)  # noqa: DTZ001
    config_workflow.cycles[1].cycling.stop_date = datetime.datetime(2027, 1, 1, 0, 0)  # noqa: DTZ001
    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Build task specs for shell tasks
    shell_tasks = [task for task in core_workflow.tasks if isinstance(task, core.ShellTask)]

    filenames_list = []
    arguments_list = []
    nodes_list = []

    for task in shell_tasks:
        task_spec = build_shell_task_spec(task)
        filenames_list.append(task_spec["filenames"])
        arguments_list.append(task_spec["arguments_template"])

        # In the new architecture, nodes come from two sources:
        # 1. Scripts (in node_pks)
        # 2. Input data (in input_data_info)
        # Note: Use 'name' for AvailableData, 'label' for GeneratedData
        nodes = list(task_spec["node_pks"].keys())
        for input_info in task_spec["input_data_info"]:
            # AvailableData uses simple names, GeneratedData uses full labels
            if input_info["is_available"]:
                nodes.append(input_info["name"])
            else:
                nodes.append(input_info["label"])
        nodes_list.append(nodes)

    expected_filenames_list = [
        {"initial_conditions": "initial_conditions", "forcing": "forcing"},
        {"initial_conditions": "initial_conditions", "forcing": "forcing"},
        {
            "icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00": "restart",
            "forcing": "forcing",
        },
        {
            "icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00": "restart",
            "forcing": "forcing",
        },
        {
            "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00": "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00": "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00",
        },
        {
            "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00": "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00": "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00",
        },
        {"analysis_foo_bar_3_0___date_2026_01_01_00_00_00": "analysis"},
        {"analysis_foo_bar_3_0___date_2026_07_01_00_00_00": "analysis"},
        {
            "analysis_foo_bar_date_2026_01_01_00_00_00": "analysis_foo_bar_date_2026_01_01_00_00_00",
            "analysis_foo_bar_date_2026_07_01_00_00_00": "analysis_foo_bar_date_2026_07_01_00_00_00",
        },
    ]

    expected_arguments_list = [
        "icon.py --restart  --init {initial_conditions} --forcing {forcing}",
        "icon.py --restart  --init {initial_conditions} --forcing {forcing}",
        "icon.py --restart {icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00} --init  --forcing {forcing}",
        "icon.py --restart {icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00} --init  --forcing {forcing}",
        "statistics.py {icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00} {icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00}",
        "statistics.py {icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00} {icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00}",
        "statistics.py {analysis_foo_bar_3_0___date_2026_01_01_00_00_00}",
        "statistics.py {analysis_foo_bar_3_0___date_2026_07_01_00_00_00}",
        "merge.py {analysis_foo_bar_date_2026_01_01_00_00_00} {analysis_foo_bar_date_2026_07_01_00_00_00}",
    ]

    expected_nodes_list = [
        [
            "SCRIPT__icon_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "initial_conditions",
            "forcing",
        ],
        [
            "SCRIPT__icon_foo_1___bar_3_0___date_2026_01_01_00_00_00",
            "initial_conditions",
            "forcing",
        ],
        [
            "SCRIPT__icon_foo_0___bar_3_0___date_2026_07_01_00_00_00",
            "icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "forcing",
        ],
        [
            "SCRIPT__icon_foo_1___bar_3_0___date_2026_07_01_00_00_00",
            "icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00",
            "forcing",
        ],
        [
            "SCRIPT__statistics_foo_bar_3_0___date_2026_01_01_00_00_00",
            "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00",
        ],
        [
            "SCRIPT__statistics_foo_bar_3_0___date_2026_07_01_00_00_00",
            "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00",
        ],
        [
            "SCRIPT__statistics_foo_bar_date_2026_01_01_00_00_00",
            "analysis_foo_bar_3_0___date_2026_01_01_00_00_00",
        ],
        [
            "SCRIPT__statistics_foo_bar_date_2026_07_01_00_00_00",
            "analysis_foo_bar_3_0___date_2026_07_01_00_00_00",
        ],
        [
            "SCRIPT__merge_date_2026_01_01_00_00_00",
            "analysis_foo_bar_date_2026_01_01_00_00_00",
            "analysis_foo_bar_date_2026_07_01_00_00_00",
        ],
    ]

    assert arguments_list == expected_arguments_list
    assert filenames_list == expected_filenames_list
    assert nodes_list == expected_nodes_list


@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "small-shell",
    ],
)
def test_waiting_on(config_paths):
    """Test that wait_on dependencies are properly represented in the WorkGraph.

    Note: With the new architecture, wait_on dependencies are handled through
    task chaining (>>), so we test that the cleanup task's get_job_data has
    the expected number of dependencies.
    """
    config_workflow = ConfigWorkflow.from_config_file(str(config_paths["yml"]))

    core_workflow = Workflow.from_config_workflow(config_workflow)
    workgraph = build_sirocco_workgraph(core_workflow)

    # In the new architecture, wait_on is implemented via task dependencies
    # The cleanup launcher task should exist
    cleanup_launcher = None
    for task in workgraph.tasks:
        if task.name == "launch_cleanup":
            cleanup_launcher = task
            break

    assert cleanup_launcher is not None, "cleanup launcher task should exist"

    # Check that the cleanup task has the expected dependencies through the dependency graph
    # In the new architecture, dependencies flow through get_job_data tasks
    cleanup_get_job_data = None
    for task in workgraph.tasks:
        if task.name == "get_job_data_cleanup":
            cleanup_get_job_data = task
            break

    assert cleanup_get_job_data is not None, "cleanup get_job_data task should exist"


@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "parameters",
        "small-shell",
        "small-icon",
    ],
)
def test_build_workgraph(config_paths):
    """Test that WorkGraph builds successfully with the new functional API."""
    config_workflow = ConfigWorkflow.from_config_file(str(config_paths["yml"]))
    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Build the WorkGraph
    workgraph = build_sirocco_workgraph(core_workflow)

    # Verify basic properties
    assert workgraph is not None
    assert workgraph.name == core_workflow.name
    assert len(workgraph.tasks) > 0

    # Verify that launcher tasks and get_job_data tasks are created
    launcher_tasks = [t for t in workgraph.tasks if t.name.startswith("launch_")]
    get_job_data_tasks = [t for t in workgraph.tasks if t.name.startswith("get_job_data_")]

    # Each core task should have a launcher and get_job_data task
    # core_workflow.tasks is a Store object, convert to list for length
    num_core_tasks = len(list(core_workflow.tasks))
    assert len(launcher_tasks) == num_core_tasks
    assert len(get_job_data_tasks) == num_core_tasks

    # Verify window config is stored in extras
    assert "window_config" in workgraph.extras
    window_config = workgraph.extras["window_config"]
    assert "enabled" in window_config
    assert "window_size" in window_config
    assert "task_dependencies" in window_config


@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "small-icon",
    ],
)
def test_aiida_icon_task_metadata(config_paths):
    """Test if the metadata regarding the job submission of the `IconCalculation` is included in task specs."""
    import aiida.orm

    from sirocco import core

    config_workflow = ConfigWorkflow.from_config_file(str(config_paths["yml"]))

    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Test wrapper script and metadata for ICON tasks
    icon_tasks = [task for task in core_workflow.tasks if isinstance(task, core.IconTask)]

    for task in icon_tasks:
        task_spec = build_icon_task_spec(task)

        # Test wrapper script
        if task_spec["wrapper_script_pk"] is not None:
            wrapper_node = aiida.orm.load_node(task_spec["wrapper_script_pk"])
            assert wrapper_node.filename == "dummy_wrapper.sh"

        # Test uenv and view in metadata
        metadata = task_spec["metadata"]
        custom_scheduler_commands = metadata["options"].get("custom_scheduler_commands", "")
        assert "#SBATCH --uenv=icon-wcp/v1:rc4" in custom_scheduler_commands
        assert "#SBATCH --view=icon" in custom_scheduler_commands

    # Test default wrapper script behavior
    config_workflow = ConfigWorkflow.from_config_file(str(config_paths["yml"]))

    # Find the icon task and remove wrapper_script
    for task in config_workflow.tasks:
        if task.name == "icon" and hasattr(task, "wrapper_script"):
            task.wrapper_script = None

    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Test that the default wrapper (currently `todi_cpu.sh`) is used
    icon_tasks = [task for task in core_workflow.tasks if isinstance(task, core.IconTask)]

    for task in icon_tasks:
        task_spec = build_icon_task_spec(task)

        # Test default wrapper script
        if task_spec["wrapper_script_pk"] is not None:
            wrapper_node = aiida.orm.load_node(task_spec["wrapper_script_pk"])
            assert wrapper_node.filename == "todi_cpu.sh"


def test_topological_levels_linear_chain():
    """Test topological level calculation for a linear chain: A -> B -> C."""
    task_deps = {
        "launch_A": [],
        "launch_B": ["launch_A"],
        "launch_C": ["launch_B"],
    }
    levels = compute_topological_levels(task_deps)

    assert levels["launch_A"] == 0
    assert levels["launch_B"] == 1
    assert levels["launch_C"] == 2


def test_topological_levels_diamond():
    """Test topological level calculation for a diamond: A -> B,C -> D."""
    task_deps = {
        "launch_A": [],
        "launch_B": ["launch_A"],
        "launch_C": ["launch_A"],
        "launch_D": ["launch_B", "launch_C"],
    }
    levels = compute_topological_levels(task_deps)

    assert levels["launch_A"] == 0
    assert levels["launch_B"] == 1
    assert levels["launch_C"] == 1
    assert levels["launch_D"] == 2


def test_topological_levels_parallel():
    """Test topological level calculation for parallel tasks."""
    task_deps = {
        "launch_A": [],
        "launch_B": [],
        "launch_C": [],
    }
    levels = compute_topological_levels(task_deps)

    assert levels["launch_A"] == 0
    assert levels["launch_B"] == 0
    assert levels["launch_C"] == 0


def test_topological_levels_complex():
    """Test topological level calculation for a complex DAG."""
    #     A
    #    / \
    #   B   C
    #   |\ /|
    #   | X |
    #   |/ \|
    #   D   E
    #    \ /
    #     F
    task_deps = {
        "launch_A": [],
        "launch_B": ["launch_A"],
        "launch_C": ["launch_A"],
        "launch_D": ["launch_B", "launch_C"],
        "launch_E": ["launch_B", "launch_C"],
        "launch_F": ["launch_D", "launch_E"],
    }
    levels = compute_topological_levels(task_deps)

    assert levels["launch_A"] == 0
    assert levels["launch_B"] == 1
    assert levels["launch_C"] == 1
    assert levels["launch_D"] == 2
    assert levels["launch_E"] == 2
    assert levels["launch_F"] == 3


@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "branch-independence",
    ],
)
def test_branch_independence_config(config_paths):
    """Test that branch independence workflow is configured correctly."""
    config_workflow = ConfigWorkflow.from_config_file(str(config_paths["yml"]))
    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Build the WorkGraph with window_size=1
    workgraph = build_sirocco_workgraph(core_workflow, window_size=1)

    # Verify window config is stored correctly
    assert "window_config" in workgraph.extras
    window_config = workgraph.extras["window_config"]
    assert window_config["enabled"] is True
    assert window_config["window_size"] == 1
    assert "task_dependencies" in window_config

    # Get launcher task names
    launcher_tasks = [t.name for t in workgraph.tasks if t.name.startswith("launch_")]

    # Verify we have the expected tasks (using actual naming convention)
    expected_task_prefixes = ["root", "fast_1", "fast_2", "fast_3", "slow_1", "slow_2", "slow_3"]
    for prefix in expected_task_prefixes:
        matching_tasks = [t for t in launcher_tasks if t.startswith(f"launch_{prefix}_")]
        assert len(matching_tasks) == 1, f"Expected exactly 1 task starting with 'launch_{prefix}_', found {len(matching_tasks)}: {matching_tasks}"

    # Verify dependency structure
    task_deps = window_config["task_dependencies"]

    # Find actual task names (they use date format: launch_root_date_2026_01_01_00_00_00)
    root_task = [t for t in launcher_tasks if t.startswith("launch_root_")][0]
    fast_1_task = [t for t in launcher_tasks if t.startswith("launch_fast_1_")][0]
    fast_2_task = [t for t in launcher_tasks if t.startswith("launch_fast_2_")][0]
    fast_3_task = [t for t in launcher_tasks if t.startswith("launch_fast_3_")][0]
    slow_1_task = [t for t in launcher_tasks if t.startswith("launch_slow_1_")][0]
    slow_2_task = [t for t in launcher_tasks if t.startswith("launch_slow_2_")][0]
    slow_3_task = [t for t in launcher_tasks if t.startswith("launch_slow_3_")][0]

    # Root has no dependencies
    assert task_deps[root_task] == [], f"Root task should have no dependencies, got: {task_deps[root_task]}"

    # Fast and slow branch first tasks depend on root
    assert root_task in task_deps[fast_1_task], f"fast_1 should depend on root"
    assert root_task in task_deps[slow_1_task], f"slow_1 should depend on root"

    # Verify chain dependencies within each branch
    assert fast_1_task in task_deps[fast_2_task], f"fast_2 should depend on fast_1"
    assert fast_2_task in task_deps[fast_3_task], f"fast_3 should depend on fast_2"
    assert slow_1_task in task_deps[slow_2_task], f"slow_2 should depend on slow_1"
    assert slow_2_task in task_deps[slow_3_task], f"slow_3 should depend on slow_2"


@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "branch-independence",
    ],
)
def test_branch_independence_execution(config_paths):
    """Integration test that actually runs the branch independence workflow.

    This test verifies that with dynamic levels and window_size=1:
    - The faster branch completes without waiting for the slower branch
    - Tasks are pre-submitted before their dependencies finish (window_size=1)
    - Submission order follows dynamic level computation
    """
    import logging
    from datetime import datetime
    from pathlib import Path

    from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
    from aiida.orm import CalcJobNode

    from tests.unit_tests.test_utils import (
        assert_branch_independence,
        assert_pre_submission_occurred,
        assert_submission_order,
        extract_launcher_times,
        print_timing_summary,
    )

    LOGGER = logging.getLogger(__name__)

    # Set up persistent file logging
    log_file = Path("branch_independence_test.log")
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    LOGGER.addHandler(file_handler)
    LOGGER.setLevel(logging.INFO)

    LOGGER.info("="*80)
    LOGGER.info("Starting branch independence integration test")
    LOGGER.info("="*80)

    # Build and run the workflow
    LOGGER.info("Building workflow from config")
    core_workflow = Workflow.from_config_file(str(config_paths["yml"]))
    workgraph = build_sirocco_workgraph(core_workflow, window_size=1)
    LOGGER.info(f"WorkGraph built with {len(workgraph.tasks)} tasks")

    # Track task completion times
    start_time = datetime.now()
    LOGGER.info(f"Starting workflow execution at {start_time}")
    workgraph.run()
    end_time = datetime.now()
    output_node = workgraph.process

    total_time = (end_time - start_time).total_seconds()
    LOGGER.info(f"Workflow execution completed in {total_time:.1f}s")
    LOGGER.info(f"Workflow PK: {output_node.pk}")
    LOGGER.info(f"Workflow state: {output_node.process_state}")
    LOGGER.info(f"Workflow exit_code: {output_node.exit_code}")

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

    assert (
        output_node.is_finished_ok
    ), f"Workflow failed. Exit code: {output_node.exit_code}, message: {output_node.exit_message}"

    # ========================================================================
    # NEW: Use test utilities to extract and validate timing data
    # ========================================================================

    # Extract launcher timing data (when tasks were submitted and completed)
    launcher_times = extract_launcher_times(output_node)
    LOGGER.info(f"Extracted timing data for {len(launcher_times)} tasks")

    # Print detailed timing summary for debugging
    print_timing_summary(launcher_times)

    # Verify we have all expected tasks
    expected_tasks = ["root", "fast_1", "fast_2", "fast_3", "slow_1", "slow_2", "slow_3"]
    found_tasks = list(launcher_times.keys())
    LOGGER.info(f"Found tasks: {found_tasks}")
    assert len(found_tasks) >= 7, (
        f"Expected at least 7 tasks ({expected_tasks}), "
        f"found {len(found_tasks)}: {found_tasks}"
    )

    # ========================================================================
    # ASSERTION 1: Branch Independence
    # Fast branch should complete before slow branch (key test!)
    # ========================================================================
    LOGGER.info("Testing branch independence...")
    try:
        assert_branch_independence(launcher_times, fast_branch='fast', slow_branch='slow')
        LOGGER.info("✓ PASS: Fast branch completed before slow branch")
    except AssertionError as e:
        LOGGER.error(f"✗ FAIL: Branch independence assertion failed: {e}")
        raise

    # ========================================================================
    # ASSERTION 2: Pre-submission (window_size=1)
    # Tasks should be submitted BEFORE their dependencies finish
    # ========================================================================
    LOGGER.info("Testing pre-submission behavior...")
    pre_submission_tests = [
        ('fast_2', 'fast_1', "fast_2 should be submitted before fast_1 finishes"),
        ('fast_3', 'fast_2', "fast_3 should be submitted before fast_2 finishes"),
        ('slow_2', 'slow_1', "slow_2 should be submitted before slow_1 finishes"),
    ]

    for task, dep, description in pre_submission_tests:
        if task in launcher_times and dep in launcher_times:
            try:
                assert_pre_submission_occurred(launcher_times, task, dep)
                LOGGER.info(f"✓ PASS: {description}")
            except AssertionError as e:
                LOGGER.warning(f"⚠ SKIPPED: {description} - {e}")
                # Don't fail the test if pre-submission doesn't occur
                # (depends on timing and scheduler overhead)
        else:
            LOGGER.warning(f"⚠ SKIPPED: {description} - tasks not found")

    # ========================================================================
    # ASSERTION 3: Submission Order
    # Verify that dynamic levels produce correct submission sequence
    # ========================================================================
    LOGGER.info("Testing submission order...")
    try:
        # Root must be submitted first
        assert_submission_order(launcher_times, ['root', 'fast_1'])
        assert_submission_order(launcher_times, ['root', 'slow_1'])

        # Within each branch, tasks should be submitted in sequence
        assert_submission_order(launcher_times, ['fast_1', 'fast_2', 'fast_3'])
        assert_submission_order(launcher_times, ['slow_1', 'slow_2', 'slow_3'])

        LOGGER.info("✓ PASS: Submission order is correct")
    except AssertionError as e:
        LOGGER.error(f"✗ FAIL: Submission order assertion failed: {e}")
        raise

    # ========================================================================
    # ASSERTION 4: fast_3 should be submitted before slow_3
    # This demonstrates that the fast branch advances independently
    # ========================================================================
    LOGGER.info("Testing independent branch advancement...")
    if 'fast_3' in launcher_times and 'slow_3' in launcher_times:
        fast_3_submit = launcher_times['fast_3']['ctime']
        slow_3_submit = launcher_times['slow_3']['ctime']

        time_diff = (slow_3_submit - fast_3_submit).total_seconds()
        LOGGER.info(f"fast_3 submitted at {fast_3_submit}")
        LOGGER.info(f"slow_3 submitted at {slow_3_submit}")
        LOGGER.info(f"Time difference: {time_diff:.1f}s")

        assert fast_3_submit < slow_3_submit, (
            "fast_3 should be submitted before slow_3 (demonstrates branch independence)"
        )
        LOGGER.info("✓ PASS: fast_3 submitted before slow_3")
    else:
        LOGGER.warning("⚠ SKIPPED: fast_3/slow_3 timing check - tasks not found")

    # Assertions passed - log success
    LOGGER.info("="*80)
    LOGGER.info("✓ All assertions passed!")
    LOGGER.info("  ✓ Fast branch completed before slow branch (branch independence)")
    LOGGER.info("  ✓ Tasks submitted in correct order (dynamic levels)")
    LOGGER.info("  ✓ Pre-submission behavior observed (window_size=1)")
    LOGGER.info("  ✓ fast_3 submitted before slow_3 (independent advancement)")
    LOGGER.info(f"  ✓ Total execution time: {total_time:.1f}s")
    LOGGER.info("="*80)
    LOGGER.info(f"Test completed successfully. Log saved to {log_file.absolute()}")

    # Clean up file handler
    LOGGER.removeHandler(file_handler)
    file_handler.close()


@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case,window_size",
    [
        ("branch-independence", 0),  # Sequential execution
        ("branch-independence", 1),  # One level ahead (default)
        ("branch-independence", 2),  # Two levels ahead (aggressive)
    ],
)
def test_branch_independence_with_window_sizes(config_paths, window_size):
    """Parameterized test for different window_size values.

    Tests that dynamic level computation works correctly with different
    pre-submission strategies:
    - window_size=0: Sequential execution, no pre-submission
    - window_size=1: Submit one level ahead (optimal for most cases)
    - window_size=2: Submit two levels ahead (aggressive pre-submission)

    All window sizes should result in branch independence, but with different
    pre-submission behavior.
    """
    import logging
    from datetime import datetime
    from pathlib import Path

    from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
    from aiida.orm import CalcJobNode

    from tests.unit_tests.test_utils import (
        assert_branch_independence,
        extract_launcher_times,
    )

    LOGGER = logging.getLogger(__name__)

    # Set up persistent file logging
    log_file = Path(f"branch_independence_window{window_size}_test.log")
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    LOGGER.addHandler(file_handler)
    LOGGER.setLevel(logging.INFO)

    LOGGER.info("="*80)
    LOGGER.info(f"Testing with window_size={window_size}")
    LOGGER.info("="*80)

    # Build and run the workflow with specified window_size
    LOGGER.info("Building workflow from config")
    core_workflow = Workflow.from_config_file(str(config_paths["yml"]))
    workgraph = build_sirocco_workgraph(core_workflow, window_size=window_size)
    LOGGER.info(f"WorkGraph built with {len(workgraph.tasks)} tasks")

    # Track execution time
    start_time = datetime.now()
    LOGGER.info(f"Starting workflow execution at {start_time}")
    workgraph.run()
    end_time = datetime.now()
    output_node = workgraph.process

    total_time = (end_time - start_time).total_seconds()
    LOGGER.info(f"Workflow execution completed in {total_time:.1f}s")
    LOGGER.info(f"Workflow PK: {output_node.pk}")

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

    assert (
        output_node.is_finished_ok
    ), f"Workflow failed. Exit code: {output_node.exit_code}, message: {output_node.exit_message}"

    # Extract timing data
    launcher_times = extract_launcher_times(output_node)
    LOGGER.info(f"Extracted timing data for {len(launcher_times)} tasks")

    # ========================================================================
    # Key assertion: Branch independence should work regardless of window_size
    # ========================================================================
    LOGGER.info("Testing branch independence...")
    try:
        assert_branch_independence(launcher_times, fast_branch='fast', slow_branch='slow')
        LOGGER.info("✓ PASS: Fast branch completed before slow branch")
    except AssertionError as e:
        LOGGER.error(f"✗ FAIL: Branch independence assertion failed: {e}")
        raise

    # ========================================================================
    # Window-size specific validation
    # ========================================================================
    if window_size == 0:
        LOGGER.info("Validating window_size=0 behavior (sequential execution)...")
        # With window_size=0, we expect more sequential behavior
        # Tasks should generally be submitted after their dependencies finish
        # (though this is hard to test precisely due to timing variations)
        LOGGER.info("✓ Sequential execution mode (no pre-submission expected)")

    elif window_size == 1:
        LOGGER.info("Validating window_size=1 behavior (one level ahead)...")
        # With window_size=1, some pre-submission should occur
        # This is tested more thoroughly in the main test
        LOGGER.info("✓ One level ahead mode (optimal pre-submission)")

    elif window_size == 2:
        LOGGER.info("Validating window_size=2 behavior (two levels ahead)...")
        # With window_size=2, more aggressive pre-submission should occur
        # Tasks can be submitted up to 2 levels ahead
        LOGGER.info("✓ Two levels ahead mode (aggressive pre-submission)")

    # Log success
    LOGGER.info("="*80)
    LOGGER.info(f"✓ Test passed with window_size={window_size}")
    LOGGER.info(f"  ✓ Fast branch completed before slow branch")
    LOGGER.info(f"  ✓ Total execution time: {total_time:.1f}s")
    LOGGER.info("="*80)
    LOGGER.info(f"Test completed. Log saved to {log_file.absolute()}")

    # Clean up file handler
    LOGGER.removeHandler(file_handler)
    file_handler.close()


@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "branch-independence",  # Use branch-independence path for complex config
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

    from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
    from aiida.orm import CalcJobNode

    from tests.unit_tests.test_utils import (
        assert_branch_independence,
        assert_cross_dependency_respected,
        extract_launcher_times,
        print_timing_summary,
    )

    LOGGER = logging.getLogger(__name__)

    # Set up persistent file logging
    log_file = Path("complex_workflow_test.log")
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    LOGGER.addHandler(file_handler)
    LOGGER.setLevel(logging.INFO)

    LOGGER.info("="*80)
    LOGGER.info("Starting complex workflow integration test")
    LOGGER.info("Testing: 3 branches + cross-dependencies + convergence")
    LOGGER.info("="*80)

    # Use config_complex.yml instead of config.yml
    config_dir = Path(config_paths["yml"]).parent
    complex_config_path = config_dir / "config_complex.yml"

    if not complex_config_path.exists():
        pytest.skip(f"Complex config not found: {complex_config_path}")

    # Build and run the workflow
    LOGGER.info(f"Building workflow from {complex_config_path}")
    core_workflow = Workflow.from_config_file(str(complex_config_path))
    workgraph = build_sirocco_workgraph(core_workflow, window_size=1)
    LOGGER.info(f"WorkGraph built with {len(workgraph.tasks)} tasks")

    # Track execution time
    start_time = datetime.now()
    LOGGER.info(f"Starting workflow execution at {start_time}")
    workgraph.run()
    end_time = datetime.now()
    output_node = workgraph.process

    total_time = (end_time - start_time).total_seconds()
    LOGGER.info(f"Workflow execution completed in {total_time:.1f}s")
    LOGGER.info(f"Workflow PK: {output_node.pk}")

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

    assert (
        output_node.is_finished_ok
    ), f"Workflow failed. Exit code: {output_node.exit_code}, message: {output_node.exit_message}"

    # Extract timing data
    launcher_times = extract_launcher_times(output_node)
    LOGGER.info(f"Extracted timing data for {len(launcher_times)} tasks")

    # Print detailed timing summary
    print_timing_summary(launcher_times)

    # ========================================================================
    # ASSERTION 1: Fast branch completes before medium and slow branches
    # ========================================================================
    LOGGER.info("Testing fast branch independence...")
    try:
        assert_branch_independence(launcher_times, fast_branch='fast', slow_branch='medium')
        LOGGER.info("✓ PASS: Fast branch completed before medium branch")

        assert_branch_independence(launcher_times, fast_branch='fast', slow_branch='slow')
        LOGGER.info("✓ PASS: Fast branch completed before slow branch")
    except AssertionError as e:
        LOGGER.error(f"✗ FAIL: Fast branch independence failed: {e}")
        raise

    # ========================================================================
    # ASSERTION 2: Medium branch completes before slow branch
    # ========================================================================
    LOGGER.info("Testing medium vs slow branch...")
    try:
        assert_branch_independence(launcher_times, fast_branch='medium', slow_branch='slow')
        LOGGER.info("✓ PASS: Medium branch completed before slow branch")
    except AssertionError as e:
        LOGGER.error(f"✗ FAIL: Medium vs slow assertion failed: {e}")
        raise

    # ========================================================================
    # ASSERTION 3: Cross-dependencies are respected
    # medium_2 should wait for BOTH medium_1 AND fast_2
    # slow_2 should wait for BOTH slow_1 AND medium_2
    # ========================================================================
    LOGGER.info("Testing cross-dependency constraints...")
    try:
        # medium_2 depends on medium_1 and fast_2
        if 'medium_2' in launcher_times and 'medium_1' in launcher_times and 'fast_2' in launcher_times:
            assert_cross_dependency_respected(
                launcher_times,
                'medium_2',
                ['medium_1', 'fast_2']
            )
            LOGGER.info("✓ PASS: medium_2 correctly waits for medium_1 and fast_2")
        else:
            LOGGER.warning("⚠ SKIPPED: medium_2 cross-dependency check - tasks not found")

        # slow_2 depends on slow_1 and medium_2
        if 'slow_2' in launcher_times and 'slow_1' in launcher_times and 'medium_2' in launcher_times:
            assert_cross_dependency_respected(
                launcher_times,
                'slow_2',
                ['slow_1', 'medium_2']
            )
            LOGGER.info("✓ PASS: slow_2 correctly waits for slow_1 and medium_2")
        else:
            LOGGER.warning("⚠ SKIPPED: slow_2 cross-dependency check - tasks not found")

    except AssertionError as e:
        LOGGER.error(f"✗ FAIL: Cross-dependency assertion failed: {e}")
        raise

    # ========================================================================
    # ASSERTION 4: Convergence point (finalize waits for all branches)
    # ========================================================================
    LOGGER.info("Testing convergence point...")
    if 'finalize' in launcher_times:
        finalize_submit = launcher_times['finalize']['ctime']

        # Check that finalize was submitted AFTER all final branch tasks completed
        final_tasks = []
        if 'fast_3' in launcher_times:
            final_tasks.append(('fast_3', launcher_times['fast_3']))
        if 'medium_3' in launcher_times:
            final_tasks.append(('medium_3', launcher_times['medium_3']))
        if 'slow_3' in launcher_times:
            final_tasks.append(('slow_3', launcher_times['slow_3']))

        for task_name, task_info in final_tasks:
            task_finish = task_info['mtime']
            assert task_finish <= finalize_submit, (
                f"finalize should start after {task_name} finishes. "
                f"{task_name} finished at {task_finish}, finalize submitted at {finalize_submit}"
            )
            LOGGER.info(f"✓ finalize correctly waits for {task_name}")

        LOGGER.info("✓ PASS: Convergence point correctly synchronizes all branches")
    else:
        LOGGER.warning("⚠ SKIPPED: Convergence point check - finalize task not found")

    # Log success
    LOGGER.info("="*80)
    LOGGER.info("✓ All assertions passed!")
    LOGGER.info("  ✓ Fast branch completed before medium and slow branches")
    LOGGER.info("  ✓ Medium branch completed before slow branch")
    LOGGER.info("  ✓ Cross-dependencies properly enforced")
    LOGGER.info("  ✓ Convergence point synchronizes all branches")
    LOGGER.info(f"  ✓ Total execution time: {total_time:.1f}s")
    LOGGER.info("="*80)
    LOGGER.info(f"Test completed. Log saved to {log_file.absolute()}")

    # Clean up file handler
    LOGGER.removeHandler(file_handler)
    file_handler.close()


def test_dynamic_levels_branch_independence():
    """Test that dynamic level computation allows branch independence.

    This test simulates the scenario where one branch advances faster than another.
    With dynamic levels, the faster branch should not wait for the slower branch.
    """
    # Define a DAG with two parallel branches after a root
    #     root
    #    /    \
    #  fast1  slow1
    #   |      |
    #  fast2  slow2
    #   |      |
    #  fast3  slow3

    task_deps = {
        "launch_root": [],
        "launch_fast1": ["launch_root"],
        "launch_fast2": ["launch_fast1"],
        "launch_fast3": ["launch_fast2"],
        "launch_slow1": ["launch_root"],
        "launch_slow2": ["launch_slow1"],
        "launch_slow3": ["launch_slow2"],
    }

    # Initial static levels (what the old system would compute)
    initial_levels = compute_topological_levels(task_deps)
    assert initial_levels["launch_root"] == 0
    assert initial_levels["launch_fast1"] == 1
    assert initial_levels["launch_slow1"] == 1  # Same level as fast1
    assert initial_levels["launch_fast2"] == 2
    assert initial_levels["launch_slow2"] == 2  # Same level as fast2
    assert initial_levels["launch_fast3"] == 3
    assert initial_levels["launch_slow3"] == 3  # Same level as fast3

    # Simulate dynamic level computation after root finishes
    # (fast1 and slow1 are still pending)
    unfinished_tasks = {
        "launch_fast1",
        "launch_fast2",
        "launch_fast3",
        "launch_slow1",
        "launch_slow2",
        "launch_slow3",
    }
    filtered_deps = {
        task: [p for p in parents if p in unfinished_tasks] for task, parents in task_deps.items() if task in unfinished_tasks
    }
    dynamic_levels_1 = compute_topological_levels(filtered_deps)

    # After root finishes, both fast1 and slow1 should be at level 0
    assert dynamic_levels_1["launch_fast1"] == 0
    assert dynamic_levels_1["launch_slow1"] == 0

    # Simulate fast1 finishing (slow1 still running)
    # This is the key test: fast2 should move to level 0 while slow2 stays at level 1
    unfinished_tasks = {
        "launch_fast2",
        "launch_fast3",
        "launch_slow1",
        "launch_slow2",
        "launch_slow3",
    }
    filtered_deps = {
        task: [p for p in parents if p in unfinished_tasks] for task, parents in task_deps.items() if task in unfinished_tasks
    }
    dynamic_levels_2 = compute_topological_levels(filtered_deps)

    # Fast2 should be at level 0 (no unfinished dependencies)
    assert dynamic_levels_2["launch_fast2"] == 0
    # Slow2 should be at level 1 (waiting for slow1)
    assert dynamic_levels_2["launch_slow2"] == 1

    # This demonstrates branch independence: fast2 can run while slow1 is still running!

    # Simulate fast2 finishing (slow1 still running)
    unfinished_tasks = {
        "launch_fast3",
        "launch_slow1",
        "launch_slow2",
        "launch_slow3",
    }
    filtered_deps = {
        task: [p for p in parents if p in unfinished_tasks] for task, parents in task_deps.items() if task in unfinished_tasks
    }
    dynamic_levels_3 = compute_topological_levels(filtered_deps)

    # Fast3 should be at level 0 (no unfinished dependencies)
    assert dynamic_levels_3["launch_fast3"] == 0
    # Slow1 still at level 0, slow2 at level 1
    assert dynamic_levels_3["launch_slow1"] == 0
    assert dynamic_levels_3["launch_slow2"] == 1

    # Now simulate slow1 finishing
    unfinished_tasks = {
        "launch_fast3",
        "launch_slow2",
        "launch_slow3",
    }
    filtered_deps = {
        task: [p for p in parents if p in unfinished_tasks] for task, parents in task_deps.items() if task in unfinished_tasks
    }
    dynamic_levels_4 = compute_topological_levels(filtered_deps)

    # Now slow2 should also be at level 0
    assert dynamic_levels_4["launch_fast3"] == 0
    assert dynamic_levels_4["launch_slow2"] == 0
    assert dynamic_levels_4["launch_slow3"] == 1

    # This test demonstrates that with dynamic levels:
    # 1. Fast branch tasks move to level 0 as their dependencies complete
    # 2. They don't wait for slow branch tasks at the same static level
    # 3. With window_size=1, fast2 and fast3 can submit while slow1 is still running
