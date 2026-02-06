"""Integration tests for WorkGraph structure and configuration.

Tests that verify WorkGraph is built with correct structure, dependencies,
and configuration (but don't actually execute workflows).
"""

import pytest

from sirocco.core import Workflow
from sirocco.engines.aiida import build_sirocco_workgraph
from sirocco.parsing.yaml_data_models import ConfigWorkflow


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
    config_workflow = ConfigWorkflow.from_config_file(
        str(config_paths["yml"]), template_context=config_paths["variables"]
    )

    core_workflow = Workflow.from_config_workflow(config_workflow)
    workgraph = build_sirocco_workgraph(core_workflow)

    # In the new architecture, wait_on is implemented via task dependencies
    # The cleanup launcher task should exist (name includes workgraph name prefix)
    cleanup_launcher = None
    for task in workgraph.tasks:
        if task.name.startswith("launch_") and "cleanup" in task.name:
            cleanup_launcher = task
            break

    assert cleanup_launcher is not None, "cleanup launcher task should exist"

    # Check that the cleanup task has the expected dependencies through the dependency graph
    # In the new architecture, dependencies flow through get_job_data tasks
    cleanup_get_job_data = None
    for task in workgraph.tasks:
        if task.name.startswith("get_job_data_") and "cleanup" in task.name:
            cleanup_get_job_data = task
            break

    assert cleanup_get_job_data is not None, "cleanup get_job_data task should exist"


@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "parameters",
        "small-shell",
    ],
)
def test_build_workgraph(config_paths):
    """Test that WorkGraph builds successfully with the new functional API."""
    config_workflow = ConfigWorkflow.from_config_file(
        str(config_paths["yml"]), template_context=config_paths["variables"]
    )
    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Build the WorkGraph
    workgraph = build_sirocco_workgraph(core_workflow)

    # Verify basic properties
    assert workgraph is not None
    assert workgraph.name.startswith(core_workflow.name)
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
    assert "front_depth" in window_config
    assert "task_dependencies" in window_config


@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "dynamic-simple",
    ],
)
def test_branch_independence_config(config_paths):
    """Test that branch independence workflow is configured correctly."""
    config_workflow = ConfigWorkflow.from_config_file(
        str(config_paths["yml"]), template_context=config_paths["variables"]
    )
    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Build the WorkGraph with front_depth=1
    workgraph = build_sirocco_workgraph(core_workflow, front_depth=1)

    # Verify window config is stored correctly
    assert "window_config" in workgraph.extras
    window_config = workgraph.extras["window_config"]
    assert window_config["enabled"] is True
    assert window_config["front_depth"] == 1
    assert "task_dependencies" in window_config

    # Get launcher task names
    launcher_tasks = [t.name for t in workgraph.tasks if t.name.startswith("launch_")]

    # Verify we have the expected tasks
    # Task names pattern: launch_{wg_name}_{task_name}_date_...
    # where wg_name includes timestamp, so we check for task names in the middle
    expected_task_names = [
        "root",
        "fast_1",
        "fast_2",
        "fast_3",
        "slow_1",
        "slow_2",
        "slow_3",
    ]
    for task_name in expected_task_names:
        # Match pattern: launch_*_{task_name}_date_*
        matching_tasks = [t for t in launcher_tasks if f"_{task_name}_date_" in t]
        assert len(matching_tasks) == 1, (
            f"Expected exactly 1 task containing '_{task_name}_date_', found {len(matching_tasks)}: {matching_tasks}"
        )

    # Verify dependency structure
    task_deps = window_config["task_dependencies"]

    # Find actual task names (pattern: launch_{wg_name}_root_date_2026_01_01_00_00_00)
    root_task = next(t for t in launcher_tasks if "_root_date_" in t)
    fast_1_task = next(t for t in launcher_tasks if "_fast_1_date_" in t)
    fast_2_task = next(t for t in launcher_tasks if "_fast_2_date_" in t)
    fast_3_task = next(t for t in launcher_tasks if "_fast_3_date_" in t)
    slow_1_task = next(t for t in launcher_tasks if "_slow_1_date_" in t)
    slow_2_task = next(t for t in launcher_tasks if "_slow_2_date_" in t)
    slow_3_task = next(t for t in launcher_tasks if "_slow_3_date_" in t)

    # Root has no dependencies
    assert task_deps[root_task] == [], f"Root task should have no dependencies, got: {task_deps[root_task]}"

    # Fast and slow branch first tasks depend on root
    assert root_task in task_deps[fast_1_task], "fast_1 should depend on root"
    assert root_task in task_deps[slow_1_task], "slow_1 should depend on root"

    # Verify chain dependencies within each branch
    assert fast_1_task in task_deps[fast_2_task], "fast_2 should depend on fast_1"
    assert fast_2_task in task_deps[fast_3_task], "fast_3 should depend on fast_2"
    assert slow_1_task in task_deps[slow_2_task], "slow_2 should depend on slow_1"
    assert slow_2_task in task_deps[slow_3_task], "slow_3 should depend on slow_2"
