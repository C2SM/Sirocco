"""Shared test utilities for creating real Sirocco objects.

These helper functions create real task and data objects via workflow parsing,
which ensures they are properly initialized through the same path as production code.
This avoids the need to repeat complex setup code across test files.
"""

import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

from aiida.orm import ProcessNode
from aiida_workgraph.orm.workgraph import WorkGraphNode

from sirocco import core
from sirocco.core import workflow


# NOTE: See if this can be merged with `create_shell_task_from_workflow`
def create_mock_shell_task(**overrides):
    """Create a mock ShellTask with default attributes.

    Args:
        **overrides: Attribute overrides for the task

    Returns:
        Mock ShellTask with default attributes and any overrides applied
    """
    task = Mock(spec=core.ShellTask)

    # Set defaults for common attributes
    defaults = {
        "name": "test_task",
        "coordinates": {},
        "computer": "test_computer",
        "walltime": None,
        "mem": None,
        "partition": None,
        "account": None,
        "nodes": None,
        "ntasks_per_node": None,
        "cpus_per_task": None,
        "uenv": None,
        "view": None,
        "path": None,
        "command": None,
    }

    # Apply overrides
    defaults.update(overrides)

    # Set attributes on mock
    for key, value in defaults.items():
        setattr(task, key, value)

    # Add default cycle_point if not in overrides
    if "cycle_point" not in overrides:
        cycle_point = Mock()
        cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.cycle_point = cycle_point

    return task


def create_mock_icon_task(**overrides):
    """Create a mock IconTask with default attributes.

    Args:
        **overrides: Attribute overrides for the task

    Returns:
        Mock IconTask with default attributes and any overrides applied
    """
    task = Mock(spec=core.IconTask)

    # Set defaults for common attributes
    defaults = {
        "name": "test_task",
        "coordinates": {},
        "computer": "test_computer",
        "walltime": None,
        "mem": None,
        "partition": None,
        "account": None,
        "nodes": None,
        "ntasks_per_node": None,
        "cpus_per_task": None,
        "uenv": None,
        "view": None,
        "path": None,
        "command": None,
    }

    # Apply overrides
    defaults.update(overrides)

    # Set attributes on mock
    for key, value in defaults.items():
        setattr(task, key, value)

    # Add default cycle_point if not in overrides
    if "cycle_point" not in overrides:
        cycle_point = Mock()
        cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.cycle_point = cycle_point

    return task


def create_available_data(name, computer_label, path, coordinates=None, file_format=None):
    """Create a real AvailableData instance.

    Args:
        name: Data name
        computer_label: Computer label
        path: Path to data (can be str or Path)
        coordinates: Optional coordinates dict (defaults to empty dict)
        format: Optional format string

    Returns:
        Real AvailableData instance
    """
    return core.AvailableData(
        name=name,
        computer=computer_label,
        path=Path(path) if not isinstance(path, Path) else path,
        coordinates=coordinates if coordinates is not None else {},
        format=file_format,
    )


def create_mock_transport(path_exists=True, isfile=True):  # noqa FBT002: Boolean default positional argument in function definition
    """Create a mock transport configured as a context manager.

    Note: We still mock transports for tests that need to simulate
    remote file checks without actual remote connections.

    Args:
        path_exists: Whether path_exists() should return True
        isfile: Whether isfile() should return True

    Returns:
        MagicMock configured as transport context manager
    """
    mock_transport = MagicMock()
    mock_transport.path_exists.return_value = path_exists
    mock_transport.isfile.return_value = isfile
    mock_transport.__enter__.return_value = mock_transport
    mock_transport.__exit__.return_value = None
    return mock_transport


def create_authinfo(transport=None):
    """Create or mock authinfo with transport.

    For tests simulating remote scenarios, we mock the authinfo.
    For local scenarios, we could use real authinfo.

    Args:
        computer: AiiDA Computer instance
        transport: Optional mock transport (creates mock if None)

    Returns:
        MagicMock configured as authinfo with transport
    """
    if transport is None:
        transport = create_mock_transport()

    mock_authinfo = MagicMock()
    mock_authinfo.get_transport.return_value = transport
    return mock_authinfo


def create_shell_task_from_workflow(
    tmp_path,
    name="test_task",
    command="echo hello",
    computer="localhost",
    walltime="01:00:00",
    **task_config,
):
    """Create a real ShellTask by parsing a minimal workflow.

    Args:
        tmp_path: Temporary directory for workflow rootdir
        name: Task name
        command: Shell command
        computer: Computer label
        walltime: Wall time limit
        **task_config: Additional task config (nodes, partition, etc.)

    Returns:
        ShellTask: A real ShellTask extracted from the workflow
    """
    # Build additional task config lines with proper indentation
    extra_config = ""
    if task_config:
        extra_config = "\n" + "\n".join(f"              {k}: {v}" for k, v in task_config.items())

    config_yaml = textwrap.dedent(
        f"""
        name: test_workflow
        scheduler: slurm
        cycles:
          - single:
              tasks:
                - {name}: {{}}
        tasks:
          - {name}:
              plugin: shell
              computer: {computer}
              command: {command}
              walltime: {walltime}{extra_config}
        data:
          available: []
          generated: []
        """
    )
    wf = workflow.Workflow.from_config_str(config_yaml, rootdir=tmp_path)
    # Return the first task from the workflow by iterating
    return next(iter(wf.tasks))


def create_icon_task_from_workflow(
    tmp_path,
    name="test_icon",
    computer="localhost",
    bin_path=None,
    walltime="01:00:00",
    nodes=1,
    ntasks_per_node=12,
    **task_config,
):
    """Create a real IconTask by parsing a workflow with namelist files.

    Args:
        tmp_path: Temporary directory for workflow rootdir and namelists
        name: Task name
        computer: Computer label
        bin_path: Path to ICON binary (defaults to tmp_path/icon)
        walltime: Wall time limit
        nodes: Number of nodes
        ntasks_per_node: Tasks per node
        **task_config: Additional task config

    Returns:
        IconTask: A real IconTask extracted from the workflow
    """
    # Create minimal namelist files
    icon_dir = tmp_path / "ICON"
    icon_dir.mkdir(exist_ok=True)

    master_nml = icon_dir / "icon_master.namelist"
    master_nml.write_text(
        textwrap.dedent("""
        &master_nml
         lrestart = .false.
        /
    """)
    )

    model_nml = icon_dir / "model.namelist"
    model_nml.write_text(
        textwrap.dedent("""
        &run_nml
         num_lev = 90
        /
    """)
    )

    bin_path = bin_path or tmp_path / "icon"
    bin_path.touch(exist_ok=True)

    # Build additional task config lines with proper indentation
    extra_config = ""
    if task_config:
        extra_config = "\n" + "\n".join(f"              {k}: {v}" for k, v in task_config.items())

    # Use date cycling for IconTask (required by update_icon_namelists_from_workflow)
    config_yaml = textwrap.dedent(
        f"""
        name: test_workflow
        scheduler: slurm
        cycles:
          - single:
              cycling:
                start_date: 2026-01-01T00:00
                stop_date: 2026-01-02T00:00
                period: P1D
              tasks:
                - {name}: {{}}
        tasks:
          - {name}:
              plugin: icon
              computer: {computer}
              bin: {bin_path}
              namelists:
                - ./ICON/icon_master.namelist
                - ./ICON/model.namelist
              walltime: {walltime}
              nodes: {nodes}
              ntasks_per_node: {ntasks_per_node}{extra_config}
        data:
          available: []
          generated: []
        """
    )
    wf = workflow.Workflow.from_config_str(config_yaml, rootdir=tmp_path)
    # Return the first task from the workflow by iterating
    return next(iter(wf.tasks))


def create_icon_task_with_model_namelists(
    tmp_path,
    name="test_icon",
    computer="localhost",
    models=None,
):
    """Create an ICON task with model namelists (atm, oce, etc.).

    Args:
        tmp_path: Temporary directory for workflow rootdir and namelists
        name: Task name
        computer: Computer label
        models: List of model names (defaults to ["atm", "oce"])

    Returns:
        IconTask: An ICON task with master and model namelists
    """
    models = models or ["atm", "oce"]

    # Create namelist directory
    icon_dir = tmp_path / "ICON"
    icon_dir.mkdir(exist_ok=True)

    # Create master namelist referencing model namelists
    master_content = "&master_nml\n lrestart = .false.\n/\n"
    for model in models:
        master_content += f"""&master_model_nml
 model_name="{model}"
 model_namelist_filename="{model}.namelist"
 model_type=1
/
"""

    master_nml = icon_dir / "icon_master.namelist"
    master_nml.write_text(master_content)

    # Create model namelist files
    namelist_paths = ["./ICON/icon_master.namelist"]
    for model in models:
        model_nml = icon_dir / f"{model}.namelist"
        model_nml.write_text("&run_nml\n num_lev = 90\n/\n")
        namelist_paths.append(f"./ICON/{model}.namelist")

    # Create bin path
    bin_path = tmp_path / "icon"
    bin_path.touch()

    # Hardcoded YAML config with model namelists
    namelist_yaml = "\n".join(f"                - {path}" for path in namelist_paths)

    config_yaml = f"""name: test_workflow
scheduler: slurm
cycles:
  - single:
      cycling:
        start_date: 2026-01-01T00:00
        stop_date: 2026-01-02T00:00
        period: P1D
      tasks:
        - {name}: {{}}
tasks:
  - {name}:
      plugin: icon
      computer: {computer}
      bin: {bin_path}
      namelists:
{namelist_yaml}
      walltime: 01:00:00
      nodes: 1
      ntasks_per_node: 12
data:
  available: []
  generated: []
"""

    wf = workflow.Workflow.from_config_str(config_yaml, rootdir=tmp_path)
    return next(iter(wf.tasks))


def create_generated_data(
    name="output_data",
    path="output.txt",
    coordinates=None,
):
    """Create a real GeneratedData object directly.

    Args:
        name: Data name
        path: Path to output file (can be None)
        coordinates: Cycle coordinates (defaults to empty dict)

    Returns:
        GeneratedData: A real GeneratedData instance
    """
    from sirocco.core import GeneratedData

    return GeneratedData(
        name=name,
        path=Path(path) if path else None,
        coordinates=coordinates or {},
    )


# TODO: Move this to integration test utils (probably)
def extract_launcher_times(workgraph_process: ProcessNode) -> dict[str, dict[str, Any]]:
    """Extract launcher WorkGraph creation and completion times.

    Analyzes launcher sub-WorkGraphs to determine when tasks were submitted
    (ctime) and when they completed (mtime). This is essential for validating
    dynamic level computation and pre-submission behavior.

    Args:
        workgraph_process: The main WorkGraph process node

    Returns:
        Dictionary mapping task names to timing info:
        {
            'task_name': {
                'label': 'launch_task_name_...',
                'ctime': datetime,  # When launcher was created (task submitted)
                'mtime': datetime,  # When launcher finished (task completed)
                'branch': 'fast'|'slow'|'medium'|'root',
                'pk': int
            }
        }
    """
    timing_data = {}

    for desc in workgraph_process.called_descendants:
        if isinstance(desc, WorkGraphNode) and desc.label.startswith("launch_"):
            # Parse task name from label
            # New format: "launch_workflow_name_timestamp_task_name_date_..."
            # Old format: "launch_task_name_date_..."
            # We need to find the task name by looking for known branch prefixes
            label_parts = desc.label.split("_")

            # Find the task name by searching for known branch prefixes
            task_name = None
            branch = None
            for i, part in enumerate(label_parts):
                if part in ["root", "fast", "slow", "medium", "setup", "finalize"]:
                    if part in ["root", "setup", "finalize"]:
                        task_name = part
                        branch = part
                    elif i + 1 < len(label_parts) and label_parts[i + 1].isdigit():
                        # For numbered tasks like fast_1, medium_2, slow_3
                        task_name = f"{part}_{label_parts[i + 1]}"
                        branch = part
                    else:
                        task_name = part
                        branch = part
                    break

            if task_name and branch:
                timing_data[task_name] = {
                    "label": desc.label,
                    "ctime": desc.ctime,  # Creation time = task submission
                    "mtime": desc.mtime,  # Modification time = task completion
                    "branch": branch,
                    "pk": desc.pk,
                }

    return timing_data


def extract_task_completion_times(
    workgraph_process: ProcessNode,
) -> dict[str, datetime]:
    """Extract actual task execution completion times.

    This extracts the completion times of the actual task ProcessNodes (not
    the launcher WorkGraphs), which represents when the shell job or CalcJob
    finished executing.

    Args:
        workgraph_process: The main WorkGraph process node

    Returns:
        Dictionary mapping task names to completion timestamps:
        {
            'fast_1': datetime,
            'slow_2': datetime,
            ...
        }

    Note:
        This is different from extract_launcher_times() - launcher times show
        when the WorkGraph engine submitted tasks, while completion times show
        when the actual jobs finished.
    """
    task_times = {}

    for node in workgraph_process.called_descendants:
        if isinstance(node, ProcessNode):
            # Get the label which contains the task name
            task_label = getattr(node, "label", "") or getattr(node, "process_label", "")
            if task_label:
                # Filter for task nodes (not launcher WorkGraphs)
                prefixes = ["fast_", "slow_", "medium_", "root", "setup", "finalize"]
                if any(task_label.startswith(prefix) for prefix in prefixes) and not task_label.startswith("launch_"):
                    # Only include if it's not a launcher (those have 'launch_' prefix)
                    task_times[task_label] = node.mtime

    return task_times


def compute_relative_times(
    timing_data: dict[str, dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Convert absolute timestamps to relative times from workflow start.

    Args:
        timing_data: Output from extract_launcher_times()

    Returns:
        Dictionary with relative times in seconds:
        {
            'task_name': {
                'start': float,  # Seconds from workflow start
                'end': float,    # Seconds from workflow start
                'duration': float,  # Task duration in seconds
                'branch': str
            }
        }
    """
    if not timing_data:
        return {}

    # Find workflow start time (earliest ctime)
    workflow_start = min(info["ctime"] for info in timing_data.values())

    relative_times = {}
    for task_name, info in timing_data.items():
        start_rel = (info["ctime"] - workflow_start).total_seconds()
        end_rel = (info["mtime"] - workflow_start).total_seconds()

        relative_times[task_name] = {
            "start": start_rel,
            "end": end_rel,
            "duration": end_rel - start_rel,
            "branch": info["branch"],
        }

    return relative_times


def assert_branch_independence(
    timing_data: dict[str, dict[str, Any]],
    fast_branch: str = "fast",
    slow_branch: str = "slow",
    message_prefix: str = "",
) -> None:
    """Assert that the fast branch completed before the slow branch.

    This is the key test for dynamic level computation - branches should
    advance independently without waiting for each other at the same
    topological level.

    Args:
        timing_data: Output from extract_launcher_times()
        fast_branch: Name of the fast branch (default: 'fast')
        slow_branch: Name of the slow branch (default: 'slow')
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If fast branch did not complete before slow branch
    """
    # Find last task from each branch
    fast_tasks = {name: info for name, info in timing_data.items() if info["branch"] == fast_branch}
    slow_tasks = {name: info for name, info in timing_data.items() if info["branch"] == slow_branch}

    assert fast_tasks, f"{message_prefix}No tasks found for branch '{fast_branch}'"
    assert slow_tasks, f"{message_prefix}No tasks found for branch '{slow_branch}'"

    # Get completion times of last tasks
    last_fast_task = max(fast_tasks.items(), key=lambda x: x[1]["mtime"])
    last_slow_task = max(slow_tasks.items(), key=lambda x: x[1]["mtime"])

    last_fast_name, last_fast_info = last_fast_task
    last_slow_name, last_slow_info = last_slow_task

    assert last_fast_info["mtime"] < last_slow_info["mtime"], (
        f"{message_prefix}Fast branch should complete before slow branch. "
        f"Fast: {last_fast_name} at {last_fast_info['mtime']}, "
        f"Slow: {last_slow_name} at {last_slow_info['mtime']}"
    )


def assert_pre_submission_occurred(
    timing_data: dict[str, dict[str, Any]],
    task: str,
    dependency: str,
    message_prefix: str = "",
) -> None:
    """Assert that a task was submitted before its dependency finished.

    This validates pre-submission behavior (front_depth > 0), where tasks can
    be submitted before their dependencies complete.

    Args:
        timing_data: Output from extract_launcher_times()
        task: Name of the task that should be pre-submitted
        dependency: Name of the dependency task
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If task was not submitted before dependency finished
    """
    assert task in timing_data, f"{message_prefix}Task '{task}' not found in timing data"
    assert dependency in timing_data, f"{message_prefix}Dependency '{dependency}' not found"

    task_submit = timing_data[task]["ctime"]
    dep_finish = timing_data[dependency]["mtime"]

    assert task_submit < dep_finish, (
        f"{message_prefix}Pre-submission failed: {task} should be submitted before "
        f"{dependency} finishes. Task submitted at {task_submit}, "
        f"dependency finished at {dep_finish}"
    )


def assert_submission_order(
    timing_data: dict[str, dict[str, Any]],
    task_order: list[str],
    message_prefix: str = "",
) -> None:
    """Assert that tasks were submitted in the specified order.

    This is useful for validating that dynamic level computation produces
    the expected submission sequence.

    Args:
        timing_data: Output from extract_launcher_times()
        task_order: List of task names in expected submission order
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If tasks were not submitted in the specified order
    """
    for i in range(len(task_order) - 1):
        earlier = task_order[i]
        later = task_order[i + 1]

        assert earlier in timing_data, f"{message_prefix}Task '{earlier}' not found"
        assert later in timing_data, f"{message_prefix}Task '{later}' not found"

        earlier_time = timing_data[earlier]["ctime"]
        later_time = timing_data[later]["ctime"]

        assert earlier_time <= later_time, (
            f"{message_prefix}Submission order violation: {earlier} should be "
            f"submitted before {later}. {earlier} at {earlier_time}, "
            f"{later} at {later_time}"
        )


def assert_cross_dependency_respected(
    timing_data: dict[str, dict[str, Any]],
    dependent_task: str,
    dependency_tasks: list[str],
    message_prefix: str = "",
) -> None:
    """Assert that a task started after ALL its cross-branch dependencies finished.

    Cross-dependencies between branches should be properly enforced even with
    dynamic level computation.

    Args:
        timing_data: Output from extract_launcher_times()
        dependent_task: Task that depends on others
        dependency_tasks: List of tasks that dependent_task depends on
        message_prefix: Optional prefix for error messages

    Raises:
        AssertionError: If dependent task started before all dependencies finished
    """
    assert dependent_task in timing_data, f"{message_prefix}Task '{dependent_task}' not found"

    dependent_submit = timing_data[dependent_task]["ctime"]

    for dep in dependency_tasks:
        assert dep in timing_data, f"{message_prefix}Dependency '{dep}' not found"

        dep_finish = timing_data[dep]["mtime"]

        assert dep_finish <= dependent_submit, (
            f"{message_prefix}Cross-dependency violation: {dependent_task} "
            f"submitted before {dep} finished. {dependent_task} at {dependent_submit}, "
            f"{dep} finished at {dep_finish}"
        )
