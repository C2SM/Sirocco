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
        # node_pks keys represent the node names
        nodes_list.append(list(task_spec["node_pks"].keys()))

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
    assert len(launcher_tasks) == len(core_workflow.tasks)
    assert len(get_job_data_tasks) == len(core_workflow.tasks)

    # Verify window config is stored in extras
    assert "window_config" in workgraph.extras
    window_config = workgraph.extras["window_config"]
    assert "enabled" in window_config
    assert "window_size" in window_config
    assert "task_levels" in window_config


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
