"""Integration tests for task specification building.

Tests that verify Sirocco tasks are correctly converted to AiiDA task specs
(filenames, arguments, nodes, metadata).
"""

import pytest

from sirocco.core import Workflow
from sirocco.engines.aiida.tasks import build_icon_task_spec, build_shell_task_spec
from sirocco.parsing.yaml_data_models import ConfigWorkflow


@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize(
    "config_case",
    [
        "parameters",
    ],
)
def test_shell_filenames_nodes_arguments(config_paths):
    """Test that shell task specs contain correct filenames, nodes, and arguments."""
    import datetime

    from sirocco import core

    config_workflow = ConfigWorkflow.from_config_file(
        str(config_paths["yml"]), template_context=config_paths["variables"]
    )

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
        filenames_list.append(task_spec.filenames)
        arguments_list.append(task_spec.arguments_template)

        # In the new architecture, nodes come from two sources:
        # 1. Scripts (in node_pks)
        # 2. Input data (in input_data_info)
        # Note: Use 'name' for AvailableData, 'label' for GeneratedData
        nodes = list(task_spec.node_pks.keys())
        for input_info in task_spec.input_data_info:
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

    # Build expected arguments based on actual resolved paths
    # Note: Script names are removed from arguments_template (stored in code separately)
    # Note: AvailableData uses actual paths, not placeholders
    # Note: Whitespace is normalized (empty ports collapse double spaces to single spaces)
    initial_conditions_path = str(config_paths["yml"].parent / "data/initial_conditions")
    forcing_path = str(config_paths["yml"].parent / "data/forcing")

    expected_arguments_list = [
        f"--restart --init {initial_conditions_path} --forcing {forcing_path}",
        f"--restart --init {initial_conditions_path} --forcing {forcing_path}",
        f"--restart {{icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00}} --init --forcing {forcing_path}",
        f"--restart {{icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00}} --init --forcing {forcing_path}",
        "{icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00} {icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00} {icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_01_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_date_2026_01_01_00_00_00} {analysis_foo_bar_date_2026_07_01_00_00_00}",
    ]

    # Note: Scripts are no longer stored in node_pks in the current architecture
    # The nodes list only includes input data (AvailableData names and GeneratedData labels)
    expected_nodes_list = [
        [
            "initial_conditions",
            "forcing",
        ],
        [
            "initial_conditions",
            "forcing",
        ],
        [
            "icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "forcing",
        ],
        [
            "icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00",
            "forcing",
        ],
        [
            "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00",
        ],
        [
            "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00",
        ],
        [
            "analysis_foo_bar_3_0___date_2026_01_01_00_00_00",
        ],
        [
            "analysis_foo_bar_3_0___date_2026_07_01_00_00_00",
        ],
        [
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
        "small-icon",
    ],
)
def test_aiida_icon_task_metadata(config_paths):
    """Test if the metadata regarding the job submission of the `IconCalculation` is included in task specs."""
    import aiida.orm

    from sirocco import core

    config_workflow = ConfigWorkflow.from_config_file(
        str(config_paths["yml"]), template_context=config_paths["variables"]
    )

    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Test wrapper script and metadata for ICON tasks
    icon_tasks = [task for task in core_workflow.tasks if isinstance(task, core.IconTask)]

    for task in icon_tasks:
        task_spec = build_icon_task_spec(task)

        # Test wrapper script
        if task_spec.wrapper_script_pk is not None:
            wrapper_node = aiida.orm.load_node(task_spec.wrapper_script_pk)
            assert wrapper_node.filename == "dummy_wrapper.sh"

        # Test uenv and view in metadata
        metadata = task_spec.metadata
        custom_scheduler_commands = getattr(metadata.options, "custom_scheduler_commands", "") or ""
        assert "#SBATCH --uenv=icon-wcp/v1:rc4" in custom_scheduler_commands
        assert "#SBATCH --view=icon" in custom_scheduler_commands

    # Test default wrapper script behavior
    config_workflow = ConfigWorkflow.from_config_file(
        str(config_paths["yml"]), template_context=config_paths["variables"]
    )

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
        if task_spec.wrapper_script_pk is not None:
            wrapper_node = aiida.orm.load_node(task_spec.wrapper_script_pk)
            assert wrapper_node.filename == "todi_cpu.sh"
