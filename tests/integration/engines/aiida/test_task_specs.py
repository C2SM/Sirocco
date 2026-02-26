"""Integration tests for task specification building.

Tests that verify Sirocco tasks are correctly converted to AiiDA task specs
(filenames, arguments, nodes, metadata).
"""

import pytest

from sirocco.core import Workflow
from sirocco.engines.aiida.spec_builders import ShellTaskSpecBuilder
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
    from datetime import UTC, datetime

    from sirocco import core

    config_workflow = ConfigWorkflow.from_config_file(
        str(config_paths["yml"]), template_context=config_paths["variables"]
    )

    # Update the stop_date for both cycles to make the result shorter
    config_workflow.cycles[0].cycling.stop_date = datetime(2027, 1, 1, 0, 0, tzinfo=UTC)
    config_workflow.cycles[1].cycling.stop_date = datetime(2027, 1, 1, 0, 0, tzinfo=UTC)
    core_workflow = Workflow.from_config_workflow(config_workflow)

    # Build task specs for shell tasks
    shell_tasks = [task for task in core_workflow.tasks if isinstance(task, core.ShellTask)]

    filenames_list = []
    arguments_list = []
    nodes_list = []

    for task in shell_tasks:
        task_spec = ShellTaskSpecBuilder(task).build_spec()
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
            "icon_restart_foo_0___bar_3__0___date_2026__01__01__00__00__00": "restart",
            "forcing": "forcing",
        },
        {
            "icon_restart_foo_1___bar_3__0___date_2026__01__01__00__00__00": "restart",
            "forcing": "forcing",
        },
        {
            "icon_output_foo_0___bar_3__0___date_2026__01__01__00__00__00": "icon_output_foo_0___bar_3__0___date_2026__01__01__00__00__00",
            "icon_output_foo_1___bar_3__0___date_2026__01__01__00__00__00": "icon_output_foo_1___bar_3__0___date_2026__01__01__00__00__00",
        },
        {
            "icon_output_foo_0___bar_3__0___date_2026__07__01__00__00__00": "icon_output_foo_0___bar_3__0___date_2026__07__01__00__00__00",
            "icon_output_foo_1___bar_3__0___date_2026__07__01__00__00__00": "icon_output_foo_1___bar_3__0___date_2026__07__01__00__00__00",
        },
        {"analysis_foo_bar_3__0___date_2026__01__01__00__00__00": "analysis"},
        {"analysis_foo_bar_3__0___date_2026__07__01__00__00__00": "analysis"},
        {
            "analysis_foo_bar_date_2026__01__01__00__00__00": "analysis_foo_bar_date_2026__01__01__00__00__00",
            "analysis_foo_bar_date_2026__07__01__00__00__00": "analysis_foo_bar_date_2026__07__01__00__00__00",
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
        f"--restart {{icon_restart_foo_0___bar_3__0___date_2026__01__01__00__00__00}} --init --forcing {forcing_path}",
        f"--restart {{icon_restart_foo_1___bar_3__0___date_2026__01__01__00__00__00}} --init --forcing {forcing_path}",
        "{icon_output_foo_0___bar_3__0___date_2026__01__01__00__00__00} {icon_output_foo_1___bar_3__0___date_2026__01__01__00__00__00}",
        "{icon_output_foo_0___bar_3__0___date_2026__07__01__00__00__00} {icon_output_foo_1___bar_3__0___date_2026__07__01__00__00__00}",
        "{analysis_foo_bar_3__0___date_2026__01__01__00__00__00}",
        "{analysis_foo_bar_3__0___date_2026__07__01__00__00__00}",
        "{analysis_foo_bar_date_2026__01__01__00__00__00} {analysis_foo_bar_date_2026__07__01__00__00__00}",
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
            "icon_restart_foo_0___bar_3__0___date_2026__01__01__00__00__00",
            "forcing",
        ],
        [
            "icon_restart_foo_1___bar_3__0___date_2026__01__01__00__00__00",
            "forcing",
        ],
        [
            "icon_output_foo_0___bar_3__0___date_2026__01__01__00__00__00",
            "icon_output_foo_1___bar_3__0___date_2026__01__01__00__00__00",
        ],
        [
            "icon_output_foo_0___bar_3__0___date_2026__07__01__00__00__00",
            "icon_output_foo_1___bar_3__0___date_2026__07__01__00__00__00",
        ],
        [
            "analysis_foo_bar_3__0___date_2026__01__01__00__00__00",
        ],
        [
            "analysis_foo_bar_3__0___date_2026__07__01__00__00__00",
        ],
        [
            "analysis_foo_bar_date_2026__01__01__00__00__00",
            "analysis_foo_bar_date_2026__07__01__00__00__00",
        ],
    ]

    assert arguments_list == expected_arguments_list
    assert filenames_list == expected_filenames_list
    assert nodes_list == expected_nodes_list
