import pytest

from sirocco.core import Workflow
from sirocco.workgraph import AiidaWorkGraph


# Hardcoded, explicit integration test based on the `parameters` case for now
@pytest.mark.usefixtures("aiida_localhost", "config_case")
@pytest.mark.parametrize(
    "config_case",
    [
        "parameters",
    ],
)
def test_shell_filenames_nodes_arguments(config_paths):
    import datetime

    from sirocco.parsing.yaml_data_models import ConfigWorkflow

    config_workflow = ConfigWorkflow.from_config_file(str(config_paths["yml"]))

    # Update the stop_date for both cycles to make the result shorter
    # NOTE: We currently don't use timezone-aware times in config YAML, thus ignora DTZ001 for now.
    # See https://github.com/C2SM/Sirocco/issues/161
    config_workflow.cycles[0].cycling.stop_date = datetime.datetime(2027, 1, 1, 0, 0)  # noqa: DTZ001
    config_workflow.cycles[1].cycling.stop_date = datetime.datetime(2027, 1, 1, 0, 0)  # noqa: DTZ001
    core_workflow = Workflow.from_config_workflow(config_workflow)
    aiida_workflow = AiidaWorkGraph(core_workflow)

    # NOTE: SLF001 will be fixed with https://github.com/C2SM/Sirocco/issues/82
    filenames_list = [
        task.inputs.filenames.value
        for task in aiida_workflow._workgraph.tasks  # noqa: SLF001
    ]
    arguments_list = [
        task.inputs.arguments.value
        for task in aiida_workflow._workgraph.tasks  # noqa: SLF001
    ]
    nodes_list = [
        list(task.inputs.nodes._sockets.keys())  # noqa: SLF001
        for task in aiida_workflow._workgraph.tasks  # noqa: SLF001
    ]

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
        "--restart  --init {initial_conditions} --forcing {forcing}",
        "--restart  --init {initial_conditions} --forcing {forcing}",
        "--restart {icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00} --init  --forcing {forcing}",
        "--restart {icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00} --init  --forcing {forcing}",
        "{icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00} {icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00} {icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_01_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_date_2026_01_01_00_00_00} {analysis_foo_bar_date_2026_07_01_00_00_00}",
    ]

    expected_nodes_list = [
        ["initial_conditions", "forcing"],
        ["initial_conditions", "forcing"],
        ["icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00", "forcing"],
        ["icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00", "forcing"],
        [
            "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00",
        ],
        [
            "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00",
        ],
        ["analysis_foo_bar_3_0___date_2026_01_01_00_00_00"],
        ["analysis_foo_bar_3_0___date_2026_07_01_00_00_00"],
        [
            "analysis_foo_bar_date_2026_01_01_00_00_00",
            "analysis_foo_bar_date_2026_07_01_00_00_00",
        ],
    ]

    assert arguments_list == expected_arguments_list
    assert filenames_list == expected_filenames_list
    assert nodes_list == expected_nodes_list
