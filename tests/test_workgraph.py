import pytest
from aiida import orm

from sirocco.core import Workflow
from sirocco.parsing import yaml_data_models as models
from sirocco.workgraph import AiidaWorkGraph
import textwrap


@pytest.mark.usefixtures("aiida_localhost")
def test_set_shelljob_filenames(tmp_path):
    file_name = "foo.txt"
    file_path = tmp_path / file_name
    # Dummy script, as `src` must be specified due to relative command path
    script_path = tmp_path / "my_script.sh"

    config_wf = models.ConfigWorkflow(
        name="remote",
        rootdir=tmp_path,
        cycles=[
            models.ConfigCycle(
                name="remote",
                tasks=[
                    models.ConfigCycleTask(
                        name="task",
                        inputs=[models.ConfigCycleTaskInput(name="my_data", port="unused")],
                    ),
                ],
            ),
        ],
        tasks=[
            models.ConfigShellTask(name="task", command="echo test", src=str(script_path), computer="localhost"),
        ],
        data=models.ConfigData(
            available=[
                models.ConfigAvailableData(
                    name="my_data",
                    type=models.DataType.FILE,
                    src=str(file_path),
                    computer="localhost",
                )
            ],
        ),
        parameters={},
    )

    core_wf = Workflow.from_config_workflow(config_workflow=config_wf)
    aiida_wf = AiidaWorkGraph(core_workflow=core_wf)
    remote_data = aiida_wf._workgraph.tasks[0].inputs.nodes["my_data"].value  # noqa: SLF001
    assert isinstance(remote_data, orm.RemoteData)
    filenames = aiida_wf._workgraph.tasks[0].inputs.filenames.value  # noqa: SLF001
    assert filenames == {"my_data": "foo.txt"}


@pytest.mark.usefixtures("aiida_localhost")
def test_multiple_inputs_filenames(tmp_path):
    file_names = ["foo.txt", "bar.txt", "baz.dat"]
    for name in file_names:
        (tmp_path / name).touch()
    script_path = tmp_path / "my_script.sh"

    # Create configuration with multiple inputs
    config_wf = models.ConfigWorkflow(
        name="remote",
        rootdir=tmp_path,
        cycles=[
            models.ConfigCycle(
                name="remote",
                tasks=[
                    models.ConfigCycleTask(
                        name="task",
                        inputs=[
                            models.ConfigCycleTaskInput(name=f"data_{i}", port=f"port_{i}")
                            for i in range(len(file_names))
                        ],
                    ),
                ],
            ),
        ],
        tasks=[
            models.ConfigShellTask(name="task", command="echo test", src=str(script_path), computer="localhost"),
        ],
        data=models.ConfigData(
            available=[
                models.ConfigAvailableData(
                    name=f"data_{i}",
                    type=models.DataType.FILE,
                    src=name,
                    computer="localhost",
                )
                for i, name in enumerate(file_names)
            ],
        ),
        parameters={},
    )

    core_wf = Workflow.from_config_workflow(config_workflow=config_wf)
    aiida_wf = AiidaWorkGraph(core_workflow=core_wf)

    expected_filenames = {f"data_{i}": name for i, name in enumerate(file_names)}
    filenames = aiida_wf._workgraph.tasks[0].inputs.filenames.value  # noqa: SLF001
    assert filenames == expected_filenames


@pytest.mark.usefixtures("aiida_localhost")
def test_directory_input_filenames(tmp_path):
    dir_name = "test_dir"
    dir_path = tmp_path / dir_name
    dir_path.mkdir()
    script_path = tmp_path / "my_script.sh"

    config_wf = models.ConfigWorkflow(
        name="remote",
        rootdir=tmp_path,
        cycles=[
            models.ConfigCycle(
                name="remote",
                tasks=[
                    models.ConfigCycleTask(
                        name="task",
                        inputs=[models.ConfigCycleTaskInput(name="my_dir", port="unused")],
                    ),
                ],
            ),
        ],
        tasks=[
            models.ConfigShellTask(name="task", command="echo test", src=str(script_path), computer="localhost"),
        ],
        data=models.ConfigData(
            available=[
                models.ConfigAvailableData(
                    name="my_dir",
                    type=models.DataType.DIR,
                    src=dir_name,
                    computer="localhost",
                )
            ],
        ),
        parameters={},
    )

    core_wf = Workflow.from_config_workflow(config_workflow=config_wf)
    aiida_wf = AiidaWorkGraph(core_workflow=core_wf)

    filenames = aiida_wf._workgraph.tasks[0].inputs.filenames.value  # noqa: SLF001
    assert filenames == {"my_dir": dir_name}


@pytest.mark.usefixtures("aiida_localhost")
def test_set_shelljob_filenames_parametrized(tmp_path):
    yaml_str = textwrap.dedent(
        """
        start_date: &root_start_date "2026-01-01T00:00"
        stop_date: &root_stop_date "2028-01-01T00:00"
        cycles:
            - bimonthly_tasks:
                cycling:
                    start_date: *root_start_date
                    stop_date: *root_stop_date
                    period: P6M
                tasks:
                    - icon:
                        inputs:
                            - initial_conditions:
                                when:
                                    at: *root_start_date
                                port: init
                            - icon_restart:
                                when:
                                    after: *root_start_date
                                target_cycle:
                                    lag: -P6M
                                parameters:
                                    foo: single
                                    bar: single
                                port: restart
                            - forcing:
                                port: forcing
                        outputs: [icon_output, icon_restart]
                    - statistics_foo:
                        inputs:
                            - icon_output:
                                parameters:
                                    bar: single
                                port: None
                        outputs: [analysis_foo]
                    - statistics_foo_bar:
                        inputs:
                            - analysis_foo:
                                port: None
                        outputs: [analysis_foo_bar]
            - yearly:
                cycling:
                    start_date: *root_start_date
                    stop_date: *root_stop_date
                    period: P1Y
                tasks:
                    - merge:
                        inputs:
                            - analysis_foo_bar:
                                target_cycle:
                                    lag: ["P0M", "P6M"]
                                port: None
                        outputs: [yearly_analysis]
        tasks:
            - icon:
                plugin: shell
                src: scripts/icon.py
                command: "icon.py --restart {PORT::restart} --init {PORT::init} --forcing {PORT::forcing}"
                parameters: [foo, bar]
                computer: localhost
            - statistics_foo:
                plugin: shell
                src: scripts/statistics.py
                command: "statistics.py {PORT::None}"
                parameters: [bar]
                computer: localhost
            - statistics_foo_bar:
                plugin: shell
                src: scripts/statistics.py
                command: "statistics.py {PORT::None}"
                computer: localhost
            - merge:
                plugin: shell
                src: scripts/merge.py
                command: "merge.py {PORT::None}"
                computer: localhost
        data:
            available:
                - initial_conditions:
                    type: file
                    src: data/initial_conditions
                    computer: localhost
                - forcing:
                    type: file
                    src: data/forcing
                    computer: localhost
            generated:
                - icon_output:
                    type: file
                    src: icon_output
                    parameters: [foo, bar]
                - icon_restart:
                    type: file
                    src: restart
                    parameters: [foo, bar]
                - analysis_foo:
                    type: file
                    src: analysis
                    parameters: [bar]
                - analysis_foo_bar:
                    type: file
                    src: analysis
                - yearly_analysis:
                    type: file
                    src: analysis
        parameters:
            foo: [0, 1]
            bar: [3.0]
        """
    )
    yaml_file = tmp_path / "config.yml"
    yaml_file.write_text(yaml_str)

    core_wf = Workflow.from_config_file(yaml_file)
    aiida_wf = AiidaWorkGraph(core_workflow=core_wf)
    filenames_list = [task.inputs.filenames.value for task in aiida_wf._workgraph.tasks]
    arguments_list = [task.inputs.arguments.value for task in aiida_wf._workgraph.tasks]
    import ipdb; ipdb.set_trace()
    nodes_list = [list(task.inputs.nodes._sockets.keys()) for task in aiida_wf._workgraph.tasks]
    expected_filenames_list = [
        {"forcing": "forcing", "initial_conditions": "initial_conditions"},
        {"forcing": "forcing", "initial_conditions": "initial_conditions"},
        {"forcing": "forcing", "restart": "icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00"},
        {"forcing": "forcing", "restart": "icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00"},
        {"forcing": "forcing", "restart": "icon_restart_foo_0___bar_3_0___date_2026_07_01_00_00_00"},
        {"forcing": "forcing", "restart": "icon_restart_foo_1___bar_3_0___date_2026_07_01_00_00_00"},
        {"forcing": "forcing", "restart": "icon_restart_foo_0___bar_3_0___date_2027_01_01_00_00_00"},
        {"forcing": "forcing", "restart": "icon_restart_foo_1___bar_3_0___date_2027_01_01_00_00_00"},
        {"icon_output": "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00"},
        {"icon_output": "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00"},
        {"icon_output": "icon_output_foo_0___bar_3_0___date_2027_01_01_00_00_00"},
        {"icon_output": "icon_output_foo_0___bar_3_0___date_2027_07_01_00_00_00"},
        {"analysis": "analysis_foo_bar_3_0___date_2026_01_01_00_00_00"},
        {"analysis": "analysis_foo_bar_3_0___date_2026_07_01_00_00_00"},
        {"analysis": "analysis_foo_bar_3_0___date_2027_01_01_00_00_00"},
        {"analysis": "analysis_foo_bar_3_0___date_2027_07_01_00_00_00"},
        {"analysis": "analysis_foo_bar_date_2026_01_01_00_00_00"},
        {"analysis": "analysis_foo_bar_date_2027_01_01_00_00_00"},
    ]
    expected_arguments_list = [
        "--restart  --init {initial_conditions} --forcing {forcing}",
        "--restart  --init {initial_conditions} --forcing {forcing}",
        "--restart {icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_0___bar_3_0___date_2026_07_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_1___bar_3_0___date_2026_07_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_0___bar_3_0___date_2027_01_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_1___bar_3_0___date_2027_01_01_00_00_00} --init  " "--forcing {forcing}",
        "{icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2027_01_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2027_01_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2027_07_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2027_07_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_01_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2027_01_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2027_07_01_00_00_00}",
        "{analysis_foo_bar_date_2026_01_01_00_00_00} " "{analysis_foo_bar_date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_date_2027_01_01_00_00_00} " "{analysis_foo_bar_date_2027_07_01_00_00_00}",
    ]
    assert filenames_list == expected_filenames_list
    import ipdb

    ipdb.set_trace()
