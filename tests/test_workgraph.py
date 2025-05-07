import pytest
from aiida import orm

from sirocco.core import Workflow
from sirocco.parsing import yaml_data_models as models
from sirocco.workgraph import AiidaWorkGraph


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
            models.ConfigShellTask(name="task", command="echo test", src=str(script_path)),
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
            models.ConfigShellTask(name="task", command="echo test", src=str(script_path)),
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
            models.ConfigShellTask(name="task", command="echo test", src=str(script_path)),
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
