import pathlib

import pytest

from sirocco import pretty_print
from sirocco.core import _tasks as core_tasks
from sirocco.core import workflow
from sirocco.parsing import yaml_data_models as models

pytest_plugins = ["aiida.tools.pytest_fixtures"]


@pytest.fixture(scope="session")
def minimal_config() -> models.ConfigWorkflow:
    return models.ConfigWorkflow(
        name="minimal",
        rootdir=pathlib.Path("minimal"),
        cycles=[models.ConfigCycle(name="minimal", tasks=[models.ConfigCycleTask(name="some_task")])],
        tasks=[models.ConfigShellTask(name="some_task", command="some_command")],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(name="foo", type=models.DataType.FILE, src="foo.txt")],
            generated=[models.ConfigGeneratedData(name="bar", type=models.DataType.DIR, src="bar")],
        ),
        parameters={},
    )


@pytest.fixture(scope="session")
def minimal_invert_task_io_config() -> models.ConfigWorkflow:
    return models.ConfigWorkflow(
        name="minimal",
        rootdir=pathlib.Path("minimal"),
        cycles=[
            models.ConfigCycle(
                name="minimal",
                tasks=[
                    models.ConfigCycleTask(
                        name="task_b",
                        inputs=[models.ConfigCycleTaskInput(name="output_a", port="None")],
                        outputs=[models.ConfigCycleTaskOutput(name="output_b")],
                    ),
                    models.ConfigCycleTask(
                        name="task_a",
                        inputs=[models.ConfigCycleTaskInput(name="availalble", port="None")],
                        outputs=[models.ConfigCycleTaskOutput(name="output_a")],
                    ),
                ],
            ),
        ],
        tasks=[
            models.ConfigShellTask(name="task_a", command="command_a"),
            models.ConfigShellTask(name="task_b", command="command_b"),
        ],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(name="availalble", type=models.DataType.FILE, src="foo.txt")],
            generated=[
                models.ConfigGeneratedData(name="output_a", type=models.DataType.DIR, src="bar"),
                models.ConfigGeneratedData(name="output_b", type=models.DataType.DIR, src="bar"),
            ],
        ),
        parameters={},
    )


# configs that are tested for parsing
ALL_CONFIG_CASES = ["small", "parameters", "large"]


@pytest.fixture(params=ALL_CONFIG_CASES)
def config_case(request) -> str:
    return request.param


@pytest.fixture
def pprinter() -> pretty_print.PrettyPrinter:
    return pretty_print.PrettyPrinter()


def generate_config_paths(test_case: str):
    return {
        "yml": pathlib.Path(f"tests/cases/{test_case}/config/config.yml"),
        "txt": pathlib.Path(f"tests/cases/{test_case}/data/config.txt"),
        "svg": pathlib.Path(f"tests/cases/{test_case}/svg/config.svg"),
    }


@pytest.fixture
def config_paths(config_case) -> dict[str, pathlib.Path]:
    return generate_config_paths(config_case)


def pytest_addoption(parser):
    parser.addoption("--reserialize", action="store_true", default=False)


def serialize_worklfow(config_paths: dict[str, pathlib.Path], workflow: workflow.Workflow) -> None:
    config_paths["txt"].write_text(pretty_print.PrettyPrinter().format(workflow))


def serialize_nml(config_paths: dict[str, pathlib.Path], workflow: workflow.Workflow) -> None:
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    for task in workflow.tasks:
        if isinstance(task, core_tasks.icon_task.IconTask):
            task.create_workflow_namelists(folder=nml_refdir)


def pytest_configure(config):
    if config.getoption("reserialize"):
        print("Regenerating serialized references")  # noqa: T201 # this is actual UX, not a debug print
        for config_case in ALL_CONFIG_CASES:
            config_paths = generate_config_paths(config_case)
            wf = workflow.Workflow.from_config_file(str(config_paths["yml"]))
            serialize_worklfow(config_paths=config_paths, workflow=wf)
            serialize_nml(config_paths=config_paths, workflow=wf)
