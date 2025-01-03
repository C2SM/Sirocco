from pathlib import Path

import pytest

from sirocco.core import Workflow
from sirocco.parsing._yaml_data_models import ConfigShellTask, ShellCliArgument
from sirocco.pretty_print import PrettyPrinter
from sirocco.vizgraph import VizGraph
from sirocco.workgraph import AiidaWorkGraph


# configs that are tested for parsing
def test_parsing_cli_parameters():
    cli_arguments = "-D --CMAKE_CXX_COMPILER=${CXX_COMPILER} {--init file}"
    assert ConfigShellTask.split_cli_arguments(cli_arguments) == [
        "-D",
        "--CMAKE_CXX_COMPILER=${CXX_COMPILER}",
        "{--init file}",
    ]

    assert ConfigShellTask.parse_cli_arguments(cli_arguments) == [
        ShellCliArgument("-D", False, None),
        ShellCliArgument("--CMAKE_CXX_COMPILER=${CXX_COMPILER}", False, None),
        ShellCliArgument("file", True, "--init"),
    ]


@pytest.fixture
def pprinter():
    return PrettyPrinter()


# configs that are tested for parsing
config_test_files = [
    "tests/cases/small/config/test_config_small.yml",
    "tests/cases/large/config/test_config_large.yml",
    "tests/cases/parameters/config/test_config_parameters.yml",
]


@pytest.fixture(params=config_test_files)
def config_paths(request):
    config_path = Path(request.param)
    return {
        "yml": config_path,
        "txt": (config_path.parent.parent / "data" / config_path.name).with_suffix(".txt"),
        "svg": (config_path.parent.parent / "svg" / config_path.name).with_suffix(".svg"),
    }


def test_parse_config_file(config_paths, pprinter):
    reference_str = config_paths["txt"].read_text()
    test_str = pprinter.format(Workflow.from_yaml(config_paths["yml"]))
    if test_str != reference_str:
        new_path = Path(config_paths["txt"]).with_suffix(".new.txt")
        new_path.write_text(test_str)
        assert (
            reference_str == test_str
        ), f"Workflow graph doesn't match serialized data. New graph string dumped to {new_path}."


@pytest.mark.skip(reason="don't run it each time, uncomment to regenerate serilaized data")
def test_serialize_workflow(config_paths, pprinter):
    config_paths["txt"].write_text(pprinter.format(Workflow.from_yaml(config_paths["yml"])))


def test_vizgraph(config_paths):
    VizGraph.from_yaml(config_paths["yml"]).draw(file_path=config_paths["svg"])


# configs that are tested for running workgraph
@pytest.mark.parametrize(
    "config_path",
    [
        "tests/cases/small/config/test_config_small.yml",
        "tests/cases/parameters/config/test_config_parameters.yml",
    ],
)
def test_run_workgraph(config_path, aiida_computer):
    """Tests end-to-end the parsing from file up to running the workgraph.

    Automatically uses the aiida_profile fixture to create a new profile. Note to debug the test with your profile
    please run this in a separate file as the profile is deleted after test finishes.
    """
    # some configs reference computer "localhost" which we need to create beforehand
    aiida_computer("localhost").store()

    core_workflow = Workflow.from_yaml(config_path)
    aiida_workflow = AiidaWorkGraph(core_workflow)
    out = aiida_workflow.run()
    assert out.get("execution_count", None).value == 1
