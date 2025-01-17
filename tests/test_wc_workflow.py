from pathlib import Path

import pytest

from sirocco.core import Workflow
from sirocco.core._tasks.icon_task import IconTask
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


def generate_config_paths(test_case: str):
    return {
        "yml": Path(f"tests/cases/{test_case}/config/config.yml"),
        "txt": Path(f"tests/cases/{test_case}/data/config.txt"),
        "svg": Path(f"tests/cases/{test_case}/svg/config.svg"),
    }


# configs that are tested for parsing
all_uses_cases = ["small", "parameters", "large"]


@pytest.fixture(params=all_uses_cases)
def config_paths(request):
    return generate_config_paths(request.param)


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
        "tests/cases/small/config/config.yml",
        "tests/cases/parameters/config/config.yml",
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


# configs containing task using icon plugin
@pytest.mark.parametrize(
    "config_paths",
    [generate_config_paths("large")],
)
def test_nml_mod(config_paths, tmp_path):
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    wf = Workflow.from_yaml(config_paths["yml"])
    # Create core mamelists
    for task in wf.tasks:
        if isinstance(task, IconTask):
            task.create_workflow_namelists(folder=tmp_path)
    # Compare against reference
    for nml in nml_refdir.glob("*"):
        ref_nml = nml.read_text()
        test_nml = (tmp_path / nml.name).read_text()
        if test_nml != ref_nml:
            new_path = nml.with_suffix(".new")
            new_path.write_text(test_nml)
            assert ref_nml == test_nml, f"Namelist {nml.name} differs between ref and test"


@pytest.mark.skip(reason="don't run it each time, uncomment to regenerate serilaized data")
# configs containing task using icon plugin
@pytest.mark.parametrize(
    "config_paths",
    [generate_config_paths("large")],
)
def test_serialize_nml(config_paths):
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    wf = Workflow.from_yaml(config_paths["yml"])
    for task in wf.tasks:
        if isinstance(task, IconTask):
            task.create_workflow_namelists(folder=nml_refdir)
