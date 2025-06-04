from pathlib import Path

import pytest

from sirocco.core import Workflow
from sirocco.core._tasks.icon_task import IconTask
from sirocco.vizgraph import VizGraph
from sirocco.workgraph import AiidaWorkGraph


def test_parse_config_file(config_paths, pprinter):
    reference_str = config_paths["txt"].read_text()
    test_str = pprinter.format(Workflow.from_config_file(config_paths["yml"]))
    if test_str != reference_str:
        new_path = Path(config_paths["txt"]).with_suffix(".new.txt")
        new_path.write_text(test_str)
        assert (
            reference_str == test_str
        ), f"Workflow graph doesn't match serialized data. New graph string dumped to {new_path}."


def test_vizgraph(config_paths):
    VizGraph.from_config_file(config_paths["yml"]).draw(file_path=config_paths["svg"])


@pytest.mark.requires_icon
@pytest.mark.usefixtures("icon_filepath_executable", "icon_grid_path")
def test_icon():
    # Test is performed by fixtures
    pass


# configs that are tested for running workgraph
@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "configure_aiida_localhost")
@pytest.mark.parametrize(
    "config_case",
    [
        "small-shell",
        "parameters",
    ],
)
def test_run_workgraph(config_paths):
    """Tests end-to-end the parsing from file up to running the workgraph.

    Automatically uses the aiida_profile fixture to create a new profile. Note to debug the test with your profile
    please run this in a separate file as the profile is deleted after test finishes.
    """
    core_workflow = Workflow.from_config_file(str(config_paths["yml"]))
    aiida_workflow = AiidaWorkGraph(core_workflow)
    output_node = aiida_workflow.run()
    assert (
        output_node.is_finished_ok
    ), f"Not successful run. Got exit code {output_node.exit_code} with message {output_node.exit_message}."


@pytest.mark.requires_icon
@pytest.mark.usefixtures("config_case", "configure_aiida_localhost")
@pytest.mark.parametrize(
    "config_case",
    [
        "small-icon",
    ],
)
def test_run_workgraph_with_icon(icon_filepath_executable, config_paths, tmp_path):
    """Tests end-to-end the parsing from file up to running the workgraph.

    Automatically uses the aiida_profile fixture to create a new profile. Note to debug the test with your profile
    please run this in a separate file as the profile is deleted after test finishes.
    """
    config_rootdir = config_paths["yml"].parent.absolute()
    tmp_config_rootdir = tmp_path / config_rootdir.name
    tmp_config_rootdir.symlink_to(config_rootdir)

    # we link the icon executable to the test case path
    tmp_icon_bin_path = tmp_config_rootdir / "./ICON/bin/icon"
    if tmp_icon_bin_path.exists():
        tmp_icon_bin_path.unlink()
    tmp_icon_bin_path.symlink_to(Path(icon_filepath_executable))

    core_workflow = Workflow.from_config_file(tmp_config_rootdir / config_paths["yml"].name)
    aiida_workflow = AiidaWorkGraph(core_workflow)
    output_node = aiida_workflow.run()
    assert (
        output_node.is_finished_ok
    ), f"Not successful run. Got exit code {output_node.exit_code} with message {output_node.exit_message}."


# configs containing task using icon plugin
@pytest.mark.usefixtures("config_case")
@pytest.mark.parametrize(
    "config_case",
    ["large"],
)
def test_nml_mod(config_paths, tmp_path):
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    wf = Workflow.from_config_file(config_paths["yml"])
    # Create core mamelists
    for task in wf.tasks:
        if isinstance(task, IconTask):
            task.dump_namelists(directory=tmp_path)
    # Compare against reference
    for nml in nml_refdir.glob("*"):
        ref_nml = nml.read_text()
        test_nml = (tmp_path / nml.name).read_text()
        if test_nml != ref_nml:
            new_path = nml.with_suffix(".new")
            new_path.write_text(test_nml)
            assert ref_nml == test_nml, f"Namelist {nml.name} differs between ref and test"
