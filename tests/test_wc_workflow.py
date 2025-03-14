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


# configs that are tested for running workgraph
@pytest.mark.slow
@pytest.mark.parametrize(
    "config_case",
    [
        "small",
        "parameters",
    ],
)
def test_run_workgraph(config_case, config_paths, aiida_computer):  # noqa: ARG001  # config_case is overridden
    """Tests end-to-end the parsing from file up to running the workgraph.

    Automatically uses the aiida_profile fixture to create a new profile. Note to debug the test with your profile
    please run this in a separate file as the profile is deleted after test finishes.
    """
    # some configs reference computer "localhost" which we need to create beforehand
    aiida_computer("localhost").store()

    core_workflow = Workflow.from_config_file(str(config_paths["yml"]))
    aiida_workflow = AiidaWorkGraph(core_workflow)
    out = aiida_workflow.run()
    assert out.get("execution_count", None).value == 1


# configs containing task using icon plugin
@pytest.mark.parametrize(
    "config_case",
    ["large"],
)
def test_nml_mod(config_case, config_paths, tmp_path):  # noqa: ARG001  # config_case is overridden
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    wf = Workflow.from_config_file(config_paths["yml"])
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
