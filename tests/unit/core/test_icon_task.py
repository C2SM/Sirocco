"""Unit tests for ICON task functionality."""

import pytest

from sirocco.core import Workflow
from sirocco.core._tasks.icon_task import IconTask


# configs containing task using icon plugin
@pytest.mark.usefixtures("config_case")
@pytest.mark.parametrize(
    "config_case",
    ["large"],
)
def test_nml_mod(config_paths, tmp_path):
    """Test ICON namelist generation from IconTask."""
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    wf = Workflow.from_config_file(config_paths["yml"], template_context=config_paths["variables"])
    # Create core namelists
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
