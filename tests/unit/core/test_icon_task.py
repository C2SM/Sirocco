"""Unit tests for ICON task functionality."""

import f90nml
import pytest

from sirocco.core import Workflow
from sirocco.core.tasks.icon_task import IconTask
from tests.unit import utils as test_utils


# configs containing task using icon plugin
@pytest.mark.usefixtures("config_case")
@pytest.mark.parametrize(
    "config_case",
    ["large", "coupled", "restart"],
)
def test_nml_mod(config_paths, tmp_path):
    """Test ICON namelist generation from IconTask."""
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    wf = Workflow.from_config_path(config_paths["dir"])
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


def test_checkpoint_interval_defaults_to_cycle_period(tmp_path):
    """Without checkpointTimeIntval in the master namelist, checkpoints follow the cycle period."""
    task = test_utils.create_icon_task_from_workflow(tmp_path)
    assert task.master_namelist["master_time_control_nml"]["checkpointTimeIntval"] == "P1D"


@pytest.mark.parametrize("config_case", ["restart"])
@pytest.mark.parametrize(
    ("mode", "writers", "link_name"),
    [
        ("sync", 0, "restart_atm_DOM01.nc"),
        ("async", 1, "restart_atm_DOM01.nc"),
        ("joint procs multifile", 0, "multifile_restart_atm.mfr"),
        ("dedicated procs multifile", 1, "multifile_restart_atm.mfr"),
    ],
)
def test_restart_files(config_paths, mode, writers, link_name):
    """Prepare two chunks and link the second to the first chunk's checkpoint."""
    wf = Workflow.from_config_path(
        config_paths["dir"], template_context={"RESTART_MODE": mode, "RESTART_PROCS": writers}
    )
    first, second = sorted(wf.tasks, key=lambda task: task.cycle_point.chunk_start_date)
    assert first.models["atm"].inputs["restart_in"] == []
    assert second.models["atm"].inputs["restart_in"] == first.models["atm"].outputs["restart_out"]

    first.run_dir.mkdir(parents=True)
    first.prepare_for_submission()
    output = first.models["atm"].outputs["restart_out"][0]
    assert output.resolved_path == first.run_dir / link_name

    # Emulate ICON writing a checkpoint without running the model
    checkpoint = first.run_dir / "checkpoint"
    if link_name.endswith(".mfr"):
        checkpoint.mkdir()
    else:
        checkpoint.touch()
    output.resolved_path.symlink_to(checkpoint)

    second.run_dir.mkdir(parents=True)
    second.prepare_for_submission()
    link = second.run_dir / link_name
    assert link.is_symlink()
    assert link.readlink() == output.resolved_path
    assert link.resolve() == checkpoint

    for task, restarting in ((first, False), (second, True)):
        master = f90nml.read(task.run_dir / "icon_master.namelist")
        model = f90nml.read(task.run_dir / "model.namelist")
        assert master["master_nml"]["lrestart"] is restarting
        assert model["parallel_nml"]["num_restart_procs"] == writers
        assert sum(info["pe_type"] == "io" for info in task.ranks_info.values()) == writers
