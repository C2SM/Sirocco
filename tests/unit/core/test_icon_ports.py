"""Unit tests for ICON port handlers of coupled atmosphere/ocean/land setups."""

from itertools import chain
from pathlib import Path

import pytest

from sirocco.core import Workflow
from sirocco.core.tasks.icon_task.ports import PortHandler


@pytest.fixture
def coupled_tasks(config_paths):
    wf = Workflow.from_config_path(config_paths["dir"], template_context=config_paths["variables"])
    tasks = list(wf.tasks)
    for task in tasks:
        task.resolve_output_data_paths()
        task.run_dir.mkdir(parents=True)
        for model in task.models.values():
            for port in chain(model.inputs, model.outputs):
                PortHandler.handle(port, model)
    return tasks


@pytest.fixture
def data_dir(config_paths) -> Path:
    return config_paths["dir"] / "data"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_fixed_name_links(coupled_tasks, data_dir):
    run_dir = coupled_tasks[0].run_dir
    assert (run_dir / "bc_greenhouse_gases.nc").readlink() == data_dir / "ghg"
    assert (run_dir / "lctlib_nlct21.def").readlink() == data_dir / "lctlib"
    assert (run_dir / "dust.nc").readlink() == data_dir / "dust"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_ocean_restart_init_first_chunk(coupled_tasks, data_dir):
    first = coupled_tasks[0]
    assert not first.is_restart
    assert first.models["ocean"].namelist["ocean_initialConditions_nml"]["initialize_fromRestart"]
    assert (first.run_dir / "multifile_restart_ocean.mfr").readlink() == data_dir / "spinup_restart"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_restart_chunk(coupled_tasks):
    first, second = coupled_tasks
    assert second.is_restart
    assert not second.models["ocean"].namelist["ocean_initialConditions_nml"]["initialize_fromRestart"]
    assert (second.run_dir / "multifile_restart_ocean.mfr").readlink() == first.run_dir / "multifile_restart_ocean.mfr"
