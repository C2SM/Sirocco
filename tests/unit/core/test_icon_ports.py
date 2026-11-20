"""Unit tests for ICON port handlers of coupled atmosphere/ocean/land setups."""

from itertools import chain
from pathlib import Path

import pytest

from sirocco.core import Workflow
from sirocco.core.tasks.icon_task import IconTask
from sirocco.core.tasks.icon_task.ports import PortHandler


@pytest.fixture
def coupled_tasks(config_paths) -> list[IconTask]:
    wf = Workflow.from_config_path(config_paths["dir"], template_context=config_paths["variables"])
    tasks = [task for task in wf.tasks if isinstance(task, IconTask)]
    for task in tasks:
        task.resolve_output_data_paths()
        task.run_dir.mkdir(parents=True)
    return tasks


@pytest.fixture
def handled_tasks(coupled_tasks) -> list[IconTask]:
    for task in coupled_tasks:
        for model in task.models.values():
            for port in chain(model.inputs, model.outputs):
                PortHandler.handle(port, model)
    return coupled_tasks


@pytest.fixture
def data_dir(config_paths) -> Path:
    return config_paths["dir"] / "data"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_fixed_name_links(handled_tasks, data_dir):
    run_dir = handled_tasks[0].run_dir
    assert (run_dir / "bc_greenhouse_gases.nc").readlink() == data_dir / "ghg"
    assert (run_dir / "lctlib_nlct21.def").readlink() == data_dir / "lctlib"
    assert (run_dir / "dust.nc").readlink() == data_dir / "dust"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_ocean_restart_init_first_chunk(handled_tasks, data_dir):
    first = handled_tasks[0]
    assert not first.is_restart
    assert (first.run_dir / "multifile_restart_ocean.mfr").readlink() == data_dir / "spinup_restart"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_ocean_restart_init_requires_namelist_flag(coupled_tasks):
    ocean = coupled_tasks[0].models["ocean"]
    ocean.namelist["ocean_initialConditions_nml"]["initialize_fromRestart"] = False
    with pytest.raises(ValueError, match=r"requires 'initialize_fromRestart = \.true\.'"):
        PortHandler.handle("ocean_restart_init", ocean)


@pytest.mark.parametrize("config_case", ["coupled"])
def test_restart_chunk(handled_tasks):
    first, second = handled_tasks
    assert second.is_restart
    assert (second.run_dir / "multifile_restart_ocean.mfr").readlink() == first.run_dir / "multifile_restart_ocean.mfr"
