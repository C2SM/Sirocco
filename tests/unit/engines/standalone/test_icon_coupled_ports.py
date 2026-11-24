"""ICON port handlers of the standalone engine for coupled atmosphere/ocean/land setups."""

from itertools import chain
from pathlib import Path

import pytest

from sirocco.core.tasks.icon_task import IconTask
from sirocco.engines.standalone import StandaloneEngine
from sirocco.engines.standalone.plugin_handlers.icon_handler import IconHandler


@pytest.fixture
def icon_handlers(config_paths) -> list[IconHandler]:
    engine = StandaloneEngine.from_config_path(config_paths["dir"], template_context=config_paths["variables"])
    task_handlers = [engine.task_handlers[task] for task in engine.workflow.tasks if isinstance(task, IconTask)]
    for task_handler in task_handlers:
        task_handler.get_rundir().mkdir(parents=True)
    return [task_handler for task_handler in task_handlers if isinstance(task_handler, IconHandler)]


@pytest.fixture
def applied_handlers(icon_handlers) -> list[IconHandler]:
    for task_handler in icon_handlers:
        for model in task_handler.task.models.values():
            for port_name in chain(model.inputs, model.outputs):
                task_handler.apply_port(port_name, model)
    return icon_handlers


@pytest.fixture
def data_dir(config_paths) -> Path:
    return config_paths["dir"] / "data"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_fixed_name_links(applied_handlers, data_dir):
    rundir = applied_handlers[0].get_rundir()
    assert (rundir / "bc_greenhouse_gases.nc").readlink() == data_dir / "ghg"
    assert (rundir / "lctlib_nlct21.def").readlink() == data_dir / "lctlib"
    assert (rundir / "dust.nc").readlink() == data_dir / "dust"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_ocean_restart_init_first_chunk(applied_handlers, data_dir):
    first = applied_handlers[0]
    assert not first.task.is_restart
    assert (first.get_rundir() / "multifile_restart_ocean.mfr").readlink() == data_dir / "spinup_restart"


@pytest.mark.parametrize("config_case", ["coupled"])
def test_ocean_restart_init_requires_namelist_flag(icon_handlers):
    task_handler = icon_handlers[0]
    ocean = task_handler.task.models["ocean"]
    ocean.namelist["ocean_initialConditions_nml"]["initialize_fromRestart"] = False
    with pytest.raises(ValueError, match=r"requires 'initialize_fromRestart = \.true\.'"):
        task_handler.apply_port("ocean_restart_init", ocean)


@pytest.mark.parametrize("config_case", ["coupled"])
def test_restart_chunk(applied_handlers):
    first, second = applied_handlers
    assert second.task.is_restart
    assert (second.get_rundir() / "multifile_restart_ocean.mfr").readlink() == (
        first.get_rundir() / "multifile_restart_ocean.mfr"
    )
