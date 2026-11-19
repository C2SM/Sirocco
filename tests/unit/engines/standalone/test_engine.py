import shutil
from pathlib import Path

import pytest

from sirocco.core.graph_items import AvailableData, GeneratedData
from sirocco.engines.standalone import StandaloneEngine
from sirocco.parsing import yaml_data_models as models


def test_sirocco_task_required(minimal_config):
    with pytest.raises(ValueError, match="requires the special SIROCCO task"):
        StandaloneEngine.from_config_workflow(minimal_config)


SIROCCO_TASK = models.ConfigSiroccoTask(name="SIROCCO", computer="localhost", walltime="00:05:00")


def test_scheduler_required(minimal_config):
    config = minimal_config.model_copy(update={"tasks": [*minimal_config.tasks, SIROCCO_TASK], "scheduler": None})
    with pytest.raises(ValueError, match="requires the scheduler to be specified"):
        StandaloneEngine.from_config_workflow(config)


@pytest.fixture
def engine(minimal_config) -> StandaloneEngine:
    config = minimal_config.model_copy(update={"tasks": [*minimal_config.tasks, SIROCCO_TASK]})
    return StandaloneEngine.from_config_workflow(config)


def test_sirocco_task_used(engine):
    assert engine.sirocco_continue_task.computer == "localhost"
    assert engine.sirocco_continue_task.walltime == "00:05:00"


# =========== Data paths ===========

DATA = GeneratedData(name="some_data", coordinates={})


def test_set_path_again_to_same_path(tmp_path, engine):
    engine.data_paths[DATA] = tmp_path / "a"
    engine.data_paths[DATA] = tmp_path / "a"
    assert engine.data_paths[DATA] == tmp_path / "a"


def test_set_path_conflict(tmp_path, engine):
    engine.data_paths[DATA] = tmp_path / "a"
    with pytest.raises(ValueError, match="already resolved to"):
        engine.data_paths[DATA] = tmp_path / "b"
    assert engine.data_paths[DATA] == tmp_path / "a"


def test_set_path_relative(engine):
    with pytest.raises(ValueError, match="must be absolute"):
        engine.data_paths[DATA] = Path("relative")


def test_available_data_path(engine):
    data = AvailableData(name="available", coordinates={}, path=Path("/foo.txt"), computer="localhost")
    assert engine.data_paths[data] == Path("/foo.txt")


def test_generated_data_path_resolved_by_origin_task(tmp_path, test_rootdir):
    """The path of a generated data is resolved on demand by the handler of its origin task"""
    config_dir = tmp_path / "config"
    shutil.copytree(test_rootdir / "tests/cases/parameters/config", config_dir)
    engine = StandaloneEngine.from_config_path(config_dir, template_context={"TESTS_ROOTDIR": str(test_rootdir)})
    data = next(data for data in engine.workflow.data if isinstance(data, GeneratedData))
    assert data.path is not None
    assert not data.path.is_absolute()
    assert engine.data_paths[data] == engine.task_handlers[data.origin_task].get_rundir() / data.path
