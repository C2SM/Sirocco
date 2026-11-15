import pytest

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


def test_sirocco_task_used(minimal_config):
    config = minimal_config.model_copy(update={"tasks": [*minimal_config.tasks, SIROCCO_TASK]})
    engine = StandaloneEngine.from_config_workflow(config)
    assert engine.sirocco_continue_task.computer == "localhost"
    assert engine.sirocco_continue_task.walltime == "00:05:00"
