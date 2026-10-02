import pytest

from sirocco.parsing import yaml_data_models as models


def test_base_data():
    models.ConfigBaseData(name="name", format=None)


def test_load_workflow_config(minimal_config_path):
    testee = models.ConfigWorkflow.from_config_path(minimal_config_path)
    assert testee.name == "minimal"
    assert testee.rootdir == minimal_config_path


def test_file_does_not_exist(tmp_path):
    """Test that `ConfigWorkflow` fails if rootdir is None."""
    nonexistant_dir = tmp_path / "nonexistent_dir"
    with pytest.raises(FileNotFoundError, match=r".*nonexistent_dir does not exist.*"):
        _ = models.ConfigWorkflow.from_config_path(nonexistant_dir)


def test_from_config_file_is_not_file(tmp_path):
    file = tmp_path / "sirocco.yaml"
    file.touch()
    with pytest.raises(FileNotFoundError, match=r".*sirocco.yaml is not a directory.*"):
        _ = models.ConfigWorkflow.from_config_path(file)


def test_from_config_file_is_empty(tmp_path):
    empty_config = tmp_path / "empty_config"
    empty_config.mkdir(exist_ok=True)
    (empty_config / "sirocco.yaml").touch()
    with pytest.raises(ValueError, match=rf".*{models.ConfigWorkflow._CONFIG_FILENAME} is empty.*"):
        _ = models.ConfigWorkflow.from_config_path(empty_config)
