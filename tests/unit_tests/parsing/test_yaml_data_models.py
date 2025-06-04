import textwrap

import pytest

from sirocco.parsing import yaml_data_models as models


def test_base_data():
    models.ConfigBaseData(name="name", src="foo.txt", format=None)


@pytest.fixture
def minimal_config_path(tmp_path):
    minimal_config = textwrap.dedent(
        """
        cycles:
          - minimal:
              tasks:
                - a:
        tasks:
          - b:
              plugin: shell
              computer: localhost
              command: some_command
        data:
          available:
            - c:
                computer: "localhost"
                src: "c.txt"
          generated:
            - d:
                src: "d"
        """
    )
    minimal = tmp_path / "minimal.yml"
    minimal.write_text(minimal_config)
    return minimal


def test_load_workflow_config(minimal_config_path):
    testee = models.ConfigWorkflow.from_config_file(str(minimal_config_path))
    assert testee.name == "minimal"
    assert testee.rootdir == minimal_config_path.parent


def test_file_does_not_exist(tmp_path):
    """Test that `ConfigWorkflow` fails if rootdir is None."""
    nonexistant_file = str(tmp_path / "nonexistent_file")
    with pytest.raises(FileNotFoundError, match=r".*nonexistent_file does not exist.*"):
        _ = models.ConfigWorkflow.from_config_file(str(nonexistant_file))


def test_from_config_file_is_not_file(tmp_path):
    directory = tmp_path / "dir"
    directory.mkdir()
    with pytest.raises(FileNotFoundError, match=r".*dir is not a file.*"):
        _ = models.ConfigWorkflow.from_config_file(str(directory))


def test_from_config_file_is_empty(tmp_path):
    empty_file = tmp_path / "empty_file"
    empty_file.write_text("")
    with pytest.raises(ValueError, match=r".*empty_file is empty.*"):
        _ = models.ConfigWorkflow.from_config_file(empty_file)
