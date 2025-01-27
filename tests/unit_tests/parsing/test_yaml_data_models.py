import textwrap

import pytest
from pydantic import ValidationError

from sirocco.parsing import _yaml_data_models as models


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
        data:
          available:
            - c:
          generated:
            - d:
        """
    )
    minimal = tmp_path / "minimal.yml"
    minimal.write_text(minimal_config)
    return minimal


def test_load_workflow_config(minimal_config_path):
    testee = models.ConfigWorkflow.from_config_file(str(minimal_config_path))
    assert testee.name == "minimal"
    assert testee.rootdir == minimal_config_path.parent


def test_name_none_fail(minimal_config):
    """Test that `ConfigWorkflow` fails if rootdir is None."""

    with pytest.raises(ValidationError, match=r".*1 validation error for ConfigWorkflow\nname.*"):
        models.ConfigWorkflow(
            name=None,
            rootdir=minimal_config.rootdir,
            cycles=minimal_config.cycles,
            tasks=minimal_config.tasks,
            data=minimal_config.data,
            parameters=minimal_config.parameters,
        )


def test_rootdir_none_fail(minimal_config):
    """Test that `ConfigWorkflow` fails if rootdir is None."""

    with pytest.raises(ValidationError, match=r".*1 validation error for ConfigWorkflow\nrootdir.*"):
        models.ConfigWorkflow(
            name=minimal_config.name,
            rootdir=None,
            cycles=minimal_config.cycles,
            tasks=minimal_config.tasks,
            data=minimal_config.data,
            parameters=minimal_config.parameters,
        )
