import pathlib

import pytest

from sirocco.parsing import _yaml_data_models as models

pytest_plugins = ["aiida.tools.pytest_fixtures"]


@pytest.fixture(scope="session")
def minimal_config():
    return models.ConfigWorkflow(
        name="minimal",
        rootdir=pathlib.Path("minimal"),
        cycles=[models.ConfigCycle(minimal={"tasks": [models.ConfigCycleTask(some_task={})]})],
        tasks=[models.ConfigShellTask(some_task={"plugin": "shell"})],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(foo={})],
            generated=[models.ConfigGeneratedData(bar={})],
        ),
        parameters={},
    )
