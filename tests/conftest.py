import pathlib

import pytest

from sirocco.parsing import _yaml_data_models as models

pytest_plugins = ["aiida.tools.pytest_fixtures"]


@pytest.fixture(scope="session")
def minimal_config() -> models.ConfigWorkflow:
    return models.ConfigWorkflow(
        name="minimal",
        rootdir=pathlib.Path("minimal"),
        cycles=[models.ConfigCycle(name="minimal", tasks=[models.ConfigCycleTask(name="some_task")])],
        tasks=[models.ConfigShellTask(name="some_task")],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(name="foo", type=models.DataType.FILE, src="foo.txt")],
            generated=[models.ConfigGeneratedData(name="bar", type=models.DataType.DIR, src="bar")],
        ),
        parameters={},
    )
