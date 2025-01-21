import pathlib

from sirocco import pretty_print
from sirocco.core import Workflow

# NOTE: import of ShellTask is required to populated in Task.plugin_classes in __init_subclass__
from sirocco.core._tasks.shell_task import ShellTask  # noqa: F401
from sirocco.parsing import _yaml_data_models as models


def test_minimal_workflow():
    minimal_config = models.ConfigWorkflow(
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

    testee = Workflow.from_config_workflow(minimal_config)

    pretty_print.PrettyPrinter().format(testee)

    assert len(list(testee.tasks)) == 1
    assert len(list(testee.cycles)) == 1
    assert testee.data[("foo", {})].available
