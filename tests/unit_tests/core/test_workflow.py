import pathlib

from sirocco import pretty_print
from sirocco.core import workflow
from sirocco.parsing import _yaml_data_models as models


def test_minimal_workflow():
    minimal_config = models.CanonicalWorkflow(
        name="minimal",
        rootdir=pathlib.Path("minimal"),
        cycles=[models.ConfigCycle(some_cycle={"tasks": []})],
        tasks=[models.ConfigShellTask(some_task={"plugin": "shell"})],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(foo={})],
            generated=[models.ConfigGeneratedData(bar={})],
        ),
        parameters={},
    )

    testee = workflow.Workflow(minimal_config)

    pretty_print.PrettyPrinter().format(testee)

    assert len(list(testee.tasks)) == 0
    assert len(list(testee.cycles)) == 1
    assert testee.data[("foo", {})].available
