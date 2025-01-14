from sirocco import pretty_print
from sirocco.core import workflow
from sirocco.parsing import _yaml_data_models as models


def test_minimal_workflow():
    minimal_config = models.ConfigWorkflow(
        cycles=[],
        tasks=[{"some_task": {"plugin": "shell"}}],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(foo={})],
            generated=[models.ConfigGeneratedData(bar={})],
        ),
    )

    testee = workflow.Workflow(minimal_config)

    pretty_print.PrettyPrinter().format(testee)

    assert testee.name is None
    assert len(list(testee.tasks)) == 0
    assert len(list(testee.cycles)) == 0
    assert testee.data[("foo", {})].available
