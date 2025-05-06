from sirocco import pretty_print
from sirocco.core import AvailableData, Workflow

# NOTE: import of ShellTask is required to populated in Task.plugin_classes in __init_subclass__
from sirocco.core._tasks.shell_task import ShellTask  # noqa: F401


def test_minimal_workflow(minimal_config):
    testee = Workflow.from_config_workflow(minimal_config)

    pretty_print.PrettyPrinter().format(testee)

    assert len(list(testee.tasks)) == 1
    assert len(list(testee.cycles)) == 1
    assert isinstance(testee.data[("available", {})], AvailableData)
    assert testee.config_rootdir == minimal_config.rootdir


def test_invert_task_io_workflow(minimal_invert_task_io_config):
    testee = Workflow.from_config_workflow(minimal_invert_task_io_config)
    pretty_print.PrettyPrinter().format(testee)
