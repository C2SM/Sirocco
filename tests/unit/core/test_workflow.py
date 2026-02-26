from pathlib import Path

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


def test_parse_config_file(config_paths, pprinter):
    """Test parsing workflow from YAML config file and comparing with reference output."""
    reference_str = config_paths["txt"].read_text()
    test_str = pprinter.format(
        Workflow.from_config_file(config_paths["yml"], template_context=config_paths["variables"])
    )

    # Normalize paths: replace tmp_path with placeholder to match reference files
    tests_rootdir = config_paths["variables"]["TESTS_ROOTDIR"]
    test_str_normalized = test_str.replace(str(tests_rootdir), "/TESTS_ROOTDIR")

    if test_str_normalized != reference_str:
        new_path = Path(config_paths["txt"]).with_suffix(".new.txt")
        new_path.write_text(test_str_normalized)
        assert reference_str == test_str_normalized, (
            f"Workflow graph doesn't match serialized data. New graph string dumped to {new_path}."
        )
