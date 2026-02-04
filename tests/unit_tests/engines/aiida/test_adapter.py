"""Unit tests for sirocco.engines.aiida.adapter module."""

from unittest.mock import Mock, patch

import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter


def create_mock_shell_task(**overrides):
    """Create a mock ShellTask with default attributes.

    Args:
        **overrides: Attribute overrides for the task

    Returns:
        Mock ShellTask with default attributes and any overrides applied
    """
    task = Mock(spec=core.ShellTask)

    # Set defaults for common attributes
    defaults = {
        "computer": "test_computer",
        "walltime": None,
        "mem": None,
        "partition": None,
        "account": None,
        "nodes": None,
        "ntasks_per_node": None,
        "cpus_per_task": None,
        "uenv": None,
        "view": None,
        "path": None,
        "command": None,
    }

    # Apply overrides
    defaults.update(overrides)

    # Set attributes on mock
    for key, value in defaults.items():
        setattr(task, key, value)

    # Add default cycle_point if not in overrides
    if "cycle_point" not in overrides:
        cycle_point = Mock()
        cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.cycle_point = cycle_point

    return task


def create_mock_icon_task(**overrides):
    """Create a mock IconTask with default attributes.

    Args:
        **overrides: Attribute overrides for the task

    Returns:
        Mock IconTask with default attributes and any overrides applied
    """
    task = Mock(spec=core.IconTask)

    # Set defaults for common attributes
    defaults = {
        "computer": "test_computer",
        "walltime": None,
        "mem": None,
        "partition": None,
        "account": None,
        "nodes": None,
        "ntasks_per_node": None,
        "cpus_per_task": None,
        "uenv": None,
        "view": None,
        "path": None,
        "command": None,
    }

    # Apply overrides
    defaults.update(overrides)

    # Set attributes on mock
    for key, value in defaults.items():
        setattr(task, key, value)

    # Add default cycle_point if not in overrides
    if "cycle_point" not in overrides:
        cycle_point = Mock()
        cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.cycle_point = cycle_point

    return task


@pytest.mark.parametrize(
    ("input_name", "expected"),
    [
        # Scripting languages - extensions should be removed
        ("script.sh", "script"),  # Shell/Bash
        ("script.py", "script"),  # Python
        ("script.pl", "script"),  # Perl
        ("script.rb", "script"),  # Ruby
        ("script.R", "script"),  # R
        ("script.lua", "script"),  # Lua
        ("script.js", "script"),  # JavaScript/Node
        ("script.tcl", "script"),  # TCL
        # Non-scripting extensions - should be preserved
        ("script.txt", "script.txt"),
        ("script.yaml", "script.yaml"),
        ("script.json", "script.json"),
        # No extension - should be unchanged
        ("script", "script"),
        # Edge cases
        ("run_model.sh", "run_model"),
        ("process.py", "process"),
    ],
)
def test_remove_script_extension(input_name, expected):
    """Test script extension removal for common scripting languages."""
    assert AiidaAdapter.remove_script_extension(input_name) == expected


@pytest.mark.parametrize(
    ("name", "coordinates", "expected_in_label", "not_in_label"),
    [
        # Simple case - no coordinates
        ("test_task", {}, ["test_task"], []),
        # With coordinates
        ("test_task", {"member": 0, "date": "2026-01-01"}, ["test_task", "member_0"], []),
        # Invalid characters should be replaced
        ("test-task.v1", {"date": "2026-01-01"}, ["test_task_v1"], ["-", "."]),
    ],
)
def test_build_graph_item_label(name, coordinates, expected_in_label, not_in_label):
    """Test label generation for various graph items."""
    item = Mock()
    item.name = name
    item.coordinates = coordinates

    label = AiidaAdapter.build_graph_item_label(item)

    for expected in expected_in_label:
        assert expected in label
    for not_expected in not_in_label:
        assert not_expected not in label


def test_translate_mpi_placeholder():
    """Test MPI placeholder translation."""
    result = AiidaAdapter.translate_mpi_placeholder(core.MpiCmdPlaceholder.MPI_TOTAL_PROCS)
    assert result == "tot_num_mpiprocs"


def test_build_scheduler_options_basic():
    """Test building scheduler options without special flags."""
    task = create_mock_shell_task(
        walltime="01:00:00",
        mem=4096,
        partition="normal",
        account="test_account",
        nodes=2,
        ntasks_per_node=12,
        cpus_per_task=1,
    )

    options = AiidaAdapter.build_scheduler_options(task)

    print("\n=== Scheduler options ===")
    pprint(options)
    print("\n=== Resources ===")
    pprint(options.resources)

    assert options.max_wallclock_seconds == 3600  # 1 hour
    assert options.max_memory_kb == 4096 * 1024
    assert options.queue_name == "normal"
    assert options.resources.num_machines == 2
    assert options.resources.num_mpiprocs_per_machine == 12
    assert options.resources.num_cores_per_mpiproc == 1


def test_build_scheduler_options_with_uenv_view():
    """Test building scheduler options with uenv and view."""
    task = create_mock_icon_task(
        uenv="icon-wcp/v1:rc4",
        view="icon",
    )

    options = AiidaAdapter.build_scheduler_options(task)

    print("\n=== Custom scheduler commands ===")
    print(options.custom_scheduler_commands)

    assert "#SBATCH --uenv=icon-wcp/v1:rc4" in options.custom_scheduler_commands
    assert "#SBATCH --view=icon" in options.custom_scheduler_commands


@patch("aiida.orm.Computer")
def test_build_metadata(mock_computer):
    """Test building metadata for a task."""
    # Setup mock computer
    mock_comp_instance = Mock()
    mock_comp_instance.label = "test_computer"
    mock_computer.collection.get.return_value = mock_comp_instance

    task = create_mock_shell_task(
        walltime="01:00:00",
        account="test_account",
    )

    metadata = AiidaAdapter.build_metadata(task)

    print("\n=== Metadata ===")
    pprint(metadata)
    print("\n=== Metadata options ===")
    pprint(metadata.options)

    assert metadata.computer_label == "test_computer"
    assert metadata.options.account == "test_account"
    assert metadata.options.max_wallclock_seconds == 3600
    assert "_scheduler-stdout.txt" in metadata.options.additional_retrieve_list
    assert "_scheduler-stderr.txt" in metadata.options.additional_retrieve_list


@patch("aiida.orm.load_code")
@patch("aiida_shell.ShellCode")
def test_create_shell_code_executable_name(mock_shell_code_class, mock_load_code):
    """Test creating code for executable name (no path)."""
    from aiida.common.exceptions import NotExistent

    mock_load_code.side_effect = NotExistent("Code not found")  # Force code creation

    mock_computer = Mock()
    mock_computer.label = "localhost"

    task = create_mock_shell_task(command="bash script.sh")

    mock_code_instance = Mock()
    mock_code_instance.store.return_value = None
    mock_shell_code_class.return_value = mock_code_instance

    code = AiidaAdapter.create_shell_code(task, mock_computer)

    # Should create InstalledCode for "bash"
    assert mock_shell_code_class.called
    call_args = mock_shell_code_class.call_args
    assert call_args[1]["label"] == "bash"
    assert call_args[1]["filepath_executable"] == "bash"
    assert code == mock_code_instance


@patch("aiida.orm.load_code")
@patch("aiida.orm.PortableCode")
def test_create_shell_code_local_script(mock_portable_code_class, mock_load_code, tmp_path):
    """Test creating code for local script file."""
    from aiida.common.exceptions import NotExistent

    mock_load_code.side_effect = NotExistent("Code not found")

    mock_computer = Mock()
    mock_computer.label = "localhost"

    # Create a temporary script file
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\necho hello")

    task = create_mock_shell_task(path=script_file, command="bash test_script.sh")

    mock_code_instance = Mock()
    mock_code_instance.store.return_value = None
    mock_portable_code_class.return_value = mock_code_instance

    code = AiidaAdapter.create_shell_code(task, mock_computer)

    # Should create PortableCode for local script
    assert mock_portable_code_class.called
    call_args = mock_portable_code_class.call_args
    assert "test_script" in call_args[1]["label"]  # Extension removed
    assert call_args[1]["filepath_executable"] == "test_script.sh"
    assert code == mock_code_instance


@pytest.mark.parametrize(
    ("input_label", "expected"),
    [
        # Individual invalid characters
        ("task-name", "task_name"),  # Dash
        ("task name", "task_name"),  # Space
        ("task:name", "task_name"),  # Colon
        ("task.name", "task_name"),  # Dot
        # Multiple invalid characters
        ("task-name.v1:final", "task_name_v1_final"),
        # Valid label unchanged
        ("task_name_123", "task_name_123"),
    ],
)
def test_sanitize_label(input_label, expected):
    """Test label sanitization for various invalid characters."""
    assert AiidaAdapter.sanitize_label(input_label) == expected


@pytest.mark.parametrize(
    ("template", "mapping", "expected"),
    [
        # Standalone placeholders
        (
            "command {input_file} {output_file}",
            {"input_file": "input_123", "output_file": "output_456"},
            ["command", "{input_123}", "{output_456}"],
        ),
        # Embedded placeholders
        (
            "--input=input_file --output=output_file",
            {"input_file": "input_123", "output_file": "output_456"},
            ["--input={input_123}", "--output={output_456}"],
        ),
        # No mapping - keep original
        (
            "command {unknown_placeholder}",
            {},
            ["command", "{unknown_placeholder}"],
        ),
        # Empty template
        ("", {}, []),
        # None template
        (None, {}, []),
    ],
)
def test_substitute_argument_placeholders(template, mapping, expected):
    """Test argument placeholder substitution for various scenarios."""
    result = AiidaAdapter.substitute_argument_placeholders(template, mapping)
    assert result == expected
