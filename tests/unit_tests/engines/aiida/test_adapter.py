"""Unit tests for sirocco.engines.aiida.adapter module."""

from datetime import UTC
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from tests.utils import (
    create_authinfo,
    create_available_data,
    create_mock_icon_task,
    create_mock_shell_task,
    create_transport,
)


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
        (
            "test_task",
            {"member": 0, "date": "2026-01-01"},
            ["test_task", "member_0"],
            [],
        ),
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


def test_build_metadata(aiida_localhost):
    """Test building metadata for a task."""
    task = create_mock_shell_task(
        computer=aiida_localhost.label,
        walltime="01:00:00",
        account="test_account",
    )

    metadata = AiidaAdapter.build_metadata(task)

    print("\n=== Metadata ===")
    pprint(metadata)
    print("\n=== Metadata options ===")
    pprint(metadata.options)

    assert metadata.computer_label == aiida_localhost.label
    assert metadata.options.account == "test_account"
    assert metadata.options.max_wallclock_seconds == 3600
    assert "_scheduler-stdout.txt" in metadata.options.additional_retrieve_list
    assert "_scheduler-stderr.txt" in metadata.options.additional_retrieve_list


# Patch: Force NotExistent exception to test code creation path when code doesn't exist
@patch("aiida.orm.load_code")
# Patch: Mock ShellCode class to prevent actual database storage during test
@patch("aiida_shell.ShellCode")
def test_create_shell_code_executable_name(mock_shell_code_class, mock_load_code, aiida_localhost):
    """Test creating code for executable name (no path)."""
    from aiida.common.exceptions import NotExistent

    mock_load_code.side_effect = NotExistent("Code not found")  # Force code creation

    task = create_mock_shell_task(command="bash script.sh")

    mock_code_instance = Mock()
    mock_code_instance.store.return_value = None
    mock_shell_code_class.return_value = mock_code_instance

    code = AiidaAdapter.create_shell_code(task, aiida_localhost)

    # Should create InstalledCode for "bash"
    assert mock_shell_code_class.called
    call_args = mock_shell_code_class.call_args
    assert call_args[1]["label"] == "bash"
    assert call_args[1]["filepath_executable"] == "bash"
    assert code == mock_code_instance


# Patch: Force NotExistent exception to test code creation path when code doesn't exist
@patch("aiida.orm.load_code")
# Patch: Mock PortableCode class to prevent actual database storage during test
@patch("aiida.orm.PortableCode")
def test_create_shell_code_local_script(mock_portable_code_class, mock_load_code, tmp_path, aiida_localhost):
    """Test creating code for local script file."""
    from aiida.common.exceptions import NotExistent

    mock_load_code.side_effect = NotExistent("Code not found")

    # Create a temporary script file
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\necho hello")

    task = create_mock_shell_task(path=script_file, command="bash test_script.sh")

    mock_code_instance = Mock()
    mock_code_instance.store.return_value = None
    mock_portable_code_class.return_value = mock_code_instance

    code = AiidaAdapter.create_shell_code(task, aiida_localhost)

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


def test_create_input_data_node_remote_data_for_icon(tmp_path, aiida_localhost):
    """Test creating RemoteData node when used_by_icon=True."""
    import aiida.orm

    test_file = tmp_path / "data.txt"
    test_file.write_text("test")

    core_data = create_available_data("test_data", aiida_localhost.label, test_file)

    result = AiidaAdapter.create_input_data_node(core_data, used_by_icon=True)

    # Verify it created a real RemoteData node with correct attributes
    assert isinstance(result, aiida.orm.RemoteData)
    assert result.get_remote_path() == str(test_file)
    assert result.computer.pk == aiida_localhost.pk


def test_create_input_data_node_local_file(tmp_path, aiida_localhost):
    """Test creating SinglefileData for local file."""
    import aiida.orm

    test_file = tmp_path / "input.txt"
    test_file.write_text("test content")

    core_data = create_available_data("input_file", aiida_localhost.label, test_file)

    result = AiidaAdapter.create_input_data_node(core_data, used_by_icon=False)

    # Verify it created a real SinglefileData node with correct content
    assert isinstance(result, aiida.orm.SinglefileData)
    assert result.get_content() == "test content"


def test_create_input_data_node_local_folder(tmp_path, aiida_localhost):
    """Test creating FolderData for local directory."""
    import aiida.orm

    test_dir = tmp_path / "input_folder"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_text("content1")

    core_data = create_available_data("input_folder", aiida_localhost.label, test_dir)

    result = AiidaAdapter.create_input_data_node(core_data, used_by_icon=False)

    # Verify it created a real FolderData node with correct content
    assert isinstance(result, aiida.orm.FolderData)
    # Check that the file exists in the folder node
    assert "file1.txt" in result.list_object_names()


# Patch: Mock RemoteData to prevent actual database storage during test
@patch("sirocco.engines.aiida.adapter.aiida.orm.RemoteData")
def test_create_input_data_node_remote_transport(mock_remote_data, tmp_path, aiida_localhost):
    """Test creating RemoteData for remote computer."""
    from pathlib import Path

    remote_path = Path(tmp_path).absolute() / "remote_data.nc"
    remote_path.write_text("netcdf data")

    core_data = create_available_data("remote_file", aiida_localhost.label, remote_path)

    mock_node = Mock()
    mock_remote_data.return_value = mock_node

    # Make the LocalTransport check fail (so it's treated as remote)
    # Patch: Mock LocalTransport to control computer type detection (local vs remote)
    with (
        patch("sirocco.engines.aiida.adapter.LocalTransport"),
        patch.object(aiida_localhost, "get_transport_class", return_value=Mock()),
    ):
        result = AiidaAdapter.create_input_data_node(core_data, used_by_icon=False)

        assert mock_remote_data.called
        assert mock_remote_data.call_args[1]["remote_path"] == str(remote_path)
        assert mock_remote_data.call_args[1]["computer"] == aiida_localhost
        assert result == mock_node


# Patch: Force NotExistent exception to test error handling when computer is not found
@patch("sirocco.engines.aiida.adapter.aiida.orm.load_computer")
def test_create_input_data_node_computer_not_found(mock_load_computer):
    """Test error when computer is not found."""
    from aiida.common.exceptions import NotExistent

    mock_load_computer.side_effect = NotExistent("Computer not found")
    core_data = create_available_data("test_data", "nonexistent_computer", "/some/path")

    with pytest.raises(ValueError, match="Could not find computer 'nonexistent_computer'"):
        AiidaAdapter.create_input_data_node(core_data)


def test_create_input_data_node_path_not_exists(aiida_localhost):
    """Test error when path does not exist on computer."""
    core_data = create_available_data(
        "missing_data",
        aiida_localhost.label,
        "/this/path/definitely/does/not/exist.txt",
    )

    with pytest.raises(FileNotFoundError, match="Could not find available data"):
        AiidaAdapter.create_input_data_node(core_data)


def test_create_shell_code_remote_file_exists(aiida_localhost):
    """Test creating InstalledCode for remote file that exists."""
    import uuid

    import aiida.orm

    unique_name = f"script_{uuid.uuid4().hex[:8]}.sh"
    remote_path = f"/remote/path/{unique_name}"
    task = create_mock_shell_task(command=remote_path)

    mock_transport = create_transport(path_exists=True, isfile=True)
    mock_authinfo = create_authinfo(mock_transport)

    with patch.object(aiida_localhost, "get_authinfo", return_value=mock_authinfo):
        code = AiidaAdapter.create_shell_code(task, aiida_localhost)

        assert isinstance(code, aiida.orm.Code)
        assert unique_name.replace(".sh", "") in code.label or unique_name in code.label
        assert code.computer.pk == aiida_localhost.pk


# Patch: Force NotExistent exception to trigger remote file validation path
@patch("sirocco.engines.aiida.adapter.aiida.orm.load_code")
def test_create_shell_code_remote_file_not_found(mock_load_code, aiida_localhost):
    """Test error when remote file doesn't exist."""
    from aiida.common.exceptions import NotExistent

    mock_load_code.side_effect = NotExistent("Code not found")
    task = create_mock_shell_task(command="/remote/path/missing.sh")

    mock_transport = create_transport(path_exists=False, isfile=False)
    mock_authinfo = create_authinfo(mock_transport)

    with (
        patch.object(aiida_localhost, "get_authinfo", return_value=mock_authinfo),
        pytest.raises(FileNotFoundError, match="File not found locally or remotely"),
    ):
        AiidaAdapter.create_shell_code(task, aiida_localhost)


# Patch: Mock User class to control default user availability for code creation
@patch("sirocco.engines.aiida.adapter.aiida.orm.User")
# Patch: Force NotExistent exception to trigger code creation path
@patch("sirocco.engines.aiida.adapter.aiida.orm.load_code")
def test_create_shell_code_no_default_user(mock_load_code, mock_user_class, aiida_localhost):
    """Test error when no default AiiDA user is available."""
    from aiida.common.exceptions import NotExistent

    mock_load_code.side_effect = NotExistent("Code not found")
    mock_user_class.collection.get_default.return_value = None

    task = create_mock_shell_task(command="/remote/path/script.sh")

    with pytest.raises(RuntimeError, match="No default AiiDA user available"):
        AiidaAdapter.create_shell_code(task, aiida_localhost)


# Patch: Force NotExistent exception to test relative path validation logic
@patch("sirocco.engines.aiida.adapter.aiida.orm.load_code")
def test_create_shell_code_relative_path_rejected(mock_load_code, aiida_localhost):
    """Test that relative paths are rejected for non-local files."""
    from aiida.common.exceptions import NotExistent

    mock_load_code.side_effect = NotExistent("Code not found")

    # Use a relative path that doesn't exist locally
    task = create_mock_shell_task(command="./nonexistent_script.sh")

    with pytest.raises(FileNotFoundError, match="relative paths are not supported for remote files"):
        AiidaAdapter.create_shell_code(task, aiida_localhost)


# Patch: Mock load_code to return existing code and verify code reuse path
@patch("sirocco.engines.aiida.adapter.aiida.orm.load_code")
def test_create_shell_code_loads_existing_code(mock_load_code, aiida_localhost):
    """Test that existing code is loaded instead of creating new one."""
    mock_existing_code = Mock()
    mock_load_code.return_value = mock_existing_code

    task = create_mock_shell_task(command="bash")

    code = AiidaAdapter.create_shell_code(task, aiida_localhost)

    # Should load existing code without creating new one
    assert code == mock_existing_code
    assert mock_load_code.called


def test_create_shell_code_resolves_relative_path(tmp_path, aiida_localhost):
    """Test that relative paths are resolved to absolute paths."""
    import uuid

    import aiida.orm

    # Create a script with unique name
    unique_name = f"test_script_{uuid.uuid4().hex[:8]}.sh"
    script_file = tmp_path / unique_name
    script_file.write_text("#!/bin/bash\necho hello")

    # Change to tmp directory
    import os

    original_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)

        # Use relative path
        task = create_mock_shell_task(path=Path(f"./{unique_name}"), command=f"bash {unique_name}")

        code = AiidaAdapter.create_shell_code(task, aiida_localhost)

        # Should create a code (PortableCode or loaded from cache)
        assert isinstance(code, aiida.orm.Code)
        assert unique_name.replace(".sh", "") in code.label or unique_name in code.label
        # For PortableCode, filepath_files should be absolute
        if isinstance(code, aiida.orm.PortableCode):
            assert Path(code.filepath_files).is_absolute()
    finally:
        os.chdir(original_cwd)


# Patch: Force NotExistent exception to test error handling when computer lookup fails
@patch("aiida.orm.Computer")
def test_build_metadata_computer_not_found(mock_computer_class):
    """Test error when computer is not found in database."""
    from aiida.common.exceptions import NotExistent

    mock_computer_class.collection.get.side_effect = NotExistent("Computer not found")

    task = create_mock_shell_task(computer="nonexistent")

    with pytest.raises(ValueError, match="Could not find computer 'nonexistent'"):
        AiidaAdapter.build_metadata(task)


def create_date_cycle_point():
    """Helper to create DateCyclePoint mock."""
    from datetime import datetime

    from sirocco.parsing.cycling import DateCyclePoint

    # Create a proper DateCyclePoint instance
    # DateCyclePoint typically needs initialization but we can mock its attributes
    cycle_point = Mock(spec=DateCyclePoint)
    cycle_point.__class__ = DateCyclePoint
    cycle_point.chunk_start_date = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    cycle_point.chunk_stop_date = datetime(2026, 1, 2, 0, 0, 0, tzinfo=UTC)
    return cycle_point


@pytest.mark.parametrize(
    ("existing_prepend", "expected_in_result"),
    [
        pytest.param(
            None,
            [
                "export SIROCCO_START_DATE",
                "export SIROCCO_STOP_DATE",
                "2026-01-01T00:00:00",
                "2026-01-02T00:00:00",
            ],
            id="no_existing_prepend",
        ),
        pytest.param(
            "module load python\n",
            [
                "module load python",
                "export SIROCCO_START_DATE",
                "export SIROCCO_STOP_DATE",
            ],
            id="with_existing_prepend",
        ),
        pytest.param(
            None,
            ["T", "+00:00"],  # ISO format markers
            id="date_format",
        ),
    ],
)
def test_add_sirocco_time_prepend_text(existing_prepend, expected_in_result, aiida_localhost):
    """Test adding Sirocco time environment exports with DateCyclePoint.

    Tests three scenarios:
    1. No existing prepend_text - should add exports
    2. Existing prepend_text - should concatenate
    3. Date formatting - should use ISO format with timezone
    """
    task = create_mock_shell_task(computer=aiida_localhost.label, walltime="01:00:00")
    task.cycle_point = create_date_cycle_point()

    if existing_prepend:
        # Test concatenation by calling internal method directly
        from sirocco.engines.aiida.types import AiidaMetadataOptions

        base_options = AiidaMetadataOptions(
            prepend_text=existing_prepend,
            additional_retrieve_list=["_scheduler-stdout.txt", "_scheduler-stderr.txt"],
        )
        result_options = AiidaAdapter._add_sirocco_time_prepend_text(base_options, task)
        result_text = result_options.prepend_text
    else:
        # Test via build_metadata (normal path)
        metadata = AiidaAdapter.build_metadata(task)
        result_text = metadata.options.prepend_text

    # Verify result
    assert result_text is not None, "prepend_text should not be None with DateCyclePoint"
    for expected in expected_in_result:
        assert expected in result_text, f"Expected '{expected}' in prepend_text"


def test_build_scheduler_options_with_date_cycle_point(aiida_localhost):
    """Test that metadata is built correctly with DateCyclePoint."""
    task = create_mock_icon_task(
        computer=aiida_localhost.label,
        walltime="02:00:00",
        account="test_account",
        nodes=4,
        ntasks_per_node=24,
    )
    task.cycle_point = create_date_cycle_point()

    metadata = AiidaAdapter.build_metadata(task)

    # Verify all components are present
    assert metadata.computer_label == aiida_localhost.label
    assert metadata.options.account == "test_account"
    assert metadata.options.max_wallclock_seconds == 7200
    assert metadata.options.resources.num_machines == 4
    assert metadata.options.resources.num_mpiprocs_per_machine == 24
    assert metadata.options.prepend_text is not None, "prepend_text should not be None with DateCyclePoint"
    assert "SIROCCO_START_DATE" in metadata.options.prepend_text


@pytest.mark.parametrize(
    ("input_cmd", "expected_contains", "expected_not_contains", "expected_count"),
    [
        # Single placeholder
        (
            "srun -n {MPI_TOTAL_PROCS} ./executable",
            ["{tot_num_mpiprocs}"],
            ["{MPI_TOTAL_PROCS}"],
            None,
        ),
        # Multiple identical placeholders
        (
            "mpirun -np {MPI_TOTAL_PROCS} --bind-to core:{MPI_TOTAL_PROCS}",
            [],
            ["{MPI_TOTAL_PROCS}"],
            2,  # Should contain 2 occurrences of {tot_num_mpiprocs}
        ),
        # No placeholders
        ("srun -n 24 ./executable", [], [], None),
    ],
)
def test_parse_mpi_cmd(input_cmd, expected_contains, expected_not_contains, expected_count):
    """Test parsing MPI commands with various placeholder scenarios."""
    result = AiidaAdapter.parse_mpi_cmd(input_cmd)

    for expected in expected_contains:
        assert expected in result

    for not_expected in expected_not_contains:
        assert not_expected not in result

    if expected_count is not None:
        assert result.count("{tot_num_mpiprocs}") == expected_count
    elif not expected_contains and not expected_not_contains:
        # No placeholders case - should remain unchanged
        assert result == input_cmd


@pytest.mark.parametrize(
    "use_custom_script",
    [True, False],
    ids=["custom_script", "default_script"],
)
def test_get_wrapper_script_data(use_custom_script, tmp_path):
    """Test getting wrapper script with custom or default script."""
    task = create_mock_icon_task()

    if use_custom_script:
        # Test with custom script
        custom_script = tmp_path / "custom_wrapper.sh"
        custom_script.write_text("#!/bin/bash\necho custom")
        task.wrapper_script = custom_script

        # Patch: Mock SinglefileData to prevent actual database storage during test
        with patch("sirocco.engines.aiida.adapter.aiida.orm.SinglefileData") as mock_singlefile:
            mock_node = Mock()
            mock_singlefile.return_value = mock_node

            result = AiidaAdapter.get_wrapper_script_data(task)

            # Should create SinglefileData with custom script
            assert mock_singlefile.called
            call_args = mock_singlefile.call_args
            assert str(custom_script) in str(call_args)
            assert result == mock_node
    else:
        # Test with default script
        task.wrapper_script = None

        # Patch: Mock get_default_wrapper_script to test default wrapper script path
        with patch("sirocco.engines.aiida.adapter.AiidaAdapter.get_default_wrapper_script") as mock_get_default:
            mock_default_node = Mock()
            mock_get_default.return_value = mock_default_node

            result = AiidaAdapter.get_wrapper_script_data(task)

            # Should use default wrapper script
            assert mock_get_default.called
            assert result == mock_default_node
