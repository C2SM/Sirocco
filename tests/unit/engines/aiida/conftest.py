"""Shared fixtures for AiiDA engine tests."""

import uuid

import aiida.orm
import pytest


@pytest.fixture
def stored_shell_code(aiida_localhost):
    """Return a stored shell code for testing.

    Creates an InstalledCode with /bin/bash for testing code-related functionality
    without mocking aiida.orm.load_code.

    Returns:
        Stored InstalledCode instance with unique label
    """
    code = aiida.orm.InstalledCode(
        computer=aiida_localhost,
        filepath_executable="/bin/bash",
        label=f"bash_{uuid.uuid4().hex[:6]}",
        default_calc_job_plugin="core.shell",
    )
    code.store()
    return code


@pytest.fixture
def stored_icon_code(aiida_localhost):
    """Return a stored ICON code for testing.

    Creates an InstalledCode configured for ICON calculations.

    Returns:
        Stored InstalledCode instance with unique label
    """
    code = aiida.orm.InstalledCode(
        computer=aiida_localhost,
        filepath_executable="/path/to/icon",
        label=f"icon_{uuid.uuid4().hex[:6]}",
        default_calc_job_plugin="icon.icon",
        with_mpi=True,
        use_double_quotes=True,
    )
    code.store()
    return code


@pytest.fixture
def stored_remote_data(aiida_localhost, tmp_path):
    """Return stored RemoteData for testing.

    Creates a RemoteData node pointing to a temporary directory.

    Returns:
        Stored RemoteData instance
    """
    remote = aiida.orm.RemoteData(
        computer=aiida_localhost,
        remote_path=str(tmp_path),
    )
    remote.store()
    return remote


@pytest.fixture
def stored_singlefile(tmp_path):
    """Return stored SinglefileData for testing.

    Creates a SinglefileData node with test content.

    Returns:
        Stored SinglefileData instance
    """
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")

    sfd = aiida.orm.SinglefileData(file=test_file)
    sfd.store()
    return sfd


@pytest.fixture
def stored_folder(tmp_path):
    """Return stored FolderData for testing.

    Creates a FolderData node with test directory structure.

    Returns:
        Stored FolderData instance
    """
    # Create test directory structure
    test_dir = tmp_path / "test_folder"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_text("content1")
    (test_dir / "file2.txt").write_text("content2")

    folder = aiida.orm.FolderData()
    folder.base.repository.put_object_from_tree(test_dir)
    folder.store()
    return folder
