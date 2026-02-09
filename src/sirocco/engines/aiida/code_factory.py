"""Factory for creating AiiDA Code objects.

This module handles the creation and loading of AiiDA Code objects for different
task types, managing the complexities of portable vs. installed codes and local
vs. remote file resolution.
"""

# TODO: Strip `uenv` from the command, as well

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

import aiida.orm
from aiida.common.exceptions import NotExistent

from sirocco.engines.aiida.utils import split_cmd_arg

if TYPE_CHECKING:
    from sirocco import core


class CodeFactory:
    """Factory for creating and loading AiiDA Code objects.

    Handles the logic for determining whether to create PortableCode or
    InstalledCode based on file location and existence, and manages code
    labeling and caching.
    """

    @staticmethod
    def remove_script_extension(filename: str) -> str:
        """Remove script extensions from filenames for common scripting languages for code labels.

        Args:
            filename: The filename to process

        Returns:
            Filename with script extension removed if applicable

        Examples:
            >>> CodeFactory.remove_script_extension("script.sh")
            'script'
            >>> CodeFactory.remove_script_extension("script.py")
            'script'
            >>> CodeFactory.remove_script_extension("myapp.exe")
            'myapp.exe'
        """
        # Extensions for interpreted/scripting languages
        script_extensions = {".sh", ".bash", ".py", ".pl", ".rb", ".R", ".lua", ".tcl", ".js", ".php"}

        path = Path(filename)
        if path.suffix in script_extensions:
            return path.stem
        return filename

    @staticmethod
    def create_shell_code(task: core.ShellTask, computer: aiida.orm.Computer) -> aiida.orm.Code:
        """Create or load an AiiDA Code for a shell task.

        Determines whether to create PortableCode or InstalledCode based on where the
        executable/script actually exists:
        - If file exists locally (absolute or relative path) -> PortableCode (upload)
        - If file exists remotely (absolute path only) -> InstalledCode (reference)
        - If just executable name (no path separators) -> InstalledCode (assume in PATH)

        Args:
            task: The ShellTask to create code for
            computer: The AiiDA computer

        Returns:
            The AiiDA Code object

        Raises:
            FileNotFoundError: If the file doesn't exist locally or remotely
            RuntimeError: If no default AiiDA user is available
        """

        # Determine the executable path to use
        if task.path is not None:
            executable_path = str(task.path)
        else:
            executable_path, _ = split_cmd_arg(task.command)

        path_obj = Path(executable_path)

        # Check if this is a path (contains separators) or just an executable name
        is_path = "/" in executable_path or executable_path.startswith("./")

        if not is_path:
            # Just an executable name (e.g., "python", "bash") -> InstalledCode
            return CodeFactory._create_executable_code(executable_path, computer)

        # It's a path - check if it exists locally
        if not path_obj.is_absolute():
            path_obj = path_obj.resolve()

        exists_locally = path_obj.exists() and path_obj.is_file()

        if exists_locally:
            # File exists locally -> PortableCode
            return CodeFactory._create_portable_code(path_obj, computer)

        # File doesn't exist locally - check remotely
        if not Path(executable_path).is_absolute():
            msg = (
                f"File not found locally at {path_obj}, and relative paths are not "
                f"supported for remote files. Use an absolute path for remote files."
            )
            raise FileNotFoundError(msg)

        # Check remote file existence and create InstalledCode
        return CodeFactory._create_remote_installed_code(executable_path, computer)

    @staticmethod
    def _create_executable_code(executable_name: str, computer: aiida.orm.Computer) -> aiida.orm.Code:
        """Create or load an InstalledCode for a simple executable name.

        Args:
            executable_name: Name of the executable (e.g., "python", "bash")
            computer: The AiiDA computer

        Returns:
            InstalledCode (ShellCode) for the executable
        """
        from aiida_shell import ShellCode

        code_label = executable_name

        try:
            return aiida.orm.load_code(f"{code_label}@{computer.label}")
        except NotExistent:
            code = ShellCode(
                label=code_label,
                computer=computer,
                filepath_executable=executable_name,
                default_calc_job_plugin="core.shell",
                use_double_quotes=True,
            )
            _ = code.store()
            return code

    @staticmethod
    def _create_portable_code(local_path: Path, computer: aiida.orm.Computer) -> aiida.orm.Code:
        """Create or load a PortableCode for a local file.

        Args:
            local_path: Absolute path to the local file
            computer: The AiiDA computer

        Returns:
            PortableCode for the script
        """
        script_name = local_path.name
        script_dir = local_path.parent

        base_label = CodeFactory.remove_script_extension(script_name)

        # Generate unique label based on path and content hashes
        path_hash = hashlib.sha256(str(local_path).encode()).hexdigest()[:8]

        with open(local_path, "rb") as f:
            content_hash = hashlib.sha256(f.read()).hexdigest()[:8]

        code_label = f"{base_label}-{path_hash}-{content_hash}"

        try:
            return aiida.orm.load_code(f"{code_label}@{computer.label}")
        except NotExistent:
            code = aiida.orm.PortableCode(
                label=code_label,
                description=f"Shell script: {local_path}",
                computer=computer,
                filepath_executable=script_name,
                filepath_files=str(script_dir),
                default_calc_job_plugin="core.shell",
            )
            _ = code.store()
            return code

    @staticmethod
    def _create_remote_installed_code(remote_path: str, computer: aiida.orm.Computer) -> aiida.orm.Code:
        """Create or load an InstalledCode for a remote file.

        Args:
            remote_path: Absolute path to the remote file
            computer: The AiiDA computer

        Returns:
            InstalledCode (ShellCode) for the remote script

        Raises:
            FileNotFoundError: If the file doesn't exist remotely
            RuntimeError: If no default AiiDA user is available
        """
        from aiida_shell import ShellCode

        # Check remote file existence
        user = aiida.orm.User.collection.get_default()
        if user is None:
            msg = "No default AiiDA user available."
            raise RuntimeError(msg)

        authinfo = computer.get_authinfo(user)
        with authinfo.get_transport() as transport:
            if not transport.isfile(remote_path):
                msg = (
                    f"File not found remotely: {remote_path} on {computer.label}\n"
                    f"Please verify the file exists and the path is correct."
                )
                raise FileNotFoundError(msg)

        # File exists remotely -> InstalledCode
        script_name = Path(remote_path).name
        base_label = CodeFactory.remove_script_extension(script_name)

        path_hash = hashlib.sha256(remote_path.encode()).hexdigest()[:8]
        code_label = f"{base_label}-{path_hash}"

        try:
            return aiida.orm.load_code(f"{code_label}@{computer.label}")
        except NotExistent:
            code = ShellCode(
                label=code_label,
                description=f"Shell script: {remote_path}",
                computer=computer,
                filepath_executable=remote_path,
                default_calc_job_plugin="core.shell",
                use_double_quotes=True,
            )
            _ = code.store()
            return code
