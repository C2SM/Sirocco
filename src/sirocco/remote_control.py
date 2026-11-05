"""Remote control of workflows on remote machines, configured by a local user config"""

from __future__ import annotations

import logging
import os
import re
import stat
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Literal, Self

from firecrest import ClientCredentialsAuth
from firecrest.v2 import Firecrest
from firecrest.FirecrestException import UnexpectedStatusException
from pydantic import BaseModel, ConfigDict, SecretStr
from ruamel.yaml import YAML

from sirocco.core import SiroccoContinueTask, Task, Workflow
from sirocco.parsing import ConfigWorkflow

LOGGER = logging.getLogger(__name__)

# NOTE: generated items are not pushed so that the state of a remote workflow cannot be overwritten
GENERATED_CONFIG_DIR_ITEMS = (
    Task.RUN_ROOT,
    ConfigWorkflow._CONFIG_RESOLVED_FILENAME,
    SiroccoContinueTask.STDOUTERR_FILENAME,
    SiroccoContinueTask.SUBMIT_FILENAME,
    SiroccoContinueTask.LOCK_FILE_NAME,
)


def split_config_dir(config_dir: Path | str) -> tuple[str | None, str]:
    """Extract potential pattern "machine:" from config_dir"""

    split_pattern = re.compile(r"((?P<machine>\w+):)?(?P<path>.*)")

    # NOTE: mostly for type checking, should never happen
    msg = "incorrect config directory specification"
    if (m := split_pattern.match(str(config_dir))) is None:
        raise ValueError(msg)
    if m.group("path") is None:
        raise ValueError(msg)

    return m.group("machine"), m.group("path")


def upload_config_to_remote(machine: str, local_config_dir: Path | str, remote_target_path: str) -> None:
    """Upload a local config directory to a remote machine, excluding generated items"""

    client = create_firecrest_client(machine)

    try:
        if not (client.file_type(machine, remote_target_path) == "directory"):
            raise NotADirectoryError(f"target path for upload is not a directory: {remote_target_path}.")
    except UnexpectedStatusException as error:
        raise FileNotFoundError(f"target path for upload not found on remote machine ({machine}): {remote_target_path}.") from error

    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_name = Path(local_config_dir).name + ".tar.gz"
        local_archive_path = Path(tmp_dir, archive_name)

        pack_local_config_dir(local_config_dir, local_archive_path)

        client.upload(machine, local_archive_path, remote_target_path, archive_name)

    remote_archive_path = str(PurePosixPath(remote_target_path, archive_name))

    try:
        client.extract(machine, remote_archive_path, remote_target_path, compression="gzip")
    finally:
        client.rm(machine, remote_archive_path)


def download_config_from_remote(machine: str, remote_config_dir: str, local_target_path: Path | str) -> None:
    """Download a remote config directory (and run state) to the local machine"""

    if not Path(local_target_path).is_dir():
        raise NotADirectoryError(f"target path for download is not a directory: {str(local_target_path)}.")

    client = create_firecrest_client(machine)

    remote_run_dir = str(PurePosixPath(remote_config_dir, Task.RUN_ROOT))
    remote_run_dir_content = client.list_files(machine, remote_run_dir, show_hidden=True)
    remote_run_dir_content_names = [element["name"] for element in remote_run_dir_content]

    # Stop if the lock file exists in the run directory on the remote machine
    if SiroccoContinueTask.LOCK_FILE_NAME in remote_run_dir_content_names:
        raise RuntimeError(f"a process is currently handling the Sirocco workflow, cannot download config directory: {remote_config_dir}. Please retry later.")

    sirocco_config_str = client.view(machine, str(PurePosixPath(remote_config_dir, ConfigWorkflow._CONFIG_FILENAME)))
    sirocco_config = YAML(typ="safe", pure=True).load(sirocco_config_str)

    account = sirocco_config["tasks"]["SIROCCO"].get("account")
    if not account:
        account = sirocco_config["tasks"]["ROOT"].get("account")

    remote_config_dir_content = client.list_files(machine, remote_config_dir, show_hidden=True)

    with tempfile.TemporaryDirectory() as tmp_dir:
        for element in remote_config_dir_content:
            if element["name"] == Task.RUN_ROOT:
                continue

            element_path = str(PurePosixPath(remote_config_dir, element["name"]))

            if element["type"] == "d":
                remote_archive_path = element_path.rstrip("/") + ".tar.gz"
                client.compress(machine, element_path, remote_archive_path, compression="gzip")

                local_archive_path = Path(tmp_dir, PurePosixPath(remote_archive_path).name)
                try:
                    client.download(machine, remote_archive_path, local_archive_path, account)
                finally:
                    client.rm(machine, remote_archive_path)

                unpack_local_config_dir(local_archive_path, local_target_path)
            else:
                local_file = Path(local_target_path, PurePosixPath(element_path).name)
                client.download(machine, element_path, local_file, account)

    remote_state_file = str(PurePosixPath(remote_run_dir, Workflow.STATE_FILE_NAME))
    if Workflow.STATE_FILE_NAME in remote_run_dir_content_names:
        local_run_dir = Path(local_target_path, Task.RUN_ROOT)
        local_run_dir.mkdir(exist_ok=True)

        local_state_file = Path(local_run_dir, Workflow.STATE_FILE_NAME)

        client.download(machine, remote_state_file, local_state_file, account)
    else:
        LOGGER.warning(f"state file not found on remote machine ({machine}): {remote_state_file}.")


def create_firecrest_client(machine: str) -> Firecrest:
    """Create a Firecrest client from the local config element of the given machine"""

    local_config = LocalConfig.from_file()
    machine_config = local_config.remote(machine)

    client_credentials = ClientCredentialsAuth(
        machine_config.consumer_key,
        machine_config.consumer_secret.get_secret_value(),
        machine_config.token_uri
    )

    return Firecrest(firecrest_url=machine_config.firecrest_url, authorization=client_credentials)


def pack_local_config_dir(local_config_dir: Path | str, local_archive_path: Path | str) -> None:
    """Pack a local config directory into a gzip compressed tar archive, excluding generated items"""

    with tarfile.open(local_archive_path, "w:gz") as archive:
        for item in Path(local_config_dir).iterdir():
            if item.name not in GENERATED_CONFIG_DIR_ITEMS:
                archive.add(item, arcname=item.name)


def unpack_local_config_dir(local_archive_path: Path | str, local_config_dir: Path | str) -> None:
    """Unpack a gzip compressed tar archive of a local config directory into local_config_dir"""

    with tarfile.open(local_archive_path, "r:gz") as archive:
        # NOTE: the "tar" filter refuses members outside local_config_dir but keeps symlinks to absolute paths,
        #       which config dirs may contain
        archive.extractall(local_config_dir, filter="tar")


class ConfigFirecrestRemote(BaseModel):
    """Connection information for a remote machine reached through Firecrest

    Examples:

        >>> import textwrap
        >>> from sirocco.parsing.yaml_data_models import validate_yaml_content
        >>> remote = validate_yaml_content(
        ...     ConfigFirecrestRemote,
        ...     textwrap.dedent('''
        ...       protocol: firecrest
        ...       firecrest_url: https://api.cscs.ch/cw/firecrest/v2
        ...       token_uri: https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token
        ...       consumer_key: XXX
        ...       consumer_secret: YYY
        ...     '''),
        ... )
        >>> remote.consumer_secret
        SecretStr('**********')
    """

    model_config = ConfigDict(extra="forbid")

    protocol: Literal["firecrest"]
    firecrest_url: str
    token_uri: str
    consumer_key: str
    consumer_secret: SecretStr


class LocalConfig(BaseModel):
    """Local user configuration, read from `$XDG_CONFIG_HOME/sirocco/config.yaml`

    Examples:

        >>> import textwrap
        >>> from sirocco.parsing.yaml_data_models import validate_yaml_content
        >>> local_config = validate_yaml_content(
        ...     LocalConfig,
        ...     textwrap.dedent('''
        ...     remote_control:
        ...       santis:
        ...         protocol: firecrest
        ...         firecrest_url: https://api.cscs.ch/cw/firecrest/v2
        ...         token_uri: https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token
        ...         consumer_key: XXX
        ...         consumer_secret: YYY
        ...     '''),
        ... )
        >>> local_config.remote("santis").consumer_key
        'XXX'
    """

    model_config = ConfigDict(extra="forbid")

    remote_control: dict[str, ConfigFirecrestRemote] = {}

    @staticmethod
    def default_path() -> Path:
        config_home = os.environ.get("XDG_CONFIG_HOME") or Path(Path.home(), ".config")

        return Path(config_home, "sirocco", "config.yaml")

    @classmethod
    def from_file(cls) -> Self:
        config_file_path = cls.default_path()

        if not config_file_path.is_file():
            raise FileNotFoundError(f"local config file {config_file_path} not found")

        if config_file_path.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            LOGGER.warning(f"local config file {config_file_path} contains secrets but is accessible by other users, consider `chmod 600 {config_file_path}`")

        return cls.model_validate(YAML(typ="safe", pure=True).load(config_file_path) or {})

    def remote(self, machine: str) -> ConfigFirecrestRemote:
        if machine not in self.remote_control:
            raise ValueError(f"machine {machine} not found in local config, available machines: {list(self.remote_control)}")

        return self.remote_control[machine]