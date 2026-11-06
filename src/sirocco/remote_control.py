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
from collections.abc import Collection

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
    # TODO: apply templating.
    sirocco_config_path = Path(local_config_dir, ConfigWorkflow._CONFIG_FILENAME)
    sirocco_config = YAML(typ="safe", pure=True).load(sirocco_config_path)

    account = _get_project_account(sirocco_config, str(sirocco_config_path))

    upload_dir_to_remote(machine, local_config_dir, remote_target_path, exclude=GENERATED_CONFIG_DIR_ITEMS, account=account)


def upload_dir_to_remote( 
    machine: str,
    local_dir: Path | str,
    remote_target_path: str,
    exclude: Collection[str] = (),
    account: str | None = None,
    client: Firecrest | None = None
) -> None:
    """Upload a local directory to a remote machine"""

    if client is None:
        client = create_firecrest_client(machine)

    try:
        if not (client.file_type(machine, remote_target_path) == "directory"):
            msg = f"target path for upload is not a directory: {remote_target_path}."
            raise NotADirectoryError(msg)
    except UnexpectedStatusException as error:
        msg = f"target path for upload not found on remote machine ({machine}): {remote_target_path}."
        raise FileNotFoundError(msg) from error

    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_name = Path(local_dir).name + ".tar.gz"
        local_archive_path = Path(tmp_dir, archive_name)

        pack_local_dir(local_dir, local_archive_path, exclude=exclude)

        client.upload(machine, local_archive_path, remote_target_path, archive_name, account=account)

        remote_archive_path = str(PurePosixPath(remote_target_path, archive_name))

        try:
            client.extract(machine, remote_archive_path, remote_target_path, compression="gzip", account=account)
        finally:
            client.rm(machine, remote_archive_path)


def pack_local_dir(local_dir: Path | str, local_archive_path: Path | str, exclude: Collection[str] = ()) -> None:
    """Pack a local config directory into a gzip compressed tar archive"""

    with tarfile.open(local_archive_path, "w:gz") as archive:
        for item in Path(local_dir).iterdir():
            if item.name not in exclude:
                archive.add(item, arcname=str(PurePosixPath(Path(local_dir).name, item.name)))


def _get_project_account(sirocco_config: dict, sirocco_config_location: str) -> str:
    """Get the project account from tasks.SIROCCO, falling back to tasks.ROOT"""

    account = sirocco_config["tasks"]["SIROCCO"].get("account")
    if account is None:
        msg = f"project account is not set in {sirocco_config_location} (neither in tasks.SIROCCO nor tasks.ROOT)."
        if (root_config := sirocco_config["tasks"].get("ROOT")) is None:
            raise ValueError(msg)
        if (account := root_config.get("account")) is None:
            raise ValueError(msg)

    return account


def download_config_from_remote(machine: str, remote_config_dir: str, local_target_path: Path | str) -> None:
    """Download a remote config directory (and run state) to the local machine"""

    client = create_firecrest_client(machine)

    remote_run_dir = str(PurePosixPath(remote_config_dir, Task.RUN_ROOT))
    remote_run_dir_content = client.list_files(machine, remote_run_dir, show_hidden=True)
    remote_run_dir_content_names = [element["name"] for element in remote_run_dir_content]

    # Stop if the lock file exists in the run directory on the remote machine
    if SiroccoContinueTask.LOCK_FILE_NAME in remote_run_dir_content_names:
        raise RuntimeError(f"a process is currently handling the Sirocco workflow, cannot download config directory: {remote_config_dir}. Please retry later.")

    sirocco_config_path = str(PurePosixPath(remote_config_dir, ConfigWorkflow._CONFIG_FILENAME))
    sirocco_config_str = client.view(machine, sirocco_config_path)
    sirocco_config = YAML(typ="safe", pure=True).load(sirocco_config_str)

    account = _get_project_account(sirocco_config, f"{machine}:{sirocco_config_path}")

    download_dir_from_remote(machine, remote_config_dir, local_target_path, exclude=(Task.RUN_ROOT,), account=account, client=client)

    remote_state_file = str(PurePosixPath(remote_run_dir, Workflow.STATE_FILE_NAME))
    if Workflow.STATE_FILE_NAME in remote_run_dir_content_names:
        local_run_dir = Path(local_target_path, PurePosixPath(remote_config_dir).name, Task.RUN_ROOT)
        local_run_dir.mkdir(exist_ok=True)

        local_state_file = Path(local_run_dir, Workflow.STATE_FILE_NAME)

        client.download(machine, remote_state_file, local_state_file, account=account)
    else:
        msg = f"state file not found on remote machine ({machine}): {remote_state_file}."
        LOGGER.warning(msg)


def download_dir_from_remote(
    machine: str,
    remote_dir: str,
    local_target_path: Path | str,
    exclude: Collection[str] = (),
    account: str | None = None,
    client: Firecrest | None = None
) -> None:
    """Download a remote directory to the local machine"""

    if client is None:
        client = create_firecrest_client(machine)

    if not Path(local_target_path).is_dir():
        msg = f"target path for download is not a directory: {str(local_target_path)}."
        raise NotADirectoryError(msg)

    with tempfile.TemporaryDirectory() as tmp_dir:
        if len(exclude) == 0:
            _download_full_dir_from_remote(machine, remote_dir, tmp_dir, local_target_path, client, account=account)
        else:
            local_dir = Path(local_target_path, PurePosixPath(remote_dir).name)
            local_dir.mkdir(exist_ok=True)

            remote_dir_content = client.list_files(machine, remote_dir, show_hidden=True)

            for element in remote_dir_content:
                if element["name"] not in exclude:
                    element_path = str(PurePosixPath(remote_dir, element["name"]))

                    if element["type"] == "d":
                        _download_full_dir_from_remote(machine, element_path, tmp_dir, local_dir, client, account=account)
                    else:
                        local_file = Path(local_dir, element["name"])
                        client.download(machine, element_path, local_file, account=account)


def _download_full_dir_from_remote(
    machine: str,
    remote_dir: str,
    tmp_dir: Path | str, 
    local_target_path: Path | str,
    client: Firecrest,
    account: str | None = None,
) -> None:
    """Utility to download a full (no exclusion of content) remote directory to the local machine"""

    remote_archive_path = remote_dir.rstrip("/") + ".tar.gz"
    local_archive_path = Path(tmp_dir, PurePosixPath(remote_archive_path).name)

    client.compress(machine, remote_dir, remote_archive_path, compression="gzip", account=account)

    try:
        client.download(machine, remote_archive_path, local_archive_path, account=account)
    finally:
        client.rm(machine, remote_archive_path)

    unpack_local_archive(local_archive_path, local_target_path)


def unpack_local_archive(local_archive_path: Path | str, local_target_path: Path | str) -> None:
    """Unpack a gzip compressed tar archive of a local config directory into local_target_path"""

    with tarfile.open(local_archive_path, "r:gz") as archive:
        # NOTE: the "tar" filter refuses members outside local_target_path but keeps symlinks to absolute paths,
        #       which config dirs may contain
        archive.extractall(local_target_path, filter="tar") # TODO: check that this actually unpacks in a new local_target_path/config_dir (it depends how Firecrest compress works)


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
            msg = f"local config file {config_file_path} not found"
            raise FileNotFoundError(msg)

        if config_file_path.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            msg = f"local config file {config_file_path} contains secrets but is accessible by other users, consider `chmod 600 {config_file_path}`"
            LOGGER.warning(msg)

        return cls.model_validate(YAML(typ="safe", pure=True).load(config_file_path) or {})

    def remote(self, machine: str) -> ConfigFirecrestRemote:
        if machine not in self.remote_control:
            msg = f"machine {machine} not found in local config, available machines: {list(self.remote_control)}"
            raise ValueError(msg)

        return self.remote_control[machine]