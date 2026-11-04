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
from pydantic import BaseModel, ConfigDict, SecretStr
from ruamel.yaml import YAML

from sirocco.core import Scheduler, SiroccoContinueTask, SiroccoTask, Task
from sirocco.parsing import ConfigWorkflow

LOGGER = logging.getLogger(__name__)


def split_config_dir(config_dir: str) -> tuple[str | None, str]:
    """Extract potential pattern "machine:" from config_dir"""

    split_pattern = re.compile(r"((?P<machine>\w+):)?(?P<path>.*)")
    # NOTE: mostly for type checking, should never happen
    msg = "incorrect config dir specification"
    if (m := split_pattern.match(config_dir)) is None:
        raise ValueError(msg)
    if m.group("path") is None:
        raise ValueError(msg)
    return m.group("machine"), m.group("path")


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
        config_home = os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config"
        return Path(config_home) / "sirocco" / "config.yaml"

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


# NOTE: generated items are not pushed so that the state of a remote workflow cannot be overwritten
GENERATED_CONFIG_DIR_ITEMS = (
    Task.RUN_ROOT,
    ConfigWorkflow._CONFIG_RESOLVED_FILENAME,
    SiroccoContinueTask.STDOUTERR_FILENAME,
    SiroccoContinueTask.SUBMIT_FILENAME,
    SiroccoTask.LOCK_FILE_NAME,
)
PUSH_ARCHIVE_NAME = ".sirocco_push.tar.gz"


def firecrest_client(machine: str) -> Firecrest:
    """Create a Firecrest client from the local config entry of the given machine"""
    remote = LocalConfig.from_file().remote(machine)
    auth = ClientCredentialsAuth(remote.consumer_key, remote.consumer_secret.get_secret_value(), remote.token_uri)
    return Firecrest(firecrest_url=remote.firecrest_url, authorization=auth)


def pack_config_dir(config_rootdir: Path, archive_path: Path) -> None:
    """Pack a local config dir into a gzip compressed tar archive, excluding generated items"""
    with tarfile.open(archive_path, "w:gz") as archive:
        for item in config_rootdir.iterdir():
            if item.name not in GENERATED_CONFIG_DIR_ITEMS:
                archive.add(item, arcname=item.name)


def unpack_config_dir(archive_path: Path, config_rootdir: Path) -> None:
    """Unpack a gzip compressed tar archive of a config dir into config_rootdir"""
    with tarfile.open(archive_path, "r:gz") as archive:
        # NOTE: the "tar" filter refuses members outside config_rootdir but keeps symlinks to absolute paths,
        #       which config dirs may contain
        archive.extractall(config_rootdir, filter="tar")


def push_config_dir(config_rootdir: Path, machine: str, remote_config_rootdir: str) -> None:
    """Push a local config dir to a remote machine as a single archive, excluding generated items"""
    if not PurePosixPath(remote_config_rootdir).is_absolute():
        msg = f"remote config dir {remote_config_rootdir} must be an absolute path"
        raise ValueError(msg)
    client = firecrest_client(machine)
    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_path = Path(tmp_dir) / PUSH_ARCHIVE_NAME
        pack_config_dir(config_rootdir, archive_path)
        client.mkdir(machine, remote_config_rootdir, create_parents=True)
        client.upload(machine, archive_path, directory=remote_config_rootdir, filename=PUSH_ARCHIVE_NAME)
    remote_archive_path = str(PurePosixPath(remote_config_rootdir) / PUSH_ARCHIVE_NAME)
    client.extract(machine, remote_archive_path, remote_config_rootdir, compression="gzip")
    client.rm(machine, remote_archive_path)


def submit_sirocco_task(sirocco_task: SiroccoTask, scheduler: Scheduler, machine: str, remote_config_rootdir: Path):
    client = firecrest_client(machine)
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_runscript_path = Path(tmp_dir) / sirocco_task.SUBMIT_FILENAME
        scheduler.generate_run_script(task=sirocco_task, stdouterr_mode="append", script_path=local_runscript_path)
        client.submit(machine, working_directory=str(remote_config_rootdir), script_local_path=str(local_runscript_path))
