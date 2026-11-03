"""Remote control of workflows on remote machines, configured by a local user config"""

from __future__ import annotations

import logging
import os
import re
import stat
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, SecretStr
from ruamel.yaml import YAML

LOGGER = logging.getLogger(__name__)


def split_config_dir(config_dir: Path | str) -> tuple[str | None, str]:
    """Extract potential pattern "machine:" from config_dir"""

    split_pattern = re.compile(r"((?P<machine>\w+):)?(?P<path>.*)")
    # NOTE: mostly for type checking, should never happen
    msg = "incorrect config dir specification"
    if (m := split_pattern.match(str(config_dir))) is None:
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
        ...       firecrest_url: https://api.cscs.ch/hpc/firecrest/v2
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
        ...         firecrest_url: https://api.cscs.ch/hpc/firecrest/v2
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
