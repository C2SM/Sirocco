import logging
import textwrap

import pytest

from sirocco.parsing.local_config import LocalConfig

SANTIS_CONFIG = textwrap.dedent("""
    remote_control:
      santis:
        protocol: firecrest
        firecrest_url: https://api.cscs.ch/hpc/firecrest/v2
        token_uri: https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token
        consumer_key: XXX
        consumer_secret: YYY
""")


@pytest.fixture
def local_config_path(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    path = tmp_path / "sirocco" / "config.yaml"
    path.parent.mkdir()
    path.write_text(SANTIS_CONFIG)
    path.chmod(0o600)
    return path


@pytest.mark.usefixtures("local_config_path")
def test_load_default_path(caplog):
    with caplog.at_level(logging.WARNING):
        remote = LocalConfig.from_file().remote("santis")
    assert remote.consumer_key == "XXX"
    assert remote.consumer_secret.get_secret_value() == "YYY"
    assert not caplog.records


def test_warn_permissions(local_config_path, caplog):
    local_config_path.chmod(0o644)
    with caplog.at_level(logging.WARNING):
        LocalConfig.from_file()
    assert "accessible by other users" in caplog.text


@pytest.mark.usefixtures("local_config_path")
def test_unknown_machine():
    with pytest.raises(ValueError, match=r"machine balfrin not found .*\['santis'\]"):
        LocalConfig.from_file().remote("balfrin")
