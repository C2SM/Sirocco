import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from sirocco.core.graph_items import GeneratedData, Task
from sirocco.core.tasks.shell_task import ShellTask
from sirocco.engines.standalone import StandaloneEngine
from sirocco.engines.standalone.plugin_handlers import PluginHandler, ShellHandler, SiroccoTaskHandler

DATA = GeneratedData(name="some_data", coordinates={})


class DummyHandler(PluginHandler):
    """Not registered: defines no `task_type`"""

    def resolve_output_paths(self) -> None:
        pass

    def prepare_rundir_content(self) -> None:
        pass

    def get_runscript_lines(self) -> list[str]:
        return []


@pytest.fixture
def handler(tmp_path) -> DummyHandler:
    """Handler with a stub engine holding only what handlers share"""
    engine = cast("StandaloneEngine", SimpleNamespace(run_root=tmp_path / "run", data_paths={}))
    return DummyHandler(cast("Task", SimpleNamespace(label="dummy")), engine)


def test_set_path_again_to_same_path(tmp_path, handler):
    handler.set_data_path(DATA, tmp_path / "a")
    handler.set_data_path(DATA, tmp_path / "a")
    assert handler.get_data_path(DATA) == tmp_path / "a"


def test_set_path_conflict(tmp_path, handler):
    handler.set_data_path(DATA, tmp_path / "a")
    with pytest.raises(ValueError, match="already resolved to"):
        handler.set_data_path(DATA, tmp_path / "b")
    assert handler.get_data_path(DATA) == tmp_path / "a"


def test_set_path_relative(handler):
    with pytest.raises(ValueError, match="must be absolute"):
        handler.set_data_path(DATA, Path("relative"))


def test_data_paths_shared_between_handlers(tmp_path, handler):
    other = DummyHandler(cast("Task", SimpleNamespace(label="other")), handler.engine)
    handler.set_data_path(DATA, tmp_path / "a")
    assert other.get_data_path(DATA) == tmp_path / "a"


def test_no_handler():
    task = cast("Task", SimpleNamespace(plugin="unknown"))
    with pytest.raises(NotImplementedError, match="no handler for plugin 'unknown'"):
        PluginHandler.handler_class(task)


def test_duplicate_registration():
    with pytest.raises(ValueError, match="already registered"):

        class OtherShellHandler(ShellHandler):
            task_type = ShellTask


def test_sirocco_task_runs_from_config_dir(tmp_path, test_rootdir):
    config_dir = tmp_path / "config"
    shutil.copytree(test_rootdir / "tests/cases/parameters/config", config_dir)
    engine = StandaloneEngine.from_config_path(config_dir)
    handler = engine.handlers[engine.sirocco_continue_task]
    assert isinstance(handler, SiroccoTaskHandler)
    assert handler.get_rundir() == config_dir
    assert handler.get_runscript_path() == config_dir / ".sirocco_continue.sh"
