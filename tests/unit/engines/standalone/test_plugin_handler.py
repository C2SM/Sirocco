import shutil
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from sirocco.core.tasks.shell_task import ShellTask
from sirocco.engines.standalone import StandaloneEngine
from sirocco.engines.standalone.plugin_handlers import PluginHandler, ShellHandler, SiroccoTaskHandler

if TYPE_CHECKING:
    from sirocco.core.graph_items import Task


def test_no_handler():
    task = cast("Task", SimpleNamespace(plugin="unknown"))
    with pytest.raises(NotImplementedError, match="no handler for plugin 'unknown'"):
        PluginHandler.handler_class(task)


def test_duplicate_registration():
    with pytest.raises(ValueError, match="already registered"):

        class OtherShellHandler(PluginHandler[ShellTask]):
            pass


def test_invalid_parametrization():
    with pytest.raises(TypeError, match="must be parametrized with a `Task` subclass"):

        class ForwardRefHandler(PluginHandler["ShellTask"]):
            pass


def test_no_registration_without_task_class():
    registered = dict(PluginHandler.handler_classes)

    class DerivedShellHandler(ShellHandler):
        pass

    class GenericHandler[T: Task](PluginHandler[T]):
        pass

    assert PluginHandler.handler_classes == registered


def test_sirocco_task_runs_from_config_dir(tmp_path, test_rootdir):
    config_dir = tmp_path / "config"
    shutil.copytree(test_rootdir / "tests/cases/parameters/config", config_dir)
    engine = StandaloneEngine.from_config_path(config_dir)
    task_handler = engine.task_handlers[engine.sirocco_continue_task]
    assert isinstance(task_handler, SiroccoTaskHandler)
    assert task_handler.get_rundir() == config_dir
    assert task_handler.get_runscript_path() == config_dir / ".sirocco_continue.sh"
