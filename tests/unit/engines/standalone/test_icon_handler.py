from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from sirocco.core.tasks.icon_task import ports as core_icon_ports
from sirocco.engines.standalone.plugin_handlers.icon_handler import (
    IconPortHandler,
    RestartInPortHandler,
    special_port_handler,
)

if TYPE_CHECKING:
    from sirocco.core.tasks.icon_task.models import IconModel


def test_unsupported_restart_write_mode():
    """The error reports the restart mode found in the namelist"""
    namelist = {"io_nml": {"restart_write_mode": "dedicated procs multifile"}}
    model = cast("IconModel", SimpleNamespace(namelist=namelist))
    handler = RestartInPortHandler(core_icon_ports.restart_in_port)
    with pytest.raises(ValueError, match="got 'dedicated procs multifile'"):
        handler.get_restart_data(model, [])


def test_register_regular_port():
    with pytest.raises(ValueError, match="is not special"):

        @special_port_handler(core_icon_ports.dynamics_grid_file_port)
        class RegularPortHandler(IconPortHandler):
            pass


def test_register_special_port_twice():
    with pytest.raises(ValueError, match="already implemented"):

        @special_port_handler(core_icon_ports.restart_in_port)
        class OtherRestartInPortHandler(IconPortHandler):
            pass
