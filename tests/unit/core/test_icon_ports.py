"""Unit tests for ICON port handlers."""

from types import SimpleNamespace

import pytest

from sirocco.core import AvailableData, GeneratedData
from sirocco.core.tasks.icon_task.models import ModelType
from sirocco.core.tasks.icon_task.ports import PortHandler


def make_model(tmp_path, data, model_type=ModelType.ATMOSPHERE):
    return SimpleNamespace(
        name="atm",
        task_label="icon",
        model_type=model_type,
        inputs={"latbc": [data]},
        namelist={"limarea_nml": {"latbc_path": "./somewhere"}},
        task_run_dir=tmp_path,
    )


def test_latbc_generated_data(tmp_path):
    latbc_dir = tmp_path / "upstream" / "lbc_files"
    model = make_model(tmp_path, GeneratedData(name="lbc", path=latbc_dir, coordinates={}))
    PortHandler.handle("latbc", model)
    assert model.namelist["limarea_nml"]["latbc_path"] == "./latbc"
    # The link may be dangling as the upstream task has not run yet
    assert (tmp_path / "latbc").readlink() == latbc_dir


def test_latbc_available_data(tmp_path):
    latbc_dir = tmp_path / "lbc_files"
    model = make_model(tmp_path, AvailableData(name="lbc", computer="localhost", path=latbc_dir, coordinates={}))
    PortHandler.handle("latbc", model)
    assert model.namelist["limarea_nml"]["latbc_path"] == str(latbc_dir)
    assert not (tmp_path / "latbc").exists()


def test_latbc_invalid_model_type(tmp_path):
    model = make_model(tmp_path, GeneratedData(name="lbc", path=tmp_path / "lbc", coordinates={}), ModelType.OCEAN)
    with pytest.raises(ValueError, match="not valid for model"):
        PortHandler.handle("latbc", model)
