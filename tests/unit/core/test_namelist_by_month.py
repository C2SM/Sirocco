"""Unit tests for namelist parameters given per calendar month ("by_month")."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing.cycling import DateCyclePoint, OneOffPoint
from sirocco.parsing.yaml_data_models import ConfigNamelistFile

MONTHLY = [float(month) for month in range(1, 13)]
SPECS = {"nwp_tuning_nml": {"allow_overcast": {"by_month": MONTHLY}, "tune_zvz0i": 0.85}}


def cycle_point(start: str, stop: str) -> DateCyclePoint:
    start_date, stop_date = (datetime.fromisoformat(date).replace(tzinfo=UTC) for date in (start, stop))
    return DateCyclePoint(
        start_date=start_date,
        stop_date=stop_date,
        chunk_start_date=start_date,
        chunk_stop_date=stop_date,
        period=stop_date - start_date,
    )


@pytest.mark.parametrize(
    ("start", "stop", "expected"),
    [
        ("2002-01-01", "2002-02-01", 1.0),
        ("2002-05-01", "2002-06-01", 5.0),
        ("2002-12-01", "2003-01-01", 12.0),
        ("2002-02-11", "2002-02-21", 2.0),
    ],
)
def test_resolve_by_month(start, stop, expected):
    resolved = NamelistFile.resolve_by_month(SPECS, cycle_point(start, stop))
    assert resolved == {"nwp_tuning_nml": {"allow_overcast": expected, "tune_zvz0i": 0.85}}


def test_resolve_by_month_chunk_longer_than_month():
    with pytest.raises(ValueError, match="within one calendar month"):
        NamelistFile.resolve_by_month(SPECS, cycle_point("2002-01-01", "2003-01-01"))


def test_resolve_by_month_without_date_cycling():
    with pytest.raises(TypeError, match="requires date cycling"):
        NamelistFile.resolve_by_month(SPECS, OneOffPoint())


def test_resolve_without_by_month_needs_no_date():
    specs = {"nwp_tuning_nml": {"tune_zvz0i": 0.85}}
    assert NamelistFile.resolve_by_month(specs, None) == specs


def test_config_by_month_requires_12_values():
    with pytest.raises(ValidationError, match="requires a list of 12 values"):
        ConfigNamelistFile(path="some.nml", specs={"nwp_tuning_nml": {"allow_overcast": {"by_month": [0.9] * 11}}})
