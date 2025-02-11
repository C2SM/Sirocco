from collections.abc import Iterator
from datetime import datetime
from typing import Any

from isoduration import parse_duration
from isoduration.types import Duration


class TimeUtils:
    @staticmethod
    def duration_is_less_equal_zero(duration: Duration) -> bool:
        if (
            duration.date.years == 0
            and duration.date.months == 0
            and duration.date.days == 0
            and duration.time.hours == 0
            and duration.time.minutes == 0
            and duration.time.seconds == 0
            or (
                duration.date.years < 0
                or duration.date.months < 0
                or duration.date.days < 0
                or duration.time.hours < 0
                or duration.time.minutes < 0
                or duration.time.seconds < 0
            )
        ):
            return True
        return False


def convert_to_date(value: Any) -> datetime:
    match value:
        case datetime():
            return value
        case str():
            return datetime.fromisoformat(value)
        case _:
            raise TypeError


def convert_to_duration(value: Any) -> Duration:
    match value:
        case Duration():
            return value
        case str():
            return parse_duration(value)
        case _:
            raise TypeError


def convert_to_date_or_none(value: Any) -> datetime | None:
    return None if value is None else convert_to_date(value)


def iter_yaml_item(values: Any) -> Iterator[Any]:
    if isinstance(values, list):
        yield from values
    else:
        yield values


def convert_to_date_list(values: Any) -> list[datetime]:
    return [convert_to_date(item) for item in iter_yaml_item(values)]


def convert_to_duration_list(values: Any) -> list[Duration]:
    return [convert_to_duration(item) for item in iter_yaml_item(values)]
