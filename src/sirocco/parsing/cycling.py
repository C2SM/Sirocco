from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator  # noqa: TCH003 needed for pydantic
from dataclasses import dataclass
from datetime import datetime  # noqa: TCH003 needed for pydantic
from typing import Annotated, Self

from isoduration.types import Duration  # noqa: TCH002 needed for pydantic
from pydantic import BaseModel, BeforeValidator, ConfigDict, model_validator

from sirocco.parsing._utils import TimeUtils, convert_to_date, convert_to_duration


class CyclePoint:
    pass


class OneOffPoint(CyclePoint):
    def __str__(self) -> str:
        return "[]"


@dataclass(kw_only=True)
class DateCyclePoint(CyclePoint):
    """
    Dates of the current point in the cycle

    start_date and stop_date are the overall dates
    chunk_start_date and chunk_stop_date relate to the current chunk
    """

    start_date: datetime
    stop_date: datetime
    chunk_start_date: datetime
    chunk_stop_date: datetime

    def __str__(self) -> str:
        return f"[{self.chunk_start_date} -- {self.chunk_stop_date}]"


class Cycling(ABC):
    @abstractmethod
    def iter_cycle_points(self) -> Iterator[CyclePoint]:
        raise NotImplementedError


class OneOff(Cycling):
    def iter_cycle_points(self) -> Iterator[OneOffPoint]:
        yield OneOffPoint()


class DateCycling(BaseModel, Cycling):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    start_date: Annotated[datetime, BeforeValidator(convert_to_date)]
    stop_date: Annotated[datetime, BeforeValidator(convert_to_date)]
    period: Annotated[Duration, BeforeValidator(convert_to_duration)]

    @model_validator(mode="after")
    def check_dates_and_period(self) -> Self:
        if self.start_date > self.stop_date:
            msg = f"start_date {self.start_date!r} lies after given stop_date {self.stop_date!r}."
            raise ValueError(msg)
        if TimeUtils.duration_is_less_equal_zero(self.period):
            msg = f"period {self.period!r} is negative or zero."
            raise ValueError(msg)
        if self.start_date + self.period > self.stop_date:
            msg = f"period {self.period!r} larger than the duration between start date {self.start_date!r} and stop_date {self.stop_date!r}"
            raise ValueError(msg)
        return self

    def iter_cycle_points(self) -> Iterator[DateCyclePoint]:
        begin = self.start_date
        while begin < self.stop_date:
            end = min(begin + self.period, self.stop_date)
            yield DateCyclePoint(
                start_date=self.start_date, stop_date=self.stop_date, chunk_start_date=begin, chunk_stop_date=end
            )
            begin = end
