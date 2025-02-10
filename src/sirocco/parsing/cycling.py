from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from isoduration.types import Duration


class CyclePoint:
    pass


class OneOffPoint(CyclePoint):
    def __str__(self) -> str:
        return "[]"


@dataclass(kw_only=True)
class DateCyclePoint(CyclePoint):
    start_date: datetime
    stop_date: datetime
    begin_date: datetime
    end_date: datetime

    def __str__(self) -> str:
        return f"[{self.begin_date} -- {self.end_date}]"


class Cycling(ABC):
    @abstractmethod
    def iter_cycle_points(self) -> Iterator[CyclePoint]:
        raise NotImplementedError


class OneOff(Cycling):
    def iter_cycle_points(self) -> Iterator[OneOffPoint]:
        yield OneOffPoint()


@dataclass(kw_only=True)
class DateCycling(Cycling):
    start_date: datetime
    stop_date: datetime
    period: Duration

    def iter_cycle_points(self) -> Iterator[DateCyclePoint]:
        begin = self.start_date
        while begin < self.stop_date:
            end = min(begin + self.period, self.stop_date)
            yield DateCyclePoint(start_date=self.start_date, stop_date=self.stop_date, begin_date=begin, end_date=end)
            begin = end
