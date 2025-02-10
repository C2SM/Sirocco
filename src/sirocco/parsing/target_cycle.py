from dataclasses import dataclass
from datetime import datetime

from isoduration.types import Duration


class TargetCycle:
    pass


class NoTargetCycle(TargetCycle):
    pass


@dataclass(kw_only=True)
class DateList(TargetCycle):
    dates: list[datetime]


@dataclass(kw_only=True)
class LagList(TargetCycle):
    lags: list[Duration]
