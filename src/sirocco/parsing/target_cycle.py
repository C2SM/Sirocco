from datetime import datetime
from typing import Annotated

from isoduration.types import Duration
from pydantic import BaseModel, BeforeValidator, ConfigDict

from sirocco.parsing._utils import convert_target_dates, convert_to_duration_list


class TargetCycle:
    pass


class NoTargetCycle(TargetCycle):
    pass


class DateList(BaseModel, TargetCycle):
    dates: Annotated[list[datetime | str], BeforeValidator(convert_target_dates)]


class LagList(BaseModel, TargetCycle):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    lags: Annotated[list[Duration], BeforeValidator(convert_to_duration_list)]
