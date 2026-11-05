from typing import Annotated

from isoduration.types import Duration
from pydantic import BaseModel, BeforeValidator, ConfigDict

from sirocco.parsing._utils import TargetDate, convert_to_duration_list, convert_to_target_date_list


class TargetCycle:
    pass


class NoTargetCycle(TargetCycle):
    pass


class TargetDateList(BaseModel, TargetCycle):
    dates: Annotated[list[TargetDate], BeforeValidator(convert_to_target_date_list)]


class LagList(BaseModel, TargetCycle):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    lags: Annotated[list[Duration], BeforeValidator(convert_to_duration_list)]
