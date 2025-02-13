from abc import ABC, abstractmethod
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, BeforeValidator

from sirocco.parsing._utils import convert_to_date, convert_to_date_or_none


class When(ABC):
    @abstractmethod
    def is_active(self, date: datetime | None) -> bool:
        raise NotImplementedError


class AnyWhen(When):
    def is_active(self, date: datetime | None) -> bool:  # noqa: ARG002  # dummy argument needed
        return True


class AtDate(When, BaseModel):
    at: Annotated[datetime, BeforeValidator(convert_to_date)]

    def is_active(self, date: datetime | None) -> bool:
        if date is None:
            msg = "Cannot use a when.at specification in a one-off cycle"
            raise ValueError(msg)
        return date == self.at


class BeforeAfterDate(When, BaseModel):
    before: Annotated[datetime | None, BeforeValidator(convert_to_date_or_none)] = None
    after: Annotated[datetime | None, BeforeValidator(convert_to_date_or_none)] = None

    def is_active(self, date: datetime | None) -> bool:
        if date is None:
            msg = "Cannot use a when.before or when.after specification in a one-off cycle"
            raise ValueError(msg)
        return (self.before is None or date < self.before) and (self.after is None or date > self.after)
