from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


class When(ABC):
    @abstractmethod
    def is_active(self, date: datetime | None) -> bool:
        raise NotImplementedError


class AnyWhen(When):
    def is_active(self, date: datetime | None) -> bool:  # noqa: ARG002  # dummy argument needed
        return True


@dataclass(kw_only=True)
class AtDate(When):
    at: datetime

    def is_active(self, date: datetime | None) -> bool:
        if date is None:
            msg = "Cannot use a when.at specification in a one-off cycle"
            raise ValueError(msg)
        return date == self.at


@dataclass(kw_only=True)
class BeforeAfterDate(When):
    before: datetime | None = None
    after: datetime | None = None

    def is_active(self, date: datetime | None) -> bool:
        if date is None:
            msg = "Cannot use a when.before or when.after specification in a one-off cycle"
            raise ValueError(msg)
        return (self.before is None or date < self.before) and (self.after is None or date > self.after)
