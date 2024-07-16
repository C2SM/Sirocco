from isoduration.constants import PERIOD_PREFIX as PERIOD_PREFIX, TIME_PREFIX as TIME_PREFIX
from isoduration.formatter.checking import validate_date_duration as validate_date_duration
from isoduration.types import DateDuration as DateDuration, TimeDuration as TimeDuration

def format_date(date_duration: DateDuration, global_sign: int) -> str: ...
def format_time(time_duration: TimeDuration, global_sign: int) -> str: ...
