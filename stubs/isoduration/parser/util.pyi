from isoduration.constants import PERIOD_PREFIX as PERIOD_PREFIX, TIME_PREFIX as TIME_PREFIX, WEEK_PREFIX as WEEK_PREFIX
from isoduration.parser.exceptions import OutOfDesignators as OutOfDesignators

def is_period(ch: str) -> bool: ...
def is_time(ch: str) -> bool: ...
def is_week(ch: str) -> bool: ...
def is_number(ch: str) -> bool: ...
def is_letter(ch: str) -> bool: ...
def parse_designator(designators: dict[str, str], target: str) -> str: ...
