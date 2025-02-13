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
