from datetime import datetime, timezone


def to_iso8601_utc(datetime):
    """
    Accepts a datetime.datetime object with UTC timezone.
    Returns a string like: 2020-03-26T09:10:11Z
    """
    isoformat = datetime.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
    return f"{isoformat}Z"


def to_iso8601_tz(timedelta):
    """
    Accepts a datetime.timedelta object.
    Returns a string like "+05:00" or "-05:00" (ie five hours ahead or behind).
    """
    abs_delta = datetime.utcfromtimestamp(abs(timedelta).seconds).strftime('%H:%M')
    return f"+{abs_delta}" if abs(timedelta) == timedelta else f"-{abs_delta}"


def commit_time_to_text(iso8601z, iso_offset):
    """
    Given an isoformat time in UTC, and a isoformat timezone offset,
    returns the time in a human readable format, for that timezone.
    """
    right_time = datetime.fromisoformat(iso8601z.replace("Z", "+00:00"))
    right_tzinfo = datetime.fromisoformat(iso8601z.replace("Z", iso_offset))
    return right_time.astimezone(right_tzinfo.tzinfo).strftime("%c %z")
