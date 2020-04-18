from datetime import datetime, timedelta, timezone


def datetime_to_iso8601_utc(datetime):
    """
    Accepts a datetime.datetime object with UTC timezone.
    Returns a string like: 2020-03-26T09:10:11Z
    """
    isoformat = datetime.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
    return f"{isoformat}Z"


def timedelta_to_iso8601_tz(timedelta):
    """
    Accepts a datetime.timedelta object.
    Returns a string like "+05:00" or "-05:00" (ie five hours ahead or behind).
    """
    abs_delta = datetime.utcfromtimestamp(abs(timedelta).seconds).strftime('%H:%M')
    return f"+{abs_delta}" if abs(timedelta) == timedelta else f"-{abs_delta}"


def iso8601_utc_to_datetime(iso8601z):
    """
    Accepts a string like: 2020-03-26T09:10:11Z
    Returns a datetime.datetime object with UTC timezone.
    """
    return datetime.fromisoformat(iso8601z.replace("Z", "+00:00"))


def iso8601_tz_to_timedelta(iso8601_tz):
    """
    Accepts a string like "+05:00" or "-05:00" (ie five hours ahead or behind).
    Returns a datetime.timedelta object.
    """
    hours, minutes = iso8601_tz[1:].split(':')
    hours = int(hours)
    minutes = int(minutes)
    r = timedelta(hours=int(hours), minutes=int(minutes))
    if iso8601_tz[0] == '-':
        r = -r
    return r


def commit_time_to_text(iso8601z, iso_offset):
    """
    Given an isoformat time in UTC, and a isoformat timezone offset,
    returns the time in a human readable format, for that timezone.
    """
    right_time = iso8601_utc_to_datetime(iso8601z)
    right_tzinfo = datetime.fromisoformat(iso8601z.replace("Z", iso_offset))
    return right_time.astimezone(right_tzinfo.tzinfo).strftime("%c %z")
