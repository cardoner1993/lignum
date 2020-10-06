import calendar

import pytz
import datetime
from dateutil.tz import tzoffset
from pytz.tzinfo import tzinfo
from dateutil.relativedelta import relativedelta, MO


DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
DATE_AND_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TIME_MILLISECS_FORMAT = '%H:%M:%S.%f'
AVAILABLE_TIME_FORMATS = [TIME_FORMAT, TIME_MILLISECS_FORMAT]
DEFAULT_DATETIME_FORMAT = '%Y-%m-%dT%H:%M'
MADRID_TIME_ZONE = 'Europe/Madrid'
UTC_TIME_ZONE = 'UTC'
DEFAULT_TIME_ZONE = MADRID_TIME_ZONE
OFFSET_FORMAT = '%z'
MONTH_FORMAT = '%Y-%m'
YEAR_FORMAT = '%Y'
WEEK_FORMAT = '%Y-%W'



def def_tz():
    """
    :return: Default pytz time zone
    """
    return pytz.timezone(DEFAULT_TIME_ZONE)


def utc_tz():
    """
    :return: UTC pytz time zone
    """
    return pytz.utc


def time_zone(tz):
    """
    Parse time zone
    :param tz: time zone representation
    :return: a tzinfo object representing the passed timezone
    :raise ValueError: if tz is an invalid time zone representation
    """
    if isinstance(tz, str):
        return pytz.timezone(tz)
    elif issubclass(type(tz), tzinfo):
        return tz
    else:
        raise ValueError("Time Zone must be a str or BaseTzInfo type")


def parse_iso_8601_offset(offset):
    if offset is None:
        raise ValueError("TZ must be not None")
    if offset == 'Z':
        return datetime.timedelta(hours=0)
    if len(offset) == 3:
        return datetime.timedelta(hours=int(offset))
    if len(offset) == 5:
        sign = offset[0]
        hours = int(offset[1:2])
        minutes = int(offset[3:4])
        delta = datetime.timedelta(hours, minutes)
        return -delta if sign is '+' else delta
    if len(offset) == 6:
        sign = offset[0]
        hours = int(offset[1:2])
        minutes = int(offset[4:5])
        delta = datetime.timedelta(hours, minutes)
        return -delta if sign is '+' else delta
    raise ValueError("Invalid ISO 8601 format")


def is_localized(dt):
    """
    :param dt: datetime object
    :return: true if datetime object contains tzinfo otherwise false
    """
    return dt.tzinfo is not None


def replace_tz(dt, tz=def_tz()):
    """
    :param dt: datetime object
    :param tz: time zone representation
    :return: new datetime object with the timezone replaced by tz
    """
    dt = dt.replace(tzinfo=None)
    if isinstance(tz, tzoffset):
        return dt.astimezone(tz)
    else:
        return tz.localize(dt)


def unlocalize(dt):
    """
    :param dt: datetime object
    :return: remove tzinfo from the datetime object
    """
    return dt.replace(tzinfo=None)


def localize(dt, tz=def_tz()):
    """
    :param dt: datetime object
    :param tz: time zone representation
    :return: set timezone if not is_localized otherwise convert
    """
    tz = time_zone(tz)
    return dt.astimezone(tz) if is_localized(dt) else replace_tz(dt, tz)


def apply_offset(dt, offset):
    return dt + parse_iso_8601_offset(offset)


def normalize(dt, tz=def_tz()):
    tz = time_zone(tz)
    return tz.normalize(dt)


def datetime_now(tz=None):
    """
    :param tz: time zone representation (None for no tzinfo)
    :return: now datetime object
    """
    tz = time_zone(tz) if tz else None
    dt = datetime.datetime.now(tz=tz)
    return dt


def date_now(tz=None):
    """
    :param tz: time zone representation (None for no tzinfo)
    :return: now date object
    """
    return datetime_now(tz).date()


def time_now(tz=None):
    """
    :param tz: time zone representation (None for no tzinfo)
    :return: now time object
    """
    return datetime_now(tz).time()


def week_day_now(tz=None):
    """
    :param tz: time zone representation (None for no tzinfo)
    :return: iso week day integer
    """
    return date_now(tz).isoweekday()


def is_weekend_now():
    '''
    :return: True if today is weekend
    '''
    return week_day_now() > 5


def combine(date, time, tz=None):
    """
    :param date: date object
    :param time: time object
    :param tz: time zone
    :return: the combination of date + time
    """
    dt = datetime.datetime.combine(date, time)
    if tz is not None:
        dt = localize(dt, tz)
    return dt


def truncate(dt, hour=True, minute=True, second=True, micro=True):
    """
    Truncate DateTime
    :param hour: truncate hour
    :param minute: truncate minute
    :param second: truncate second
    :param micro: truncate micro
    :return: truncated datetime
    """
    if hour:
        dt = dt.replace(hour=0)
    if minute:
        dt = dt.replace(minute=0)
    if second:
        dt = dt.replace(second=0)
    if micro:
        dt = dt.replace(microsecond=0)
    return dt



def from_datetime_to_str(dt, fmt=DEFAULT_DATETIME_FORMAT):
    # type: (datetime, str) -> str
    """
    :param dt: datetime object
    :param fmt: string format
    :return: string representing the datetime object
    """
    return dt.strftime(fmt)


def datetimestr_now(tz=None, fmt=DEFAULT_DATETIME_FORMAT):
    return from_datetime_to_str(datetime_now(tz), fmt=fmt)


def from_str_to_datetime(string, fmt=DEFAULT_DATETIME_FORMAT, tz=None):
    """
    :param string: datetime string representation
    :param fmt: string format
    :param tz: time zone representation (None for no tzinfo)
    :return: datetime object of the string representation
    """
    dt = datetime.datetime.strptime(string, fmt)
    if tz is not None:
        dt = localize(dt, tz)
    return dt


def from_datetime_to_epoch(dt):
    """
    :param dt: datetime object
    :return: epoch time
    """
    utcDt = localize(dt, utc_tz())
    return calendar.timegm(utcDt.timetuple())



def from_epoch_to_datetime(epoch, tz=def_tz()):
    """
    :param epoch: epoch time
    :param tz: time zone representation
    :return: localized datetime object of the epoch time
    """
    utcDt = replace_tz(datetime.datetime.utcfromtimestamp(epoch), utc_tz())
    return localize(utcDt, tz)


def apply_day_delta(dt, days=1, fmt=DATE_FORMAT):
    if isinstance(dt, datetime.datetime):
        if is_localized(dt):
            tz = dt.tzinfo
            dt = unlocalize(dt)
            dt += datetime.timedelta(days=days)
            return normalize(localize(dt, tz), tz)
        else:
            return apply_delta(dt, days=days)
    elif isinstance(dt, datetime.date):
        return dt + datetime.timedelta(days=days)
    elif isinstance(dt, str):
        try:
            datetime.datetime.strptime(dt, fmt)
            return sanitize_to_datestr(apply_day_delta(sanitize_to_date(dt, fmt), days=days), fmt=fmt)
        except ValueError:
            raise ValueError("Invalid date format")
    else:
        raise ValueError('dt must be a date, datestr or datetime')


def apply_time_delta(dt, hours=0, minutes=0, seconds=0):
    if is_localized(dt):
        tz = dt.tzinfo
        dt += datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
        return normalize(dt, tz)
    else:
        return apply_delta(dt, hours=hours, minutes=minutes, seconds=seconds)


def apply_delta(dt, days=0, hours=0, minutes=0, seconds=0):
    """
    :param dt: datetime object
    :param days: delta days
    :param hours: delta hours
    :param minutes: delta minutes
    :param seconds: delta seconds
    :return: new datetime object applying delta
    """
    if is_localized(dt):
        raise ValueError('Only can apply dt delta to naive date times')
    return dt + datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def interval(begin, end, delta=datetime.timedelta(hours=1)):
    """
    :param begin: datetime begin
    :param end: datetime end
    :param delta: timedelta object to apply
    :return: datetime iterator with interval
    """
    if delta.total_seconds() == 0:
        raise ValueError("Time delta must be positive or negative")
    if delta.total_seconds() < 0 and end > begin:
        raise ValueError("For a negative time delta begin must be greater or equal than end")
    if delta.total_seconds() > 0 and end < begin:
        raise ValueError("For a positive time delta begin must be less or equal than end")
    date = begin
    while date <= end:
        yield date
        date += delta

def date_iterator(dateIni, dateFi, daysDelta=1, format=DATE_FORMAT):
    '''
    Date iterator with delta specified in days (default 1). Only closed left i.e. [dateIni, dateFi).
    If dates provided are str, the elements returned by the iterator will be also str.
    dateIni and dateFi must to be same type.
    :param dateIni:
    :param dateFi:
    :param daysDelta:
    :param format:
    :return:
    '''
    if type(dateIni) != type(dateFi):
        raise ValueError('Date types differ: %s, %s' % (str(dateIni), str(dateFi)))
    if sanitize_to_date(dateIni) >  sanitize_to_date(dateFi):
        raise ValueError('dateIni %s > dateFi %s' % (sanitize_to_datestr(dateIni), sanitize_to_datestr(dateFi)))

    i = dateIni
    while i < dateFi:
        yield i
        i = apply_day_delta(i, days=daysDelta, fmt=format)


"""
    Sanitize Date Input
"""


def is_date_str(date, fmt=DATE_FORMAT):
    try:
        datetime.datetime.strptime(date, fmt)
        return True
    except ValueError:
        return False


def sanitize_to_date(date, fmt=DATE_FORMAT):
    if isinstance(date, datetime.datetime):
        return date.date()
    elif isinstance(date, datetime.date):
        return date
    elif isinstance(date, str):
        return from_str_to_datetime(date, fmt).date()
    else:
        raise ValueError("Date must be datetime, date or str")


def sanitize_to_datetime(dt, fmt=DEFAULT_DATETIME_FORMAT):
    if isinstance(dt, datetime.date):
        return datetime.datetime.combine(dt, datetime.datetime.min.time())
    elif isinstance(dt, datetime.datetime):
        return dt
    elif isinstance(dt, str):
        return from_str_to_datetime(dt, fmt)
    else:
        raise ValueError("Datetime must be datetime, date or str")


def sanitize_to_datestr(date, fmt=DATE_FORMAT):
    if isinstance(date, str):
        try:
            datetime.datetime.strptime(date, fmt)
            return date
        except ValueError:
            raise ValueError("Invalid date format")
    if isinstance(date, datetime.date) or isinstance(date, datetime.datetime):
        return from_datetime_to_str(date, fmt)
    else:
        raise ValueError("Date must be datetime, date or str")


def sanitize_to_timestr(t, fmt=TIME_FORMAT):
    assert fmt in AVAILABLE_TIME_FORMATS, 'Format '+fmt+' should be in available time formats list!'
    if isinstance(t, str):
        try:
            datetime.datetime.strptime(t, fmt)
            return t
        except ValueError:
            raise ValueError("Invalid time format "+TIME_FORMAT)
    if isinstance(t, datetime.time) or isinstance(t, datetime.datetime):
        return from_datetime_to_str(t, fmt)
    else:
        raise ValueError("Time must be datetime, time or str")


def datestr_now(tz=None, fmt=DATE_FORMAT):
    return sanitize_to_datestr(date_now(tz=tz), fmt=fmt)


def get_monthstr(date):
    return from_datetime_to_str(sanitize_to_date(date), MONTH_FORMAT)


def get_quarterstr(date):
    sanDate = sanitize_to_date(date)
    quarter = (sanDate.month-1)//3+1
    return from_datetime_to_str(sanDate, YEAR_FORMAT)+'-Q'+str(quarter)

def get_yearstr(date):
    return from_datetime_to_str(sanitize_to_date(date), YEAR_FORMAT)


def datestr_now(tz=None, fmt=DATE_FORMAT):
    return sanitize_to_datestr(date_now(tz=tz), fmt=fmt)


def get_weekstr(date):
    return str(date.isocalendar()[0]) + '-W' + str(date.isocalendar()[1]).zfill(2)
    # return from_datetime_to_str(sanitize_to_date(date), WEEK_FORMAT)


"""
    Legacy Methods
"""


def iso_year_start(isoYear):
    "The gregorian calendar date of the first day of the given ISO year"
    fourthJan = datetime.date(isoYear, 1, 4)
    delta = datetime.timedelta(fourthJan.isoweekday() - 1)
    return fourthJan - delta


def iso_to_gregorian(isoYear, isoWeek, isoDay):
    "Gregorian calendar date for the given ISO year, week and day"
    yearStart = iso_year_start(isoYear)
    return yearStart + datetime.timedelta(days=isoDay - 1, weeks=isoWeek - 1)


def get_monday_of_corresponding_week(date):
    tupIso = date.isocalendar()
    mondayOfTheWeek = iso_to_gregorian(tupIso[0], tupIso[1], 1)
    return mondayOfTheWeek


def get_next_monday_of_corresponding_week(date):
    mondayOfTheWeek = get_monday_of_corresponding_week(date)
    nextMonday = mondayOfTheWeek + datetime.timedelta(days=1) + relativedelta(weekday=MO(+1))
    return nextMonday


def from_seconds_to_str_time_format(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))


def from_interval_to_date_range(firstDate, lastDate, left=True, right=True):
    dayDiff = (lastDate - firstDate).days
    datesRange = [firstDate + datetime.timedelta(days=i) for i in range(1 - left, dayDiff + right)]
    return datesRange


if __name__ == '__main__':
    dt = localize(datetime.datetime(2018, 3, 25, 3), tz='Europe/Madrid')  # 2018-03-25 03:00:00+02:00
    print(apply_time_delta(dt, hours=-24))  # 2018-03-24 02:00:00+01:00
    print(apply_day_delta(dt, days=-1))  # 2018-03-24 03:00:00+01:00
    print(apply_time_delta(dt, hours=-1))  # 2018-03-25 01:00:00+01:00

    print()

    dt = localize(datetime.datetime(2017, 10, 29, 2), tz='Europe/Madrid')  # 2017-10-29 02:00:00+01:00
    print(apply_time_delta(dt, hours=-24))  # 2017-10-28 03:00:00+02:00
    print(apply_day_delta(dt, days=-1))  # 2017-10-28 02:00:00+02:00
    print(apply_time_delta(dt, hours=-1))  # 2017-10-29 02:00:00+02:00
