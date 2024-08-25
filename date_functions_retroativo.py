from datetime import datetime, timedelta
from workalendar.america import Brazil


def dmenos(dt):
    cal = Brazil()
    previous_day = dt - timedelta(days=1)
    # while not cal.is_working_day(datetime(previous_day.year, previous_day.month, previous_day.day)) or cal.is_holiday(datetime(previous_day.year, previous_day.month, previous_day.day)):
    #     previous_day -= timedelta(days=1)
    return previous_day


def dmais(dt):
    cal = Brazil()
    next_day = dt + timedelta(days=1)
    # while not cal.is_working_day(datetime(previous_day.year, previous_day.month, previous_day.day)) or cal.is_holiday(datetime(previous_day.year, previous_day.month, previous_day.day)):
    #     next_day += timedelta(days=1)
    return next_day


def dates(dt):
    # Formatting today's date
    if dt.day < 10:
        dd = "0" + str(dt.day)
    else:
        dd = str(dt.day)

    if dt.month < 10:
        mm = "0" + str(dt.month)
    else:
        mm = str(dt.month)

    yy = str(dt.year)

    # datatxt: today's date in dd/mm/yyyy format
    datatxt = f"{dd}/{mm}/{yy}"

    # dataname1: today's full datetime string
    dataname1 = str(dt)

    # datasql: today's date in yyyy-mm-dd format
    datasql = f"{yy}-{mm}-{dd}"

    # Get yesterday's date
    yesterday = dt - timedelta(days=1)

    # Formatting yesterday's date
    if yesterday.day < 10:
        dd2 = "0" + str(yesterday.day)
    else:
        dd2 = str(yesterday.day)

    if yesterday.month < 10:
        mm2 = "0" + str(yesterday.month)
    else:
        mm2 = str(yesterday.month)

    yy2 = str(yesterday.year)

    # datatxt2: yesterday's date in dd/mm/yyyy format
    datatxt2 = f"{dd2}/{mm2}/{yy2}"

    # dataname2: full datetime string for yesterday
    dataname2 = str(yesterday)

    # Get the day before yesterday's date
    day_before_yesterday = dt - timedelta(days=2)

    # Formatting the day before yesterday's date
    if day_before_yesterday.day < 10:
        dd3 = "0" + str(day_before_yesterday.day)
    else:
        dd3 = str(day_before_yesterday.day)

    if day_before_yesterday.month < 10:
        mm3 = "0" + str(day_before_yesterday.month)
    else:
        mm3 = str(day_before_yesterday.month)

    yy3 = str(day_before_yesterday.year)

    # datatxt3: day before yesterday's date in dd/mm/yyyy format
    datatxt3 = f"{dd3}/{mm3}/{yy3}"

    # dataname3: full datetime string for the day before yesterday
    dataname3 = str(day_before_yesterday)

    # Returning all relevant date formats
    return (
        datatxt,
        dataname1,
        datasql,
        datatxt2,
        dataname2,
        datatxt3,
        dataname3,
    )
