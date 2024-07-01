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
    if dt.day < 10:
        dd = "0" + str(dt.day)
    else:
        dd = str(dt.day)
    if dt.month < 10:
        mm = "0" + str(dt.month)
    else:
        mm = str(dt.month)
    yy = str(dt.year)

    datatxt = f"{dd}/{mm}/{yy}"
    dataname1 = str(dt)
    datasql = f"{yy}-{mm}-{dd}"
    dataname2 = str(dt - timedelta(days=1))

    return datatxt, dataname1, datasql, dataname2
