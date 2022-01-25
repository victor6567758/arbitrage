import time
from datetime import datetime


# Convert unix to date object
def convert_unix_to_datetime(unix):

    # Check if unix is a string or int & proceeds with correct conversion
    if type(unix).__name__ == 'str':
        unix = int(unix[0:10])
    else:
        unix = int(str(unix)[0:10])

    date = datetime.utcfromtimestamp(unix).strftime('%Y-%m-%d %H:%M:%S')
    return date


# Convert date to unix object
def convert_date_to_unix(date):

    # Check if datetime object or raise ValueError
    if type(date).__name__ == 'datetime':
        unixtime = int(time.mktime(date.timetuple()))
    else:
        raise ValueError('You are trying to pass a None Datetime object')
    return unixtime
