import pytz
from datetime import datetime

# get_time_stamp and get_time_string are modified based on
# snipper from: https://stackoverflow.com/questions/48416511/

def get_time_stamp(timezone_str='US/Eastern'):
    
    timezone = pytz.timezone(timezone_str)

    return datetime.now().astimezone(timezone).timestamp()

def get_time_string(timezone_str='US/Eastern'):
    
    timezone = pytz.timezone(timezone_str)
    fmt = '%Y-%m-%d %H:%M:%S %Z%z'

    return datetime.now().astimezone(timezone).strftime(fmt)


def alt_utc():
    """
    Alternative for UTC time only.
    """
    utc_datetime = datetime.utcnow()
    utc_timestamp = utc_datetime.timestamp()

    utc_str = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    # Alternative: convert UTC timestamp to datetime then to string
    # datetime.datetime.fromtimestamp(d).strftime("%Y-%m-%d %H:%M:%S")
    
    return utc_timestamp, utc_str
