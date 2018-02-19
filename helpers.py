import datetime as dt
from dateutil.relativedelta import relativedelta

def get_quarter(date):
    """
    Returns the calendar quarter of `date`
    https://stackoverflow.com/questions/46019099/get-start-and-end-date-of-quarter-from-date-and-fiscal-year-end
    """
    return 1+(date.month-1)//3

def get_quarter_start_end(quarter, year=None):
    """
    Returns datetime.daet object for the start
    and end dates of `quarter` for the input `year`
    If `year` is none, it defaults to the current
    year.
    https://stackoverflow.com/questions/46019099/get-start-and-end-date-of-quarter-from-date-and-fiscal-year-end
    """
    if year is None:
        year = dt.datetime.now().year
    d = dt.date(year, 1+3*(quarter-1), 1)
    return d, d+relativedelta(months=3, days=-1)

def get_date_endpoints(q=None, year=2018, kind='year'):
    """
    Return start, end points as tuple for a year or year/quarter combination
    """
    if kind=='year':
        return (dt.date(year, 1, 1), dt.date(year, 12, 31))
    elif kind=='quarter':
        return get_quarter_start_end(q,year=year)

