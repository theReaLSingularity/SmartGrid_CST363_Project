""" Module creates a calendar dataset csv for London, UK including days of week/month/year, weekends, and holidays. """

import pandas as pd
import numpy as np
from workalendar.europe import UnitedKingdom


def create_london_calendar():
    # Date range of interest
    start_date = pd.to_datetime('2012-01-01')
    end_date = pd.to_datetime('2014-02-27')

    # Creating calendar for UK for date range
    df = pd.Series(pd.date_range(start=start_date, end=end_date, freq='D')).to_frame(name='date')

    df['dow'] = df['date'].dt.dayofweek
    df['day'] = df['date'].dt.day # number of day of month
    df['month'] = df['date'].dt.month # number of month of year
    df['year'] = df['date'].dt.year # year
    df['doy'] = df['date'].dt.dayofyear # number of day of year

    df['is_weekend'] = (df['dow'] >= 5).astype(int) # 1 if day is a weekend day else 0

    # Fetching holidays for date range with workalendar
    years = [2012, 2013, 2014]
    uk = UnitedKingdom()
    holidays_list = []
    for year in years:
        for holiday_date, holiday_name in uk.holidays(year):
            if start_date.date() <= holiday_date <= end_date.date():
                holidays_list.append(holiday_date)
    holidays_set = set(holidays_list)

    # Marking holidays in dataset
    df['is_holiday'] = (df['date'].dt.date.isin(holidays_set)).astype(int)

    # Saving calendar dataset
    df.to_csv("data/london_calendar.csv", index=False)


def main():
    create_london_calendar()


if __name__ == '__main__':
    main()