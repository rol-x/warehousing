from datetime import datetime

from handlers.data_handler import load_df
from handlers.log_handler import log


# Save a single date to the date dataframe in .csv file.
def save_date(date_ID, day, month, year, weekday):
    '''Save a single card date to the date dataframe in .csv file.'''

    # Logging
    log('== Add date ==')
    log('Day:           ' + str(day))
    log('Month:         ' + str(month))
    log('Year:          ' + str(year))
    log('Date ID:       ' + str(date_ID) + '\n')

    # Writing
    with open('data/date.csv', 'a', encoding="utf-8") as date_csv:
        date_csv.write(str(date_ID) + ';')
        date_csv.write(str(day) + ';')
        date_csv.write(str(month) + ';')
        date_csv.write(str(year) + ';')
        date_csv.write(str(weekday) + '\n')
