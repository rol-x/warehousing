import os
from datetime import datetime

import globals
from handlers.data_handler import load_df
from handlers.log_handler import log


# Prepare the local log files.
def prepare_log_files():
    '''Prepare the local log files.'''
    if not os.path.exists('logs'):
        os.mkdir('logs')
        if not os.path.exists('logs'):
            os.mkdir('logs/data-gathering')
        print("Logs directory created")

    logfile = open('logs/data-gathering/' + globals.log_filename,
                   "a+", encoding="utf-8")
    if os.path.getsize('logs/data-gathering/' + globals.log_filename):
        log(" = Separate code execution = \n")
    else:
        log(" = Creation of this file = \n")
    logfile.close()


# Add the current date, return the date ID and its log file name.
def add_date():
    '''Add the current date, return the date ID and its log file name.'''

    # Load the date dataframe
    date_df = load_df('date')

    # Prepare the attributes
    now = datetime.now()
    date = now.strftime("%d/%m/%Y").split("/")
    day = int(date[0])
    month = int(date[1])
    year = int(date[2])
    weekday = now.weekday() + 1
    date_ID = len(date_df.index) + 1

    # Create a log filename from the datetime
    # global globals.log_filename
    globals.log_filename = now.strftime("%d%m%Y_%H%M") + ".log"
    prepare_log_files()

    # Check for the same datetime record
    same_date = date_df[(date_df['day'] == int(day))
                        & (date_df['month'] == int(month))
                        & (date_df['year'] == int(year))]['date_ID']
    if(len(same_date) > 0):
        log(f'Date {day}/{month}/{year} '
            + f'already added (date ID: {same_date.values[0]})')
        return same_date.values[0]

    # Save the date locally
    save_date(date_ID, day, month, year, weekday)

    # Return the current date ID
    return date_ID


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
