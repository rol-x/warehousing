"""Scrape the card market website to get all the neccessary data."""
from datetime import datetime

import config


# Log the current url to the console and log file.
def log_url(url):
    '''Log the current url to the console and log file.'''
    log("URL change  ->  " + url)


# Log a message to a local file and the console.
def log(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    with open('./logs/data-gathering/' + config.LOG_FILENAME,
              'a+', encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)


# Log a message to the daily run file and the console.
def log_daily(msg):
    '''Log a message to the daily run file and the console.'''
    msg = str(msg)
    with open('./logs/data-gathering/' + config.DAILY_LOGNAME,
              'a+', encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)


# Log progress of gathered info (done card by card).
def log_progress(card_name, progress, cards_total):
    '''Log progress of gathered info (done card by card).'''
    log(f" == {card_name} ==    ({progress}/{cards_total}  "
        + str(round(100*progress/cards_total, 2))
        + "%)")
