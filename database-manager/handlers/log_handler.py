"""Scrape the card market website to get all the neccessary data."""
import os
from datetime import datetime

import config


# Set the current log filename
def setup_logging():
    config.LOG_FILENAME = datetime.now().strftime("%d%m%Y") + ".log"
    if not os.path.exists('./logs/database-manager'):
        os.mkdir('./logs/database-manager')


# Log a message to a local file and the console.
def log(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    with open('./logs/database-manager/' + config.LOG_FILENAME, 'a+',
              encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)
