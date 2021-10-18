"""Scrape the card market website to get all the neccessary data."""
from datetime import datetime

log_filename = ''


# Set the current log filename
def setup_logging():
    global log_filename
    log_filename = datetime.now().strftime("%d%m%Y_%H%M") + ".log"


# Log a message to a local file and the console.
def log(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    global log_filename
    with open('logs/database-manager/' + log_filename, 'a+',
              encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)
