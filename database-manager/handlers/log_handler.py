"""Scrape the card market website to get all the neccessary data."""
import os
from datetime import datetime

log_filename = 'other.log'


# Set the current log filename
def setup_logging():
    global log_filename
    log_filename = datetime.now().strftime("%d%m%Y") + ".log"
    if not os.path.exists('./logs/database-manager'):
        os.mkdir('./logs/database-manager')
    with open('./flags/data-checksum.sha1', 'a+', encoding="utf-8"):
        pass


# Log a message to a local file and the console.
def log(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    with open('./logs/database-manager/' + log_filename, 'a+',
              encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)
