"""Scrape the card market website to get all the neccessary data."""
import os
from datetime import datetime

log_filename = 'init.log'


# Set the current log filename
def setup_logging():
    global log_filename
    log_filename = datetime.now().strftime("%d%m%Y_%H%M") + ".log"
    if not os.path.exists('./logs/database-manager'):
        os.mkdir('./logs/database-manager')
    data_checksum = open('./flags/data-checksum.sha1', 'a+')
    data_checksum.close()


# Log a message to a local file and the console.
def log(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    global log_filename
    with open('./logs/database-manager/' + log_filename, 'a+',
              encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)
