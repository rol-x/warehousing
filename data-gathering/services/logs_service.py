import os
from datetime import datetime

import config


# Create the logs directory.
def setup_logs():

    # Create main logs directory
    if not os.path.exists('./logs'):
        os.mkdir('./logs')

    # Create service logs directory
    if not os.path.exists(f'./logs/{config.NAME}'):
        os.mkdir(f'./logs/{config.NAME}')


# Prepare the main log file.
def setup_main_logfile():
    '''Prepare the main log file.'''
    config.MAIN_LOGNAME = datetime.now().strftime("%d%m%Y") + ".log"
    log(f"Service {config.NAME} is running." + "\n")


# Prepare the local log files for single run.
def setup_run_logfile():
    '''Prepare the local log files for single run.'''
    config.RUN_LOGNAME = datetime.now().strftime("%d%m%Y_%H%M") + ".log"
    with open(f"./logs/{config.NAME}/{config.RUN_LOGNAME}", "a+",
              encoding="utf-8"):
        pass
    if os.path.getsize(f"./logs/{config.NAME}/{config.RUN_LOGNAME}"):
        logr(" = Separate code execution = \n")
    else:
        logr(" = Creation of this file = \n")


# Log a message to a local file and the console.
def logr(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    with open(f"./logs/{config.NAME}/{config.RUN_LOGNAME}", 'a+',
              encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)


# Log the current url to the console and log file.
def log_url(url):
    '''Log the current url to the console and log file.'''
    logr("URL change  ->  " + url)


# Log a message to the main run file and the console.
def log(msg):
    '''Log a message to the main run file and the console.'''
    msg = str(msg)
    with open(f'./logs/{config.NAME}/{config.MAIN_LOGNAME}',
              'a+', encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)


# Log progress of gathered info (done card by card).
def log_progress(card_name, progress, cards_total):
    '''Log progress of gathered info (done card by card).'''
    logr(f" == {card_name} ==    ({progress}/{cards_total}  "
         + str(round(100*progress/cards_total, 2))
         + "%)")
