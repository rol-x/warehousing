import os
from datetime import datetime

# TODO: Isolate config from this service
import config


# Create the logs directory.
def setup_logs():

    # Create main logs directory
    if not os.path.exists('./logs'):
        os.mkdir('./logs')

    # Create service logs directory
    if not os.path.exists(f'./logs/{config.NAME}'):
        os.mkdir(f'./logs/{config.NAME}')


# Prepare the daily log file.
def setup_daily_logfile():
    '''Prepare the daily log file.'''
    config.DAILY_LOGNAME = datetime.now().strftime("%d%m%Y") + ".log"
    log_daily(f"Service {config.NAME} is running." + "\n")


# Prepare the local log files for single run.
def setup_run_logfile():
    '''Prepare the local log files for single run.'''
    config.LOG_FILENAME = datetime.now().strftime("%d%m%Y_%H%M") + ".log"
    with open(f"./logs/{config.NAME}/{config.LOG_FILENAME}", "a+",
              encoding="utf-8"):
        pass
    if os.path.getsize(f"./logs/{config.NAME}/{config.LOG_FILENAME}"):
        log(" = Separate code execution = \n")
    else:
        log(" = Creation of this file = \n")


# Log a message to a local file and the console.
def log(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    with open(f"./logs/{config.NAME}/{config.LOG_FILENAME}", 'a+',
              encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)


# Log the current url to the console and log file.
def log_url(url):
    '''Log the current url to the console and log file.'''
    log("URL change  ->  " + url)


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
