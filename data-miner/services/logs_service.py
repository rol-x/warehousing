import os
import time as tm

import config


# Create the logs directory.
def setup_logs():

    # Create main logs directory
    if not os.path.exists('./logs'):
        os.mkdir('./logs')

    # Create service logs directory
    if not os.path.exists(f'./logs/{config.NAME}'):
        os.mkdir(f'./logs/{config.NAME}')

    # Prepare the main log filename
    config.MAIN_LOGNAME = tm.strftime("%d%m%Y", tm.localtime()) + ".log"


# Prepare the log file for a single run.
def setup_run_logfile():
    '''Prepare the log file for a single run.'''
    config.RUN_LOGNAME = tm.strftime("%d%m%Y_%H%M", tm.localtime()) + ".log"


# Log a message to a local file and the console.
def logr(msg):
    '''Log a message to a local file and the console.'''
    msg = str(msg)
    with open(f"./logs/{config.NAME}/{config.RUN_LOGNAME}", 'a+',
              encoding="utf-8") as logfile:
        timestamp = tm.strftime("%H:%M:%S", tm.localtime())
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)


# Log a message to the main log file and the console.
def log(msg):
    '''Log a message to the main log file and the console.'''
    msg = str(msg)
    with open(f'./logs/{config.NAME}/{config.MAIN_LOGNAME}',
              'a+', encoding="utf-8") as logfile:
        timestamp = tm.strftime("%H:%M:%S", tm.localtime())
        logfile.write(timestamp + ": " + msg + "\n")
    print(msg)
