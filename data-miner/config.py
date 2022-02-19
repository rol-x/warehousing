"""Store and initialize global configuration of the program."""

# Control variables
OVER_10M = True

# Variables connected to this single run of the code
DATE_ID = 1
DB_CONN = None
MAIN_LOGNAME = 'other_main.log'
RUN_LOGNAME = 'other_run.log'
DB_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'mysql_database',
    'database': 'gathering',
    'raise_on_warnings': False
}

# Fixed variables
CONTAINER_DELAY = 60
NAME = 'data-miner'
