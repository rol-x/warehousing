import os
import time
from shutil import copytree, rmtree

import config
import pandas as pd

from services.logs_service import log
from services.flags_service import calculate_checksum, get_database_checksum, \
                                   get_validated_checksums


# Ensure at least one proper data-gathering run is completed
def ensure_complete_dataset():
    if not os.path.getsize('/flags/validated-checksums.sha1'):
        log(" - No validated checksums yet. Waiting for "
            + "data-gathering to complete the first run.")
    while not os.path.getsize('./flags/validated-checksums.sha1'):
        time.sleep(15)


# Detect changes in data directory based on calculated checksums
def wait_for_new_data():

    # Ensure at least one proper data-gathering run is completed
    ensure_complete_dataset()

    # Wait until new verified dataset is present
    while True:

        # Check if there are differences between database and local files
        if calculate_checksum('./data') == get_database_checksum():
            log(" - Newest data already in database. Waiting 30 minutes.")
            time.sleep(30 * 60)
            continue

        # Check if ready, validated dataset is waiting for us to register
        if calculate_checksum('./data') \
                in get_validated_checksums():
            log(" - Verified new data available for database update.")
            break

        # Some change in files was detected, ensure it's a proper dataset
        log(" - New data found, but is not complete. Waiting 5 minutes.")
        time.sleep(5 * 60)


# Copy data directory, save the checksum as global variable
def isolate_data():
    config.NEW_CHECKSUM = calculate_checksum('./data')
    if os.path.exists('./.data'):
        rmtree('./.data')
    copytree('./data', './.data')
    log("Data isolated.")
    log("Checksum: %s" % calculate_checksum('./.data'))


# Remove created temporary directory for data files
def clean_up():
    rmtree('./.data')
    log("Cleaned up.")


# Try to load a .csv file content into a dataframe.
def load_df(entity_name):
    '''Try to return a dataframe from the respective .csv file.'''
    if entity_name == 'sale_offer' and config.FILE_PART > 1:
        entity_name += f'_{config.FILE_PART}'
    try:
        df = pd.read_csv('./data/' + entity_name + '.csv', sep=';')
    except pd.errors.EmptyDataError as empty_err:
        log(f'Please prepare the headers and data in {entity_name}.csv!\n')
        log(str(empty_err))
        return None
    except pd.errors.ParserError as parser_err:
        log(f'Parser error while loading {entity_name}.csv\n')
        log(str(parser_err))
        return secure_load_df(entity_name)
    except Exception as e:
        log(f'Exception occured while loading {entity_name}.csv\n')
        log(str(e))
        return None
    return df


# Try to securely load a dataframe from a .csv file.
def secure_load_df(entity_name):
    '''Try to securely load a dataframe from a .csv file.'''
    try:
        df = pd.read_csv('./data' + entity_name + '.csv',
                         sep=';', error_bad_lines=False)
    except pd.errors.ParserError as parser_err:
        log(parser_err)
        log("Importing data from csv failed - aborting.\n")
        raise SystemExit from parser_err
    return df
