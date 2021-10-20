import time
import os
import config

import pandas as pd
from checksumdir import dirhash

from handlers.log_handler import log


# Return the current database files checksum.
def setup_database_checksum():
    with open('./flags/database-checksum.sha1', 'a+', encoding='utf-8'):
        pass


# Ensure at least one proper data-gathering run is completed
def ensure_complete_dataset():
    if not os.path.getsize('/flags/validated-checksums.sha1'):
        log("No validated checksums yet. Waiting for "
            + "data-gathering to complete the first run.")
    while not os.path.getsize('./flags/validated-checksums.sha1'):
        time.sleep(15)


# Detect changes in data directory based on calculated checksums
def register_change():

    # Ensure at least one proper data-gathering run is completed
    ensure_complete_dataset()

    # Check for changes in data directory
    while calculate_data_checksum() == read_database_checksum():
        log(" - Newest data already in database. Waiting 30 minutes.")
        time.sleep(30 * 60)

    # Some change in files was detected, ensure it's a proper dataset
    while calculate_data_checksum() not in read_validated_checksums():
        log("   - New data found, but is not complete. Waiting 5 minutes.")
        time.sleep(5 * 60)

    # Note new complete validated data ready to be updated
    log("     - Data validation received.")
    log("     - Old checksum: " + read_database_checksum())
    log("     - New checksum: " + calculate_data_checksum())
    log("     - Proceeding to the database update.")


# Get checksums of data files that has been validated
def read_validated_checksums():
    with open('./flags/validated-checksums.sha1', 'r',
              encoding="utf-8") as checksums_file:
        checksums = [line.strip('\n') for line in checksums_file.readlines()]
    return checksums


# Return saved checksum of the dataset currently stored in the database
def read_database_checksum():
    with open('./flags/database-checksum.sha1', 'r',
              encoding='utf-8') as checksum_file:
        checksum = checksum_file.readline().strip('\n')
    return checksum


# Return calculated checksum based on the contents of data directory
def calculate_data_checksum():
    return str(dirhash('./data', 'sha1'))


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
