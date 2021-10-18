import time

import pandas as pd
from checksumdir import dirhash

from handlers.log_handler import log


# Detect changes in data directory based on calculated checksums
def register_change():
    last_hash = open("./flags/data-checksum.sha1", "r").readline()
    this_hash = generate_data_hash()
    change_registered = False

    # Timeout after update over four (4) hours
    while not change_registered:
        # Every thirty (30) minutes check whether the two hashes differ
        while this_hash == last_hash:
            log("Checksums checked - no changes detected. Waiting 30 minutes.")
            time.sleep(60 * 30)
            this_hash = generate_data_hash()
        log("Change in data files detected")

        # Every thirty (30) seconds check whether the update flag is active
        timeout = False
        start = time.time()
        while open("./flags/update_flag", "r").readline() == '1':

            # Exit on timeout after 4 hours
            if time.time() - start > 60 * 60 * 4:
                log("Waiting for the update to end timed out")
                reset_update_flag()
                timeout = True
                break

            # Wait before continuing
            log("Update flag active")
            time.sleep(30)

        # On exit, if the update ended properly, register a change
        if not timeout:
            log("Update flag not active")
            change_registered = True

    # On finished update or data migration the files are static
    this_hash = generate_data_hash()
    log("Changes ready to commence")
    log("Old hash: " + last_hash)
    log("New hash: " + this_hash)
    log("Saving new hash to the file")
    save_hash(this_hash)


# Return generated hash based on the contents of data directory
def generate_data_hash():
    return str(dirhash('./data', 'sha1'))


# Save given data hash to an external file
def save_hash(hash):
    with open('./flags/data-checksum.sha1', 'w+') as hash_file:
        hash_file.write(hash)


# Set up the file with information about ongoing update.
def set_update_flag():
    '''Set up the file with information about ongoing update.'''
    update_flag = open('./flags/update_flag', 'w')
    update_flag.write('1')
    update_flag.close()


# Update the flag about the end of the update
def reset_update_flag():
    update_flag = open('./flags/update_flag', 'w')
    update_flag.write('0')
    update_flag.close()


# Try to securely load a dataframe from a .csv file.
def secure_load_df(entity_name):
    '''Try to securely load a dataframe from a .csv file.'''
    try:
        df = pd.read_csv('./data' + entity_name + '.csv',
                         sep=';', error_bad_lines=False)
    except pd.errors.ParserError as parser_err:
        log(parser_err)
        log("Importing data from csv failed - aborting.\n")
        reset_update_flag()
        raise SystemExit
    return df
