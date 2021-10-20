import time

import pandas as pd
from checksumdir import dirhash

from handlers.log_handler import log


# Detect changes in data directory based on calculated checksums
def register_change():
    last_hash = get_checksums()[-1]
    this_hash = generate_data_hash()
    change_registered = False

    # Timeout after update over four (4) hours
    while not change_registered:

        # Every thirty (15) minutes check whether the two hashes differ
        while this_hash == last_hash:
            log("Checksums checked - no changes detected. Waiting 15 minutes.")
            time.sleep(60 * 15)
            this_hash = generate_data_hash()
        log(" - Change in data files detected.")

        # Every sixty (60) seconds check whether the update flag is active
        timeout = False
        start = time.time()

        # Wait for update to be over
        while True:
            with open("./flags/update-flag", "r",
                      encoding="utf-8") as update_flag:

                # Update over signalized by data-gathering
                if update_flag.readline() == '0':
                    break

            # Timeout after 4 hours
            if time.time() - start > 60 * 60 * 4:
                log("Waiting for the update to end timed out.")
                timeout = True
                break

            # Wait before continuing
            log(" - Update detected. Waiting 60 seconds.")
            time.sleep(60)

        # On exit, if the update ended properly, register a change
        if not timeout:
            log("   - No update detected. Registering a change.")
            this_hash = generate_data_hash()

            # Data found as validated in checksums - exit the scheduler loop
            if this_hash in get_checksums():
                log("   - Dataset checksum found in validated checksums.")
                change_registered = True
                break

            # When the data is downloaded, but not checked for completeness
            log("   - Waiting for data to be validated. Waiting 60 seconds.")
            time.sleep(60)

    # On finished update or data migration the files are static
    log("     - Changes ready to commence.")
    log("     - Old hash: " + last_hash)
    log("     - New hash: " + this_hash)
    log("     - Proceeding to update.")


# Return generated hash based on the contents of data directory
def generate_data_hash():
    return str(dirhash('./data', 'sha1'))


# Set up the file with information about ongoing update.
def set_update_flag():
    '''Set up the file with information about ongoing update.'''
    with open('./flags/update-flag', 'w', encoding="utf-8") as update_flag:
        update_flag.write('1')
    log("Update flag set to 1")


# Update the flag about the end of the update
def reset_update_flag():
    with open('./flags/update-flag', 'w', encoding="utf-8") as update_flag:
        update_flag.write('0')
    log("Update flag set to 0")


# Get checksums of data files that has been validated
def get_checksums():
    try:
        with open('./flags/validated-checksums.sha1', 'r',
                  encoding="utf-8") as hashes:
            checksums = [line.strip('\n') for line in hashes.readlines()]
    except FileNotFoundError:
        log("No checksums file found.")
        checksums = []
    return checksums


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
        raise SystemExit from parser_err
    return df
