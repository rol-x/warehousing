import os

import pandas as pd
from checksumdir import dirhash

from handlers.log_handler import log


# Try to securely load a dataframe from a .csv file.
def secure_load_df(entity_name):
    '''Try to securely load a dataframe from a .csv file.'''
    try:
        df = pd.read_csv('./data' + entity_name + '.csv', sep=';',
                         error_bad_lines=False)
    except pd.errors.ParserError as parser_err:
        log(parser_err)
        os.system.exit("Importing data from csv failed - aborting.\n")
    return df


# Return generated hash based on the contents of data directory
def generate_data_hash():
    return str(dirhash('./data', 'sha1'))


# Save given data hash to an external file
def save_hash(hash):
    with open('data-checksum.sha1', 'w+') as hash_file:
        hash_file.write(hash)
