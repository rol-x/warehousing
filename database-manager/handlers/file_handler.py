import os
import subprocess

import pandas as pd

from handlers.log_handler import log
from checksumdir import dirhash


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


# Generate a hash based on the contents of data directory
def generate_data_hash():
    sha1_hash = dirhash('./data', 'sha1')
    with open('./database-manager/data-checksum.sha1', 'w+') as hash_file:
        hash_file.write(sha1_hash)
    return sha1_hash
