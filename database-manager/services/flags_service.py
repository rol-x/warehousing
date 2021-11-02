import os

import config
from checksumdir import dirhash
from services.logs_service import log


# Create flags directory and validated checksums file.
def setup_flags():

    # Ensure the flags directory exists.
    if not os.path.exists('./flags'):
        os.mkdir('./flags')

    # Create a file for storing checksums of validated datasets
    create_validated_checksums_file()


# Return calculated checksum based on the contents of data directory
def calculate_checksum(directory_path):
    return str(dirhash(directory_path, 'sha1'))


# Create the checksums file for storing validated datasets.
def create_validated_checksums_file():
    '''Create the checksums file for storing validated datasets.'''
    with open('./flags/validated-checksums.sha1', 'a+', encoding="utf-8"):
        pass


# Save given data chceksum to an external file
def save_validated_checksum(checksum):
    with open('./flags/validated-checksums.sha1', 'a+',
              encoding="utf-8") as checksums_file:
        checksums_file.write(checksum + "\n")


# Get checksums of data files that has been validated
def get_validated_checksums():
    with open('./flags/validated-checksums.sha1', 'r',
              encoding="utf-8") as checksum_file:
        checksums = [line.strip('\n') for line in checksum_file.readlines()]
    return checksums


# Create the database checksum file.
def create_database_checksum_file():
    with open('./flags/database-checksum.sha1', 'a+', encoding='utf-8'):
        pass


# Save the cached database files checksum.
def save_database_checksum(checksum):
    with open('./flags/database-checksum.sha1', 'w',
              encoding='utf-8') as checksum_file:
        checksum_file.write(checksum)
    log("New database checksum saved: %s" % config.NEW_CHECKSUM)


# Return saved checksum of the dataset currently stored in the database
def get_database_checksum():
    with open('./flags/database-checksum.sha1', 'r',
              encoding='utf-8') as checksum_file:
        checksum = checksum_file.readline().strip('\n')
    return checksum
