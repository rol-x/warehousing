import os
import shutil
import time as tm

import pandas as pd

from services.logs_service import log


# Ensure at least one proper data-gathering run is completed
def ensure_complete_dataset():
    if not os.path.getsize('/flags/validated-checksums.sha1'):
        log(" - No validated checksums yet. Waiting for "
            + "data-gathering to complete the first run.")
    while not os.path.getsize('./flags/validated-checksums.sha1'):
        tm.sleep(15)


# Copy data directory, save the checksum as global variable
def isolate_data():
    shutil.rmtree('./.data', ignore_errors=True)
    for file in os.listdir('./data'):
        shutil.copy('./data/' + file, './.data/' + file)


# Remove created temporary directory for data files
def clean_up():
    shutil.rmtree('./.data', ignore_errors=True)


def load_table(rows, columns):
    return pd.DataFrame(rows, columns=columns)


# Load and write the data without compression
def decompress_data():
    files = []
    dir = './.data/'
    for file in os.listdir(dir)[::-1]:
        if file.endswith('.csv'):
            log(f'Decompressing {file}...')
            df = pd.read_csv(
                dir + file, compression='gzip', sep=';', encoding="utf-8")

            # Transform boolean values to tinyint
            if "is_foiled" in df.columns:
                df["is_foiled"] = df["is_foiled"] \
                    .apply(lambda x: 1 if x else 0)

            df.to_csv('/var/lib/mysql-files/' + file,
                      sep=';', encoding="utf-8", index=False)
            files.append(file.split('.')[0])
    return files
