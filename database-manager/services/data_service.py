import os
import shutil
import time as tm

import config
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
        if file.split('.')[-1] == 'csv':
            shutil.copyfile('./data/' + file, './.data/' + file)
    log("Data isolated.")


# Remove created temporary directory for data files
def clean_up():
    shutil.rmtree('./.data', ignore_errors=True)
    log("Cleaned up.")


def select_table(entity: str) -> pd.DataFrame:
    return pd.read_sql_table(entity, config.DB.connect(), schema='gathering')


# TODO: Check out 'append'
def update_table(entity: str, df: pd.DataFrame) -> None:
    df.to_sql(entity, config.DB.connect(), schema='gathering',
              if_exists='replace', index=False, index_label='id')


# Load and return all the data in dataframes
def load_isolated_data():
    date = pd.read_csv('./.data/date.csv',
                       compression='gzip', sep=';', encoding="utf-8")
    card = pd.read_csv('./.data/card.csv',
                       compression='gzip', sep=';', encoding="utf-8")
    card_stats = pd.read_csv('./.data/card_stats.csv',
                             compression='gzip', sep=';', encoding="utf-8")
    seller = pd.read_csv('./.data/seller.csv',
                         compression='gzip', sep=';', encoding="utf-8")
    sale_offer = pd.read_csv('./.data/sale_offer.csv',
                             compression='gzip', sep=';', encoding="utf-8")

    return {"date": date, "card": card, "card_stats": card_stats,
            "seller": seller, "sale_offer": sale_offer}


def load_database_frame(table_content):
    return pd.DataFrame(
        table_content.get('values'), columns=table_content.get('columns'))


# TODO: Redo this, so that it works as intended.
# Return those date_ids from the new dataset which are not in the old one.
def compare_dates(old_dates, new_dates):
    old = set([tuple(row) for row in old_dates.values])
    new = set([tuple(row) for row in new_dates.values])
    return [row[1] for row in new if new not in old]


# TODO: Redo this, so that it works as intended. Check for id compliance.
def calculate_deltas(old_data, new_data):

    # Take the new data and load the differences into the database,
    old_data_rows = set([tuple(row) for row in old_data.values])
    new_data_rows = set([tuple(row) for row in new_data.values])
    to_be_inserted = [row for row in new_data_rows if row not in old_data_rows]
    to_be_deleted = [row for row in old_data_rows if row not in new_data_rows]

    log(f"TBI:  {len(to_be_inserted)}")
    log(f"TBD:  {len(to_be_deleted)}")
    log(to_be_deleted)
    tm.sleep(0.5)

    return to_be_deleted, to_be_inserted
