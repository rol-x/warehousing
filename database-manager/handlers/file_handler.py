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
        log(" - New data found, but is not complete. Waiting 10 minutes.")
        time.sleep(10 * 60)


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


# Load and return all the data in dataframes
def load_isolated_data():
    date = pd.read_csv(
        './.data/date.csv', sep=';', encoding="utf-8")
    card = pd.read_csv(
        './.data/card.csv', sep=';', encoding="utf-8")
    card_stats = pd.read_csv(
        './.data/card_stats.csv', sep=';', encoding="utf-8")
    seller = pd.read_csv(
        './.data/seller.csv', sep=';', encoding="utf-8")
    sale_offers = []
    sale_offers.append(pd.read_csv(
            './.data/sale_offer.csv', sep=';', encoding="utf-8"))

    file_part = 2
    while os.path.exists(f'./.data/sale_offer_{file_part}.csv'):
        sale_offers.append(pd.read_csv(
            f'./.data/sale_offer_{file_part}.csv', sep=';', encoding="utf-8"))
        file_part += 1
    sale_offer = pd.concat(sale_offers)

    date = date.rename(columns={'date_ID': 'date_id',
                                'day_of_week': 'weekday'})
    card = card.rename(columns={'card_ID': 'card_id',
                                'card_name': 'name'})
    card_stats = card_stats.rename(columns={'date_ID': 'date_id',
                                            'card_ID': 'card_id',
                                            '30_avg_price': 'monthly_avg',
                                            '7_avg_price': 'weekly_avg',
                                            '1_avg_price': 'daily_avg'})
    seller = seller.rename(columns={'seller_ID': 'seller_id',
                                    'seller_name': 'name'})
    sale_offer = sale_offer.rename(columns={'date_ID': 'date_id',
                                            'card_ID': 'card_id',
                                            'seller_ID': 'seller_id'})

    return {"date": date, "card": card, "card_stats": card_stats,
            "seller": seller, "sale_offer": sale_offer}


def transform_database_data(table_content):
    return pd.DataFrame(
        table_content.get('values'), columns=table_content.get('columns'))


# Return those date_ids from the new dataset which are not in the old one.
def compare_dates(old_dates, new_dates):
    old = set([tuple(row) for row in old_dates.values])
    new = set([tuple(row) for row in new_dates.values])
    return [row[1] for row in new if new not in old]


def calculate_deltas(old_data, new_data):

    # Take the new data and load the differences into the database,
    old_data_rows = set([tuple(row) for row in old_data.values])
    new_data_rows = set([tuple(row) for row in new_data.values])
    to_be_inserted = [row for row in new_data_rows if row not in old_data_rows]
    to_be_deleted = [row for row in old_data_rows if row not in new_data_rows]

    log("Old data:")
    log(old_data_rows)
    log("New data:")
    log(new_data_rows)
    log("TBI:")
    log(to_be_inserted)
    log("TBD:")
    log(to_be_deleted)
    time.sleep(10)

    return to_be_deleted, to_be_inserted
