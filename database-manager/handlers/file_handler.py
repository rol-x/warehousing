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

    return {"date": date, "card": card, "card_stats": card_stats,
            "seller": seller, "sale_offer": sale_offer}


def load_database_data(db_content):
    date_db = db_content.get('date')
    card_db = db_content.get('card')
    card_stats_db = db_content.get('card_stats')
    seller_db = db_content.get('seller')
    sale_offer_db = db_content.get('sale_offer')

    date = pd.DataFrame(
        date_db.get('values'), columns=date_db.get('columns'))
    card = pd.DataFrame(
        card_db.get('values'), columns=card_db.get('columns'))
    card_stats = pd.DataFrame(
        card_stats_db.get('values'), columns=card_stats_db.get('columns'))
    seller = pd.DataFrame(
        seller_db.get('values'), columns=seller_db.get('columns'))
    sale_offer = pd.DataFrame(
        sale_offer_db.get('values'), columns=sale_offer_db.get('columns'))

    return {"date": date, "card": card, "card_stats": card_stats,
            "seller": seller, "sale_offer": sale_offer}


def calculate_deltas(old_data, new_data):

    date_d = pd.concat([old_data.get('date'), new_data.get('date')]) \
        .drop_duplicates(keep=False).to_dict(orient='index')
    card_d = pd.concat([old_data.get('date'), new_data.get('date')]) \
        .drop_duplicates(keep=False).to_dict(orient='index')
    card_stats_d = pd.concat([old_data.get('date'), new_data.get('date')]) \
        .drop_duplicates(keep=False).to_dict(orient='index')
    seller_d = pd.concat([old_data.get('date'), new_data.get('date')]) \
        .drop_duplicates(keep=False).to_dict(orient='index')
    sale_offer_d = pd.concat([old_data.get('date'), new_data.get('date')]) \
        .drop_duplicates(keep=False).to_dict(orient='index')
    return {"date": date_d, "card": card_d, "card_stats": card_stats_d,
            "seller": seller_d, "sale_offer": sale_offer_d}
