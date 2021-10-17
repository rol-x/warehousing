"""Manage the local files with data stored in .csv format."""
import os
import time
from datetime import datetime

import globals
import pandas as pd

from handlers.log_handler import log


# Check the time and files status to run the code once a day.
def schedule_run():
    '''Check the time and files status to run the code once a day.'''
    time.sleep(20)
    date = load_df('date')
    now = datetime.now()
    last_date = date[date['date_ID'] == date['date_ID'].max()] \
        .index[0]
    while last_date['day'] == now.day and \
        last_date['month'] == now.month and \
            last_date['year'] == now.year:
        time.sleep(60 * 30)
        now = datetime.now()

    # get newest date_id, check how much data is saved under it, run the code
    # if date_id.date < now.date : run the first run of the code
    # if date_id.date == now.date : check data validity and completeness
    # if the data is not valid in the database : run the code


# Load and validate local files, returning the number of removed rows.
def validate_local_data():
    '''Load and validate local files, returning the number of removed rows.'''

    # Count the rows dropped
    rows_dropped = 0

    # Validate dates
    date = pd.DataFrame(load_df('date'))
    rows_dropped += drop_rows_with_nans(date)
    rows_dropped += drop_duplicate_rows(date)
    rows_dropped += drop_negative_index(date, 'date_ID')
    rows_dropped += drop_identical_records(date, 'date_ID')
    reset_id(date, 'date_ID')

    # Validate cards
    card = load_df('card')
    rows_dropped += drop_rows_with_nans(card)
    rows_dropped += drop_duplicate_rows(card)
    rows_dropped += drop_negative_index(card, 'card_ID')
    rows_dropped += drop_identical_records(card, 'card_ID')
    reset_id(card, 'card_ID')

    # Validate card stats
    card_stats = load_df('card_stats')
    rows_dropped += drop_rows_with_nans(card_stats)
    rows_dropped += drop_duplicate_rows(card_stats)
    rows_dropped += drop_negative_index(card_stats, 'card_ID')
    rows_dropped += drop_negative_index(card_stats, 'date_ID')

    # Validate sellers
    seller = load_df('seller')
    rows_dropped += drop_duplicate_rows(seller)
    rows_dropped += drop_identical_records(seller, 'seller_ID')
    rows_dropped += drop_negative_index(seller, 'seller_ID')
    reset_id(seller, 'seller_ID')

    # Validate sale offers
    sale_offer = load_df('sale_offer')
    rows_dropped += drop_rows_with_nans(sale_offer)
    rows_dropped += drop_duplicate_rows(sale_offer)
    rows_dropped += drop_negative_index(sale_offer, 'seller_ID')
    rows_dropped += drop_negative_index(sale_offer, 'card_ID')
    rows_dropped += drop_negative_index(sale_offer, 'date_ID')

    # Save the validated data
    save_data(date, 'date')
    save_data(card, 'card')
    save_data(card_stats, 'card_stats')
    save_data(seller, 'seller')
    save_data(sale_offer, 'sale_offer')

    # Return the number of rows dropped
    return rows_dropped


# Drop rows with NaNs.
def drop_rows_with_nans(df):
    '''Drop rows with NaNs.'''
    tb_dropped = len(df.index) - len(df.dropna().index)
    if tb_dropped > 0:
        df.dropna(inplace=True)
        return tb_dropped
    return 0


# Drop rows with negative indices.
def drop_negative_index(df, id_col):
    '''Drop rows with negative indices.'''
    tb_dropped = len(df.index) - len(df[df[id_col] > 0].index)
    if tb_dropped > 0:
        tbd_index = df[df[id_col] < 1].index
        df.drop(tbd_index, inplace=True)
        df.reset_index(drop=True)
        return tb_dropped
    return 0


# Drop duplicate rows.
def drop_duplicate_rows(df):
    '''Drop duplicate rows.'''
    tb_dropped = len(df.index) - len(df.drop_duplicates().index)
    if tb_dropped > 0:
        df.drop_duplicates(inplace=True)
        return tb_dropped
    return 0


# Drop logically identical records (same data).
def drop_identical_records(df, id_col):
    '''Drop logically identical records (same data).'''
    tb_dropped = len(df.index) - \
        len(df.drop(id_col, 1).drop_duplicates().index)
    if tb_dropped > 0:
        tb_saved = df.drop(id_col, 1).drop_duplicates()
        tb_removed = pd.concat(df.drop(id_col, 1), tb_saved) \
            .drop_duplicates(keep=None).index
        df.drop(tb_removed, inplace=True)
        return tb_dropped
    return 0


# Sort the data by ID and reset the date index.
def reset_id(df, id_col):
    '''Sort the data by ID and reset the date index.'''
    df.sort_values(by=id_col, ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df[id_col] = list(map(lambda x: x + 1, df.index))


# Save the dataframe replacing the existing file.
def save_data(df, filename):
    '''Save the dataframe replacing the existing file.'''
    if filename == 'sale_offer' and globals.file_part > 1:
        filename += f'_{globals.file_part}'
    df.to_csv(f"data/{filename}.csv", sep=';', index=False)


# Try to load a .csv file content into a dataframe.
def load_df(entity_name):
    '''Try to return a dataframe from the respective .csv file.'''
    if entity_name == 'sale_offer' and globals.file_part > 1:
        entity_name += f'_{globals.file_part}'
    try:
        df = pd.read_csv('data/' + entity_name + '.csv', sep=';')
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
        df = pd.read_csv('data' + entity_name + '.csv', sep=';',
                         error_bad_lines=False)
    except pd.errors.ParserError as parser_err:
        log(parser_err)
        log("Importing data from csv failed - aborting.\n")
        reset_update_flag()
        raise SystemExit
    return df


# Return the number of rows of a specified dataframe.
def get_size(entity_name):
    '''Return the number of rows of a specified dataframe.'''
    entity_df = load_df(entity_name)
    return len(entity_df.index)


# Set up the file with information about ongoing update.
def set_update_flag():
    '''Set up the file with information about ongoing update.'''
    update_flag = open('data/update_flag', 'w')
    update_flag.write('1')
    update_flag.close()


# Update the flag about the end of the update
def reset_update_flag():
    update_flag = open('data/update_flag', 'w')
    update_flag.write('0')
    update_flag.close()


# Prepare .csv files for storing the scraped data locally
def prepare_files():
    '''Prepare .csv files for storing the scraped data locally.'''
    if not os.path.exists('data'):
        os.mkdir('data')

    set_update_flag()

    seller_csv = open('data/seller.csv', 'a+', encoding="utf-8")
    if not os.path.getsize('data/seller.csv'):
        seller_csv.write('seller_ID;seller_name;seller_type'
                         + ';member_since;country;address\n')
    seller_csv.close()

    card_csv = open('data/card.csv', 'a+', encoding="utf-8")
    if not os.path.getsize('data/card.csv'):
        card_csv.write('card_ID;card_name;expansion_name;rarity\n')
    card_csv.close()

    card_stats_csv = open('data/card_stats.csv', 'a+', encoding="utf-8")
    if not os.path.getsize('data/card_stats.csv'):
        card_stats_csv.write('card_ID;price_from;30_avg_price;7_avg_price;'
                             + '1_avg_price;available_items;date_ID\n')
    card_stats_csv.close()

    date_csv = open('data/date.csv', 'a+', encoding="utf-8")
    if not os.path.getsize('data/date.csv'):
        date_csv.write('date_ID;day;month;year;day_of_week\n')
    date_csv.close()

    filename = determine_offers_file()
    sale_offer_csv = open(f'data/{filename}', 'a+', encoding="utf-8")
    if not os.path.getsize(f'data/{filename}'):
        sale_offer_csv.write('seller_ID;price;card_ID;card_condition;'
                             + 'language;is_foiled;amount;date_ID\n')
    sale_offer_csv.close()


# Scan local files to chose the file part for sale offers.
def determine_offers_file():
    '''Scan local files to chose the file part for sale offers.'''
    filename = 'sale_offer{suffix}.csv' \
        .format(suffix=f"_{globals.file_part}"
                if globals.file_part > 1 else "")
    if not os.path.isfile(f'data/{filename}'):
        return filename
    if os.path.getsize(f'data/{filename}') < 40000000.0:
        return filename
    globals.file_part += 1
    return determine_offers_file()


# Prepare the expansion cards list file.
def prepare_expansion_list_file(exp_filename):
    '''Prepare the expansion cards list file.'''
    exp_file = open('data/' + exp_filename + '.txt', 'a+', encoding="utf-8")
    exp_file.close()
