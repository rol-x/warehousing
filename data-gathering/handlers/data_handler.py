"""Manage the local files with data stored in .csv format."""
import os
import time
from datetime import datetime

import globals
import pandas as pd
from checksumdir import dirhash

from handlers.log_handler import log, log_daily


# Check the time and files status to run the code once a day.
def schedule_run():
    '''Check the time and files status to run the code once a day.'''
    # Load the data
    time.sleep(15)
    date = load_df('date')
    now = datetime.now()
    id = date.index[-1]

    # Wait until the newest date is different than today
    while date.loc[id, 'day'] == now.day and \
        date.loc[id, 'month'] == now.month and \
            date.loc[id, 'year'] == now.year:

        # However, check the files contents for incorrect or incomplete data
        log_daily("Data from today already gathered. Validating.")

        # If the data is not complete, break out of the wait loop
        if not is_data_validated():
            if not try_to_validate_data(date.loc[id, 'date_ID']):
                log_daily("Data invalid. Proceeding to run.")
                break

            # Data validation successful
            log_daily("Data validation complete.")
            log_daily("Saving checksum: " + generate_checksum())
            save_checksum(generate_checksum())

        # Data already validated
        else:
            log_daily("Data already validated.")

        # If the data doesn't need to be gathered, wait 1 hour
        log_daily("Waiting for 1 hour.")
        time.sleep(60 * 60)

        # Reload the data after waiting
        date = load_df('date')
        id = date.index[-1]
        now = datetime.now()


# Return whether the data saved for specified date is complete.
def try_to_validate_data(date_ID):
    '''Return whether the data saved for specified date is complete.'''
    if globals.force_update:
        return True

    # Load the data
    extension_name = 'Battlebond'
    card_list = open('./data/' + extension_name + '.txt').readlines()
    card_stats = load_df('card_stats')
    seller = load_df('seller')
    sale_offer = load_df('sale_offer')

    # Check whether the number of card stats is correct
    if len(card_stats[card_stats['date_ID'] == date_ID]) != len(card_list):
        log_daily(f"The number of cards for date ID {date_ID} is incorrect")
        log_daily(f"Expected: {len(card_list)}    got: "
                  + str(len(card_stats[card_stats['date_ID'] == date_ID])))
        return False

    # Find any new sellers from sale_offer csv
    before = sale_offer[sale_offer['date_ID'] < date_ID]
    sellers_today = sale_offer['seller_ID'].unique()
    sellers_before = before['seller_ID'].unique()

    # Check if there isn't more sellers yesterday than today
    if len(sellers_before) > len(sellers_today):
        log_daily(f"The number of sellers for date ID: {date_ID} is incorrect")
        log_daily(f"Expected: >= {len(sellers_before)}    "
                  + f"got: {len(sellers_today)}")
        return False

    # Check if all sellers from offers are in the sellers file
    for seller_ID in sellers_today:
        if seller_ID not in seller['seller_ID'].values:
            log_daily("Seller from sale offer not saved in sellers")
            return False

    # TODO: Check sale_offer for outlier changes (crudely)

    return True


# Create the checksums file for storing validated datasets.
def create_checksums_file():
    '''Create the checksums file for storing validated datasets.'''
    checksums = open('./flags/validated-checksums.sha1', 'a+')
    checksums.close()


# Return whether the data in the files has already been validated.
def is_data_validated():
    '''Return whether the data in the files has already been validated.'''
    if generate_checksum() in get_checksums():
        return True
    return False


# Get checksums of data files that has been validated
def get_checksums():
    try:
        checksums = open('./flags/validated-checksums.sha1', 'r').readlines()
    except FileNotFoundError:
        log_daily("No checksums file found.")
        checksums = []
    return checksums


# Return generated hash based on the contents of data directory
def generate_checksum():
    return str(dirhash('./data', 'sha1'))


# Save given data chceksum to an external file
def save_checksum(checksum):
    with open('./flags/validated-checksums.sha1', 'w+') as checksums_file:
        checksums_file.write(checksum)


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
    update_flag = open('./flags/update_flag', 'w')
    update_flag.write('1')
    update_flag.close()
    log_daily("Update flag set to 1")


# Update the flag about the end of the update
def reset_update_flag():
    update_flag = open('./flags/update_flag', 'w')
    update_flag.write('0')
    update_flag.close()
    log_daily("Update flag set to 0")


# Prepare the daily log file.
def prepare_daily_log_file():
    '''Prepare the daily log file.'''
    globals.daily_logname = datetime.now().strftime("%d%m%Y") + ".log"
    daily_logfile = open('logs/data-gathering/' + globals.daily_logname,
                         "a+", encoding="utf-8")

    timestamp = datetime.now().strftime("%H:%M:%S")
    daily_logfile.write("\n" + timestamp + ": Data gathering run started.\n")
    daily_logfile.close()


# Prepare the local log files for single run.
def prepare_single_log_file():
    '''Prepare the local log files for single run.'''
    globals.log_filename = datetime.now().strftime("%d%m%Y_%H%M") + ".log"
    logfile = open('logs/data-gathering/' + globals.log_filename,
                   "a+", encoding="utf-8")

    timestamp = datetime.now().strftime("%H:%M:%S")
    if os.path.getsize('logs/data-gathering/' + globals.log_filename):
        logfile.write(timestamp + " = Separate code execution = \n")
    else:
        logfile.write(timestamp + " = Creation of this file = \n")
    logfile.close()


# Get the current date ID.
def generate_date_ID():
    '''Get the current date ID.'''

    # Load the date dataframe
    date_df = load_df('date')

    # Prepare the attributes
    now = datetime.now()
    date = now.strftime("%d/%m/%Y").split("/")
    day = int(date[0])
    month = int(date[1])
    year = int(date[2])
    weekday = now.weekday() + 1
    date_ID = len(date_df.index) + 1

    # Check for the same datetime record
    same_date = date_df[(date_df['day'] == int(day))
                        & (date_df['month'] == int(month))
                        & (date_df['year'] == int(year))]['date_ID']

    if(len(same_date) > 0):
        globals.this_date_ID = same_date.values[0]
    else:
        # Save the date locally
        globals.this_date_ID = date_ID
        save_date(date_ID, day, month, year, weekday)


# Prepare the local data and log files.
def prepare_files():
    '''Prepare the local data and log files.'''
    # Logs
    if not os.path.exists('logs'):
        os.mkdir('logs')
    if not os.path.exists('logs/data-gathering'):
        os.mkdir('logs/data-gathering')

    prepare_daily_log_file()
    prepare_single_log_file()

    # Data
    if not os.path.exists('data'):
        os.mkdir('data')

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

    # Set global date ID and new date if needed
    generate_date_ID()

    # Flags
    if not os.path.exists('flags'):
        os.mkdir('flags')

    # Create a file for storing checksums of validated datasets
    create_checksums_file()

    # Create a file for storing the update flag with initial value 0
    reset_update_flag()


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


# Save a single date to the date dataframe in .csv file.
def save_date(date_ID, day, month, year, weekday):
    '''Save a single card date to the date dataframe in .csv file.'''

    # Logging
    log('== Add date ==')
    log('Day:           ' + str(day))
    log('Month:         ' + str(month))
    log('Year:          ' + str(year))
    log('Date ID:       ' + str(date_ID) + '\n')

    # Writing
    with open('data/date.csv', 'a', encoding="utf-8") as date_csv:
        date_csv.write(str(date_ID) + ';')
        date_csv.write(str(day) + ';')
        date_csv.write(str(month) + ';')
        date_csv.write(str(year) + ';')
        date_csv.write(str(weekday) + '\n')
