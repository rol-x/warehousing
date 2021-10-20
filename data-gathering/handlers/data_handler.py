"""Manage the local files with data stored in .csv format."""
import os
import time
from datetime import datetime

import config
import pandas as pd
from checksumdir import dirhash

from handlers.log_handler import log, log_daily


# Check the time and files status to run the code once a day.
def schedule_run():
    '''Check the time and files status to run the code once a day.'''

    # Load the data and compare against today
    date = load_df('date')
    now = datetime.now()
    row_id = date.index[-1]

    # Run immediately if it's the first run
    if is_first_run():
        log_daily(" - Fresh start detected. Proceeding to run.")
        return

    # Run the code always if the option is set
    if config.FORCE_UPDATE:
        log_daily(" - Force update flag active. Proceeding to run.")
        config.FORCE_UPDATE = False
        return

    # If such record for today already exists
    while date.loc[row_id, 'day'] == now.day and \
        date.loc[row_id, 'month'] == now.month and \
            date.loc[row_id, 'year'] == now.year:

        # Log and check whether another run is needed
        log_daily(" - Relevant data discovered. Checking for completeness.")

        # If yes, break out of the wait loop
        if not is_data_complete(date.loc[row_id, 'date_ID']):
            log_daily("   - Gathered data is incomplete. Proceeding to run.")
            break

        # Save this complete dataset as validated in checksum form
        if not is_data_checksum_saved():
            log_daily("   - Data validation completed successfully.")
            log_daily("   - Saving checksum: " + calculate_data_checksum())
            save_checksum(calculate_data_checksum())
        # Or note that it's already validated and continue waiting
        else:
            log_daily("   - Dataset already validated. All needed data saved.")

        # If the data doesn't need to be gathered, wait 1 hour
        log_daily(" - Job is done. Waiting for 1 hour.")
        time.sleep(60 * 60)

        # Reload the data after waiting
        date = load_df('date')
        row_id = date.index[-1]
        now = datetime.now()


# Check whether all the datasets in local files are empty
def is_first_run():
    # Load the data
    with open('./data/' + config.EXPANSION_NAME + '.txt', encoding="utf-8") \
            as exp_file:
        card_list = exp_file.readlines()
    card = load_df('card')
    card_stats = load_df('card_stats')
    seller = load_df('seller')
    sale_offer = load_df('sale_offer')

    # Return if all of the data-related files have empty dataframes inside
    if len(card.index) == 0 \
        and len(card_stats.index) == 0 \
        and len(seller.index) == 0 \
        and len(sale_offer.index) == 0 \
            and len(card_list) == 0:
        return True
    return False


# Return whether the data saved for specified date is complete.
def is_data_complete(date_ID):
    '''Return whether the data saved for specified date is complete.'''

    # Load the data
    with open('./data/' + config.EXPANSION_NAME + '.txt', encoding="utf-8") \
            as exp_file:
        card_list = exp_file.readlines()
    card = load_df('card')
    card_stats = load_df('card_stats')
    seller = load_df('seller')
    sale_offer = load_df('sale_offer')

    # Check for any empty file
    if len(card.index) == 0 \
        or len(card_stats.index) == 0 \
        or len(seller.index) == 0 \
        or len(sale_offer.index) == 0 \
            or len(card_list) == 0:
        return False

    # TODO: Add faulty data.csv file exceptions

    # Check whether the number of card stats is correct
    if len(card_stats[card_stats['date_ID'] == date_ID]) != len(card_list):
        log_daily(f"The number of cards for date ID [{date_ID}] is incorrect")
        log_daily(f"Expected: {len(card_list)}    got: "
                  + str(len(card_stats[card_stats['date_ID'] == date_ID])))
        return False

    # Find any new sellers from sale_offer csv
    before = sale_offer[sale_offer['date_ID'] < date_ID]
    sellers_today = sale_offer['seller_ID'].unique()
    sellers_before = before['seller_ID'].unique()

    # Check if there isn't more sellers yesterday than today
    if len(sellers_before) > len(sellers_today):
        log_daily("The number of sellers for date ID ["
                  + date_ID + "] is incorrect")
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
    with open('./flags/validated-checksums.sha1', 'a+', encoding="utf-8"):
        pass


# Return whether the data in the files has already been validated.
def is_data_checksum_saved():
    '''Return whether the data in the files has already been validated.'''
    if calculate_data_checksum() in get_validated_checksums():
        return True
    return False


# Get checksums of data files that has been validated
def get_validated_checksums():
    with open('./flags/validated-checksums.sha1', 'r',
              encoding="utf-8") as checksum_file:
        checksums = [line.strip('\n') for line in checksum_file.readlines()]
    return checksums


# Return calculated checksum based on the contents of data directory
def calculate_data_checksum():
    return str(dirhash('./data', 'sha1'))


# Save given data chceksum to an external file
def save_checksum(checksum):
    with open('./flags/validated-checksums.sha1', 'a+',
              encoding="utf-8") as checksums_file:
        checksums_file.write(checksum + "\n")


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
        len(df.drop(id_col, axis=1).drop_duplicates().index)
    if tb_dropped > 0:
        tb_saved = df.drop(id_col, axis=1).drop_duplicates()
        tb_removed = pd.concat(df.drop(id_col, axis=1), tb_saved) \
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
    if filename == 'sale_offer' and config.FILE_PART > 1:
        filename += f'_{config.FILE_PART}'
    df.to_csv(f"data/{filename}.csv", sep=';', index=False)


# Try to load a .csv file content into a dataframe.
def load_df(entity_name):
    '''Try to return a dataframe from the respective .csv file.'''
    if entity_name == 'sale_offer' and config.FILE_PART > 1:
        entity_name += f'_{config.FILE_PART}'
    try:
        df = pd.read_csv('./data/' + entity_name + '.csv', sep=';')
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
        raise SystemExit from parser_err
    return df


# Return the number of rows of a specified dataframe.
def get_size(entity_name):
    '''Return the number of rows of a specified dataframe.'''
    entity_df = load_df(entity_name)
    return len(entity_df.index)


# Prepare the daily log file.
def prepare_daily_log_file():
    '''Prepare the daily log file.'''
    config.DAILY_LOGNAME = datetime.now().strftime("%d%m%Y") + ".log"
    with open('./logs/data-gathering/' + config.DAILY_LOGNAME,
              "a+", encoding="utf-8") as daily_logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        daily_logfile.write("\n" + timestamp
                            + ": Service data-gathering is running.\n")


# Prepare the local log files for single run.
def prepare_single_log_file():
    '''Prepare the local log files for single run.'''
    config.LOG_FILENAME = datetime.now().strftime("%d%m%Y_%H%M") + ".log"
    with open('./logs/data-gathering/' + config.LOG_FILENAME,
              "a+", encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        if os.path.getsize('./logs/data-gathering/' + config.LOG_FILENAME):
            logfile.write(timestamp + ": = Separate code execution = \n")
        else:
            logfile.write(timestamp + ": = Creation of this file = \n")


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
        config.THIS_DATE_ID = same_date.values[0]
        log_daily(f"Date ID [{config.THIS_DATE_ID}] already added.")
    else:
        # Save the date locally
        config.THIS_DATE_ID = date_ID
        save_date(date_ID, day, month, year, weekday)


# Prepare the local data and log files.
def prepare_files():
    '''Prepare the local data and log files.'''
    # Create main logs directory
    if not os.path.exists('logs'):
        os.mkdir('logs')

    # Create service logs directory
    if not os.path.exists('./logs/data-gathering'):
        os.mkdir('./logs/data-gathering')

    # Prepare a daily log file
    prepare_daily_log_file()

    # Create data directory
    if not os.path.exists('data'):
        os.mkdir('data')

    # Create sellers file
    with open('./data/seller.csv', 'a+', encoding="utf-8") as seller_csv:
        if not os.path.getsize('./data/seller.csv'):
            seller_csv.write('seller_ID;seller_name;seller_type'
                             + ';member_since;country;address\n')

    # Create cards file
    with open('./data/card.csv', 'a+', encoding="utf-8") as card_csv:
        if not os.path.getsize('./data/card.csv'):
            card_csv.write('card_ID;card_name;expansion_name;rarity\n')

    # Create card stats file
    with open('./data/card_stats.csv', 'a+',
              encoding="utf-8") as card_stats_csv:
        if not os.path.getsize('./data/card_stats.csv'):
            card_stats_csv.write('card_ID;price_from;30_avg_price;7_avg_price;'
                                 + '1_avg_price;available_items;date_ID\n')

    # Create date file
    with open('./data/date.csv', 'a+', encoding="utf-8") as date_csv:
        if not os.path.getsize('./data/date.csv'):
            date_csv.write('date_ID;day;month;year;day_of_week\n')

    # Create sale offers file
    filename = determine_offers_file()
    with open(f'./data/{filename}', 'a+', encoding="utf-8") as sale_offer_csv:
        if not os.path.getsize(f'./data/{filename}'):
            sale_offer_csv.write('seller_ID;price;card_ID;card_condition;'
                                 + 'language;is_foiled;amount;date_ID\n')

    # Create expansion card names list file
    with open(f'./data/{config.EXPANSION_NAME}.txt', 'a+', encoding="utf-8"):
        pass

    # Set global date ID and new date if needed
    generate_date_ID()

    # Create flags directory
    if not os.path.exists('flags'):
        os.mkdir('flags')

    # Create a file for storing checksums of validated datasets
    create_checksums_file()

    # Create a file for storing the update flag with initial value 0
    with open('./flags/update-flag', 'a+', encoding="utf-8"):
        pass


# Scan local files to chose the file part for sale offers.
def determine_offers_file():
    '''Scan local files to chose the file part for sale offers.'''
    filename = 'sale_offer.csv' if config.FILE_PART == 1 \
        else f'sale_offer_{config.FILE_PART}.csv'
    if not os.path.isfile(f'./data/{filename}'):
        return filename
    if os.path.getsize(f'./data/{filename}') < 40000000.0:
        return filename
    config.FILE_PART += 1
    return determine_offers_file()


# Prepare the expansion cards list file.
def prepare_expansion_list_file(exp_filename):
    '''Prepare the expansion cards list file.'''
    with open('./data/' + exp_filename + '.txt', 'a+', encoding="utf-8"):
        pass


# Save a single date to the date dataframe in .csv file.
def save_date(date_ID, day, month, year, weekday):
    '''Save a single card date to the date dataframe in .csv file.'''

    # Logging
    log_daily('== Add date ==')
    log_daily('Day:           ' + str(day))
    log_daily('Month:         ' + str(month))
    log_daily('Year:          ' + str(year))
    log_daily('Date ID:       ' + str(date_ID) + '\n')

    # Writing
    with open('./data/date.csv', 'a', encoding="utf-8") as date_csv:
        date_csv.write(str(date_ID) + ';')
        date_csv.write(str(day) + ';')
        date_csv.write(str(month) + ';')
        date_csv.write(str(year) + ';')
        date_csv.write(str(weekday) + '\n')
