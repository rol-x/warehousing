"""Manage the local files with data stored in .csv format."""
import os
import shutil
import sys
import time as tm

import config
import pandas as pd

from services import flags_service as flags
from services.logs_service import log, logr


def load_df(entity, force_csv=False):
    if entity in ['date', 'seller'] or force_csv:
        try:
            return pd.read_csv(f'./data/{entity}.csv',
                               compression='gzip', sep=';', encoding="utf-8")
        except pd.errors.EmptyDataError as empty_err:
            logr(f'No data in {entity}.csv\n')
            logr(empty_err)
        except pd.errors.ParserError as parser_err:
            logr(f'Parser error while loading {entity}.csv\n')
            logr(parser_err)
        except Exception as exception:
            logr(f'Exception occured while loading {entity}.csv\n')
            logr(exception)
    if entity in ['card', 'card_stats', 'sale_offer']:
        try:
            return pd.read_pickle(f'./.pickles/{entity}.pkl')
        except pd.errors.EmptyDataError as empty_err:
            logr(f'No data in {entity}.pkl\n')
            logr(empty_err)
            return pd.DataFrame()
        except pd.errors.ParserError as parser_err:
            logr(f'Parser error while loading {entity}.pkl\n')
            logr(parser_err)
            return pd.DataFrame()
        except Exception as exception:
            logr(f'Exception occured while loading {entity}.pkl\n')
            logr(exception)
            return pd.DataFrame()


def save_df(df, entity):
    if entity in ['date', 'seller']:
        df.to_csv(f'./data/{entity}.csv', compression='gzip',
                  sep=';', encoding='utf-8', index=False)
    if entity in ['card', 'card_stats', 'sale_offer']:
        df.to_pickle(f'./.pickles/{entity}.pkl')


# Prepare the local data directory.
def setup_data():
    '''Prepare the local data directory.'''

    # Create data directory
    if not os.path.exists('./data'):
        os.mkdir('./data')

    # Create expansion card names list file
    with open(f'./data/{config.EXPANSION_NAME}.txt', 'a+', encoding="utf-8"):
        pass

    # Ensure the files exist and are ready for writing
    for entity in ['date', 'card', 'seller', 'card_stats', 'sale_offer']:
        if not os.path.exists(f'./data/{entity}.csv'):
            empty = pd.DataFrame({col: pd.Series(dtype=col_type)
                                  for col, col_type
                                  in config.HEADERS.get(entity).items()})
            empty.to_csv(f'./data/{entity}.csv', compression='gzip',
                         sep=';', encoding='utf-8', index=False)


# Copy the dataframes from csv to pickle format for faster I/O.
def pickle_data():
    '''Copy the dataframes from csv to pickle format for faster I/O.'''

    # Create fresh pickles directory
    try:
        shutil.rmtree('./.pickles', ignore_errors=True)
        os.rmdir('./.pickles')
        log("Removed old pickles directory.")
    except Exception:
        ...
    try:
        os.mkdir('./.pickles')
        log("Created new pickles directory.")
    except Exception:
        ...

    # Set higher recursion limit for pickling
    sys.setrecursionlimit(10000)

    # Ensure the files exist and are ready for writing
    for entity in ['card', 'card_stats', 'sale_offer']:
        df = pd.read_csv(f'./data/{entity}.csv', compression='gzip',
                         sep=';', encoding="utf-8")
        df_size = len(df.index)

        # Optimize performance by slicing only the relevant data
        if entity == 'sale_offer' and df_size:
            df = df[df['date_id'] == config.DATE_ID]
            pct = 100.0 * (df_size - len(df.index)) / df_size
            log(f"Pickling sale_offer: {df_size} rows  "
                + f"[{len(df.index)} rows selected, {round(pct, 2)}% reduced]")
        else:
            log(f"Pickling {entity}: {df_size} rows")

        # Save each to a pickle
        df.to_pickle(f'./.pickles/{entity}.pkl')


# Convert the data from pickles to csv format.
def unpickle_data():
    '''Convert the data from pickles to csv format.'''
    for entity in ['card', 'card_stats', 'sale_offer']:
        df = load_df(entity)
        log(f"Unpickling {entity}.")
        if df is not None:

            # Merge new sale offers with previous ones
            if entity == 'sale_offer':
                df = replace_sale_offers(df)

            df.to_csv(f'./data/{entity}.csv', compression='gzip',
                      sep=';', encoding='utf-8', index=False)
    try:
        shutil.rmtree('./.pickles', ignore_errors=True)
        os.rmdir('./.pickles')
        logr("Removed pickles directory.")
    except Exception:
        ...


def replace_sale_offers(new_offers):
    # Load the original csv data and make room for updated version
    old = pd.read_csv(f'./data/sale_offer.csv', compression='gzip',
                      sep=';', encoding="utf-8")
    added_card_ids = list(new_offers['card_id'].unique())
    tb_dropped = old[old['card_id'].isin(added_card_ids)
                     & (old['date_id'] == config.DATE_ID)].index

    log(f"Merging sale offers: -{len(tb_dropped)}, +{len(new_offers)} rows")
    log(f"Size before: {len(old.index)}")

    # Merge new offers with previous ones
    old = old.drop(tb_dropped)
    old = pd.concat([old, new_offers]).reset_index(drop=True)
    old["id"] = old.index + 1

    log(f"Size after: {len(old.index)}")
    return old


# Get the current date ID and save the date if necessary.
def add_date():
    '''Get the current date ID and save the date if necessary.'''

    # Load the date dataframe
    date_df = load_df('date')

    # Prepare the attributes
    now = tm.localtime()
    day, month, year = tm.strftime("%d/%m/%Y", now).split("/")
    weekday = int(tm.strftime("%w", now))
    weekday = weekday if weekday > 0 else 7

    # Check for the same datetime record
    same_date = date_df.index[(date_df['day'] == int(day))
                              & (date_df['month'] == int(month))
                              & (date_df['year'] == int(year))]

    # Today's date is already saved
    if(len(same_date) > 0):
        config.DATE_ID = date_df.loc[same_date, "id"].item()
        log(f"Date ID [{config.DATE_ID}] already added.")

    # Else, update the local data
    else:
        date_df = date_df.append({"id": len(date_df.index) + 1,
                                  "day": int(day),
                                  "month": int(month),
                                  "year": int(year),
                                  "weekday": int(weekday)}, ignore_index=True)
        date_df.to_csv("./data/date.csv", compression='gzip',
                       sep=';', encoding="utf-8", index=False)
        config.DATE_ID = list(date_df["id"])[-1]

        log('== Date ==')
        log('Day:          ' + str(day))
        log('Month:        ' + str(month))
        log('Year:         ' + str(year))
        log('Date ID:      ' + str(config.DATE_ID) + '\n')


# Check the time and files status to run the code once a day.
def schedule_the_run():
    '''Check the time and files status to run the code once a day.'''

    # Run the code always if the option is set
    if config.FORCE_UPDATE:
        log(" - Force update flag active. Proceeding to run.")
        config.FORCE_UPDATE = False
        return

    # Run immediately if it's the first run
    if is_first_run():
        log(" - Fresh start detected. Proceeding to run.")
        return

    # If today's date is already saved in date.pkl
    while True:

        # Check whether dataset checksum is already saved
        log(" - Checking for completeness of local files.")
        checksum = flags.calculate_checksum("./data")
        if checksum not in flags.get_validated_checksums():

            # If not, check whether the data is complete, and run if not
            if not is_data_complete(config.DATE_ID):
                log("   - Gathered data is incomplete. Proceeding to run.")
                break

            # If the data is complete, save the checksum to the control file
            log("   - Data validation completed successfully.")
            log("   - Saving checksum: " + checksum)
            flags.save_validated_checksum(checksum)

        else:
            log("   - Dataset already validated. All needed data saved.")

        log(" - Job is done. Waiting for 1 hour.")
        tm.sleep(60 * 60)

        # After waiting, compare the date and dates in csv file to add new one
        localtm = tm.localtime()
        date = tm.strftime("%d/%m/%Y", localtm).split('/')
        date_df = load_df('date')
        this_date = date_df[(date_df['day'] == int(date[0]))
                            & (date_df['month'] == int(date[1]))
                            & (date_df['year'] == int(date[2]))]
        if len(this_date.index) == 0:
            add_date()
            config.MAIN_LOGNAME = tm.strftime("%d%m%Y", localtm) + ".log"


# Check whether all the datasets in local files are empty
def is_first_run():
    # Load the data
    with open('./data/' + config.EXPANSION_NAME + '.txt', encoding="utf-8") \
            as exp_file:
        card_list = exp_file.readlines()
    card = load_df('card', force_csv=True)
    card_stats = load_df('card_stats', force_csv=True)
    seller = load_df('seller', force_csv=True)
    sale_offer = load_df('sale_offer', force_csv=True)

    # Return if all of the data-related files have empty dataframes inside
    if len(card.index) == 0 \
        and len(card_stats.index) == 0 \
        and len(seller.index) == 0 \
        and len(sale_offer.index) == 0 \
            and len(card_list) == 0:
        return True
    return False


# Return whether the data saved for specified date is complete.
def is_data_complete(date_id):
    '''Return whether the data saved for specified date is complete.'''

    # Load the data
    with open('./data/' + config.EXPANSION_NAME + '.txt', encoding="utf-8") \
            as exp_file:
        card_list = exp_file.readlines()
    card = load_df('card', force_csv=True)
    card_stats = load_df('card_stats', force_csv=True)
    seller = load_df('seller', force_csv=True)
    sale_offer = load_df('sale_offer', force_csv=True)

    # Check for any empty file
    if len(card_list) == 0 \
        or len(card.index) == 0 \
        or len(seller.index) == 0 \
        or len(card_stats.index) == 0 \
            or len(sale_offer.index) == 0:
        return False

    # Card. Check whether the number of saved cards is correct.
    if len(card.index) != len(card_list):
        log("The number of cards in expansion %s is incorrect"
            % config.EXPANSION_NAME)
        log(f"Expected: {len(card_list)}    got: " + str(len(card.index)))
        return False

    # Card stats. Check whether the number of saved card stats is correct.
    if len(card_stats[card_stats['date_id'] == date_id]) != len(card_list):
        log(f"The number of card stats for date ID [{date_id}] is incorrect")
        log(f"Expected: {len(card_list)}    got: "
            + str(len(card_stats[card_stats['date_id'] == date_id])))
        return False

    # Sale offer. Find any new sellers from sale_offer csv.
    before = sale_offer[sale_offer['date_id'] < date_id]
    sellers_today = sale_offer['seller_id'].unique()
    sellers_before = before['seller_id'].unique()

    # Check if there was more sellers yesterday than today.
    if len(sellers_before) > len(sellers_today):
        log("The number of sellers for date ID ["
            + date_id + "] is incorrect")
        log(f"Expected: >= {len(sellers_before)}    "
            + f"got: {len(sellers_today)}")
        return False

    # Seller. Check if all sellers from offers are in the sellers file.
    for seller_id in sellers_today:
        if seller_id not in seller['id'].values:
            log("Seller from sale offer not saved in sellers")
            return False

    # TODO: Check sale_offer for outlier changes (crudely)
    daily = sale_offer.groupby('date_id').count()
    daily.pct_change()

    return True


# Load and clean local files, returning the number of removed rows.
def prune_local_data():
    '''Load and validate local files, returning the number of removed rows.'''

    # Count the rows dropped
    rows_dropped = 0

    # TODO: Check for wrong types

    # Validate dates
    date = load_df('date')
    rows = len(date.index)
    date = drop_rows_with_nans(date)
    date = drop_duplicate_rows(date)
    rows -= len(date.index)
    rows_dropped += rows
    if rows:
        log(f"{rows} rows dropped from date")

    # Validate cards
    card = load_df('card')
    rows = len(card.index)
    card = drop_rows_with_nans(card)
    card = drop_duplicate_rows(card)
    rows -= len(card.index)
    rows_dropped += rows
    if rows:
        log(f"{rows} rows dropped from card")

    # Validate sellers
    seller = load_df('seller')
    rows = len(seller.index)
    seller = drop_duplicate_rows(seller)
    rows -= len(seller.index)
    rows_dropped += rows
    if rows:
        log(f"{rows} rows dropped from seller")

    # Validate card stats
    card_stats = load_df('card_stats')
    rows = len(card_stats.index)
    card_stats = drop_rows_with_nans(card_stats)
    card_stats = drop_duplicate_rows(card_stats)
    card_stats = drop_negative_index(card_stats, 'card_id')
    card_stats = drop_negative_index(card_stats, 'date_id')
    rows -= len(card_stats.index)
    rows_dropped += rows
    if rows:
        log(f"{rows} rows dropped from card_stats")

    # Validate sale offers
    sale_offer = load_df('sale_offer')
    rows = len(sale_offer.index)
    sale_offer = drop_rows_with_nans(sale_offer)
    sale_offer = drop_duplicate_rows(sale_offer)
    sale_offer = drop_negative_index(sale_offer, 'seller_id')
    sale_offer = drop_negative_index(sale_offer, 'card_id')
    sale_offer = drop_negative_index(sale_offer, 'date_id')
    rows -= len(sale_offer.index)
    rows_dropped += rows
    if rows:
        log(f"{rows} rows dropped from sale_offer")

    # Save the validated data
    save_df(date, 'date')
    save_df(card, 'card')
    save_df(seller, 'seller')
    save_df(card_stats, 'card_stats')
    save_df(sale_offer, 'sale_offer')

    # Return the number of rows dropped
    return rows_dropped


# Drop rows with NaNs.
def drop_rows_with_nans(df):
    '''Drop rows with NaNs.'''
    return df.dropna().reset_index(drop=True)


# Drop rows with negative indices.
def drop_negative_index(df, id_col):
    '''Drop rows with negative indices.'''
    tb_saved = df[df[id_col] > 0]
    return df.loc[tb_saved.index].reset_index(drop=True)


# Drop duplicate rows.
def drop_duplicate_rows(df):
    '''Drop duplicate rows.'''
    tb_saved = df.drop("id", axis=1).drop_duplicates()
    return df.loc[tb_saved.index].reset_index(drop=True)


# Extract information about a card from provided soup.
def add_card(card_soup):
    '''Extract information about a card from provided soup.'''

    # Load the card and date dataframes

    # Get rows from the card information table
    card_info = card_soup.findAll("dd", {"class": "col-6 col-xl-7"})
    if len(card_info) == 0:
        logr('No card info found on current page')
        return

    # Get the attributes
    card_name = str(card_soup.find("h1")).split('<')[1][3:]
    expansion_name = card_info[1].find('span')['data-original-title']
    rarity = card_info[0].find('span')['data-original-title']

    card = load_df('card')
    card = card.append({"id": len(card.index) + 1,
                        "name": card_name,
                        "expansion": expansion_name,
                        "rarity": rarity}, ignore_index=True)
    save_df(card, 'card')

    # Logging
    logr('== Added card ==')
    logr('Card ID:       ' + str(len(card.index)))
    logr('Card:          ' + str(card_name))
    logr('Rarity:        ' + str(rarity))
    logr('Expansion:     ' + str(expansion_name) + '\n')


# Extract information about a seller from provided soup.
def add_seller(seller_df, seller_soup):
    '''Extract information about a seller from provided soup.'''

    # Get rows from the seller information table on page
    seller_name = seller_soup.find("h1")

    # User not loaded correctly
    if seller_name is None:
        return None

    try:
        # Seller name
        seller_name = str(seller_name.string)

        # Type
        s_type = seller_soup \
            .find("span", {"class": "ml-2 personalInfo-bold"}).string

        # Member since
        member_since = seller_soup \
            .find("span", {"class": "ml-1 personalInfo-light "
                           + "d-none d-md-block"}).string.split(' ')[-1]

        # Country
        country = seller_soup \
            .find("div", {"class": "col-12 col-md-6"}) \
            .find("span")["data-original-title"]

        # Address
        address_div = seller_soup \
            .findAll("div", {"class": "d-flex align-items-center "
                             + "justify-content-start flex-wrap "
                             + "personalInfo col-8 col-md-9"})[-1].findAll("p")
    except Exception:
        return None

    address = ''
    for line in address_div:
        address += line.string + ', '
    address = address.strip(', ')
    if address == country:
        address = ''

    # Logging
    logr(f"Seller:  {seller_name} from {country} [{len(seller_df.index) + 1}]")

    return seller_df.append({"id": len(seller_df.index) + 1,
                             "name": seller_name,
                             "type": s_type,
                             "member_since": member_since,
                             "country": country,
                             "address": address}, ignore_index=True)


# Extract information about card statistics from provided soup.
def add_card_stats(card_soup, card_id):
    '''Extract information about card statistics from provided soup.'''

    # Get rows from the card information table
    card_info = card_soup.findAll("dd", {"class": "col-6 col-xl-7"})
    if len(card_info) == 0:
        logr('No card info found on current page')
        return

    # Get the attributes
    daily_avg = card_info[-1].string.string[:-2].replace(',', '.')
    weekly_avg = card_info[-2].string.string[:-2].replace(',', '.')
    monthly_avg = card_info[-3].string.string[:-2].replace(',', '.')
    price_from = card_info[-5].string[:-2].replace(',', '.')
    available_items = card_info[-6].string

    # Update the local file
    card_stats = load_df('card_stats')
    tb_dropped = card_stats[(card_stats['card_id'] == card_id)
                            & (card_stats['date_id'] == config.DATE_ID)].index
    card_stats = card_stats.drop(tb_dropped)
    card_stats = card_stats.append({"id": len(card_stats.index) + 1,
                                    "card_id": int(card_id),
                                    "price_from": float(price_from),
                                    "available_items": int(available_items),
                                    "daily_avg": float(daily_avg),
                                    "weekly_avg": float(weekly_avg),
                                    "monthly_avg": float(monthly_avg),
                                    "date_id": config.DATE_ID},
                                   ignore_index=True)

    save_df(card_stats, 'card_stats')

    # Logging
    logr(' = Card stats = ')
    logr('Card ID:       ' + str(card_id))
    logr('Price from:    ' + str(price_from))
    logr('Averages:      ' + f'{daily_avg}, {weekly_avg}, {monthly_avg}')
    logr('Amount:        ' + str(available_items))
    logr('Date ID:       ' + str(config.DATE_ID) + '\n')


# Extract information about the offers from provided card soup.
def add_offers(card_page):
    '''Extract information about the offers from provided card soup.'''
    table = card_page \
        .find("div", {"class": "table article-table table-striped"})
    if table is None:
        logr("No offers found on page!")
        logr(f'Page title:  {card_page.find("title")}')
        return

    # Extract information about the offers
    scraped = pd.DataFrame({col: pd.Series(dtype=col_type) for col, col_type
                            in config.HEADERS.get('sale_offer').items()})
    start = tm.time()

    # Card ID
    card_name = (str(card_page.find("div", {"class": "flex-grow-1"}))
                 .split(">")[2]).split("<")[0]
    card_id = get_card_id(card_name)

    # Seller IDs
    seller = load_df('seller')
    sellers_info = table.findAll("span", {"class": "seller-info d-flex "
                                          + "align-items-center"})
    seller_names = [info.find(
                    "span", {"class": "d-flex has-content-centered mr-1"})
                    for info in sellers_info]
    seller_ids = [get_seller_id(x.string, seller) for x in seller_names]

    # Prices
    prices = table.findAll("span", {"class": "font-weight-bold color-primary "
                                    + "small text-right text-nowrap"})
    prices = [float(str(prices[2*i].string)[:-2]
              .replace(".", "").replace(",", "."))
              for i in range(len(seller_names))]

    # Amounts
    amounts = table.findAll("span", {"class": "item-count small text-right"})
    amounts = [int(amount.string) for amount in amounts]

    # Card attributes (condition, language, foil)
    attributes = table.findAll("div", {"class": "product-attributes col"})

    # Condition and language
    spans = [x.findAll("span") for x in attributes
             if x.findAll("span") is not None]
    card_language = [str(x[1]["data-original-title"]) if x[1] is not None
                     else '' for x in spans]

    card_condition = [str(attr.find("span", {"class": "badge"}).string)
                      if attr.find("span", {"class": "badge"}) is not None
                      else '' for attr in attributes]

    # Foil
    foils = [x.find("span", {"class": "icon st_SpecialIcon mr-1"})
             ["data-original-title"] if
             x.find("span", {"class": "icon st_SpecialIcon mr-1"})
             is not None else '' for x in attributes]
    is_foiled = [False if x == '' else True for x in foils]

    # Ensure the table has proper content
    if not (len(prices) == len(amounts)
            == len(seller_ids) == len(attributes) == len(foils)):
        logr("The columns don't match in size!\n")
        return

    scraped['id'] = range(len(prices))
    scraped['seller_id'] = seller_ids
    scraped['price'] = prices
    scraped['card_id'] = card_id
    scraped['card_condition'] = card_condition
    scraped['card_language'] = card_language
    scraped['is_foiled'] = is_foiled
    scraped['amount'] = amounts
    scraped['date_id'] = config.DATE_ID

    # Load and drop today's sales data for this card
    saved = load_df('sale_offer')
    this_card_today = saved[(saved['card_id'] == scraped['card_id'].values[0])]
    saved.drop(this_card_today.index, inplace=True)

    # Concatenate the remaining and new offers and save to file
    data = pd.concat([saved, scraped])
    save_df(data, 'sale_offer')

    # Log task finished
    logr(f"Done - {len(data) - len(saved)} sale offers saved  (before: "
         + f"{len(this_card_today)}, total: {len(data)})")
    logr(f"Time: {round(tm.time() - start, 3)}\n\n")


def save_new_sellers(seller_df, new_sellers, read_sellers, start):
    save_df(seller_df, 'seller')
    logr(f"Done - {new_sellers} new sellers saved  (out of: "
         + f"{read_sellers}, total: {len(seller_df.index)})")
    logr(f"Time: {round(tm.time() - start, 3)}\n")


# Return a specific card's ID given its name.
def get_card_id(card_name):
    '''Return a specific card's ID given its name.'''
    card_df = load_df('card')
    this_card = card_df[(card_df['name'] == card_name)]

    if len(this_card) == 0:
        return -1

    return int(this_card['id'].item())


# Return whether a card with the same name is already saved.
def is_card_saved(card_name):
    '''Return whether a card with the same name is already saved.'''
    card_df = load_df('card')
    if card_name in card_df['name'].values:
        return True
    return False


# Return a seller ID given its name.
def get_seller_id(seller_name, seller_df):
    '''Return a seller ID given its name.'''
    if seller_df is None:
        return -1

    this_seller = seller_df[(seller_df['name'] == seller_name)]

    if len(this_seller) == 0:
        return -1

    return int(this_seller['id'].item())


# Return a set of all sellers found in a card page.
def get_seller_names(card_soup):
    '''Return a set of all sellers found in a card page.'''
    sellers = card_soup.findAll("span", {"class": "d-flex "
                                + "has-content-centered mr-1"})

    seller_names = {str(x.find("a").string) if x.find("a") is not None else ''
                    for x in sellers}
    seller_names.discard('')
    seller_names.discard('Name')
    return seller_names


# Return whether stats given by card ID were saved that day.
def are_card_stats_saved_today(card_id):
    '''Return whether stats given by card ID were saved that day.'''
    card_stats = load_df('card_stats')
    sm = card_stats[(card_stats['card_id'] == card_id) &
                    (card_stats['date_id'] == config.DATE_ID)]

    if len(sm) > 0:
        return True
    return False
