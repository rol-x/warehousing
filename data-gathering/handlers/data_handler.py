"""Manage the local files with data stored in .csv format."""
import time

import config
import pandas as pd
import services.flags_service as flags
from services.logs_service import log, logr


# Check the time and files status to run the code once a day.
def schedule_the_run():
    '''Check the time and files status to run the code once a day.'''

    # Load the data and compare against today
    date = load_df('date')
    row_id = date.index[-1]
    today = time.strftime("%d/%m/%Y",
                          time.localtime(time.time())).split("/")

    # Run immediately if it's the first run
    if is_first_run():
        log(" - Fresh start detected. Proceeding to run.")
        return

    # Run the code always if the option is set
    if config.FORCE_UPDATE:
        log(" - Force update flag active. Proceeding to run.")
        config.FORCE_UPDATE = False
        return

    # If today's date is already saved in date.csv
    while date.loc[row_id, 'day'] == int(today[0]) and \
        date.loc[row_id, 'month'] == int(today[1]) and \
            date.loc[row_id, 'year'] == int(today[2]):

        # Log and check whether another run is needed
        log(" - Relevant data discovered. Checking for completeness.")

        # If yes, break out of the wait loop
        if not is_data_complete(date.loc[row_id, 'date_ID']):
            log("   - Gathered data is incomplete. Proceeding to run.")
            break

        # If not, save this complete dataset as validated in a checksum form
        if not flags.is_data_checksum_saved():
            log("   - Data validation completed successfully.")
            log("   - Saving checksum: " + flags.calculate_data_checksum())
            flags.save_checksum(flags.calculate_data_checksum())

        # Or note that it's already validated and continue waiting
        else:
            log("   - Dataset already validated. All needed data saved.")

        # If the data doesn't need to be gathered, wait 1 hour
        log(" - Job is done. Waiting for 1 hour.")
        time.sleep(60 * 60)

        # Reload the data after waiting
        date = load_df('date')
        row_id = date.index[-1]
        today = time.strftime("%d/%m/%Y",
                              time.localtime(time.time())).split("/")


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

    # Card. Check whether the number of saved cards is correct.
    if len(card.index) != len(card_list):
        log("The number of cards in expansion %s is incorrect"
            % config.EXPANSION_NAME)
        log(f"Expected: {len(card_list)}    got: " + str(len(card.index)))
        return False

    # Card stats. Check whether the number of saved card stats is correct.
    if len(card_stats[card_stats['date_ID'] == date_ID]) != len(card_list):
        log(f"The number of cards for date ID [{date_ID}] is incorrect")
        log(f"Expected: {len(card_list)}    got: "
            + str(len(card_stats[card_stats['date_ID'] == date_ID])))
        return False

    # Sale offer. Find any new sellers from sale_offer csv.
    before = sale_offer[sale_offer['date_ID'] < date_ID]
    sellers_today = sale_offer['seller_ID'].unique()
    sellers_before = before['seller_ID'].unique()

    # Check if there was more sellers yesterday than today.
    if len(sellers_before) > len(sellers_today):
        log("The number of sellers for date ID ["
            + date_ID + "] is incorrect")
        log(f"Expected: >= {len(sellers_before)}    "
            + f"got: {len(sellers_today)}")
        return False

    # Seller. Check if all sellers from offers are in the sellers file.
    for seller_ID in sellers_today:
        if seller_ID not in seller['seller_ID'].values:
            log("Seller from sale offer not saved in sellers")
            return False

    # TODO: Check sale_offer for outlier changes (crudely)
    daily = sale_offer.groupby('date_ID').count()
    daily.pct_change()

    return True


# Load and clean local files, returning the number of removed rows.
def clean_local_data():
    '''Load and validate local files, returning the number of removed rows.'''

    # Count the rows dropped
    rows_dropped = 0

    # Validate dates
    date = pd.DataFrame(load_df('date'))
    rows_dropped += drop_rows_with_nans(date)
    rows_dropped += drop_duplicate_rows(date)
    rows_dropped += drop_negative_index(date, 'date_ID')
    rows_dropped += drop_identical_records(date, 'date_ID')

    # Validate cards
    card = load_df('card')
    rows_dropped += drop_rows_with_nans(card)
    rows_dropped += drop_duplicate_rows(card)
    rows_dropped += drop_negative_index(card, 'card_ID')
    rows_dropped += drop_identical_records(card, 'card_ID')

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


# Save the dataframe replacing the existing file.
def save_data(df, filename):
    '''Save the dataframe replacing the existing file.'''
    df.to_pickle(f"./data/{filename}.csv", sep=';', index=False)


# Try to load a .csv file content into a dataframe.
def load_df(entity_name):
    '''Try to return a dataframe from the respective .csv file.'''
    if entity_name == 'sale_offer' and config.FILE_PART > 1:
        entity_name += f'_{config.FILE_PART}'
    try:
        df = pd.read_csv('./data/' + entity_name + '.csv', sep=';')
    except pd.errors.EmptyDataError as empty_err:
        logr(f'Please prepare the headers and data in {entity_name}.csv!\n')
        logr(str(empty_err))
        return None
    except pd.errors.ParserError as parser_err:
        logr(f'Parser error while loading {entity_name}.csv\n')
        logr(str(parser_err))
        return secure_load_df(entity_name)
    except Exception as e:
        logr(f'Exception occured while loading {entity_name}.csv\n')
        logr(str(e))
        return None
    return df


# Try to securely load a dataframe from a .csv file.
def secure_load_df(entity_name):
    '''Try to securely load a dataframe from a .csv file.'''
    try:
        df = pd.read_csv('./data' + entity_name + '.csv', sep=';',
                         error_bad_lines=False)
    except pd.errors.ParserError as parser_err:
        logr(parser_err)
        logr("Importing data from csv failed - aborting.\n")
        raise SystemExit from parser_err
    return df


# Return the number of rows of a specified dataframe.
def get_size(entity_name):
    '''Return the number of rows of a specified dataframe.'''
    entity_df = load_df(entity_name)
    return len(entity_df.index)


# Get the current date ID.
def generate_date_ID():
    '''Get the current date ID.'''

    # Load the date dataframe
    date_df = load_df('date')

    # Prepare the attributes
    now = time.time()
    date = time.strftime("%d/%m/%Y", time.localtime(now)).split("/")
    day = int(date[0])
    month = int(date[1])
    year = int(date[2])
    weekday = int(time.strftime("%w", time.localtime(now)))
    weekday = weekday if weekday > 0 else 7
    date_ID = len(date_df.index) + 1

    # Check for the same datetime record
    same_date = date_df[(date_df['day'] == int(day))
                        & (date_df['month'] == int(month))
                        & (date_df['year'] == int(year))]['date_ID']

    if(len(same_date) > 0):
        config.THIS_DATE_ID = same_date.values[0]
        log(f"Date ID [{config.THIS_DATE_ID}] already added.")
    else:
        # Save the date locally
        config.THIS_DATE_ID = date_ID
        save_date(date_ID, day, month, year, weekday)


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
    with open('./data/date.csv', 'a', encoding="utf-8") as date_csv:
        date_csv.write(str(date_ID) + ';')
        date_csv.write(str(day) + ';')
        date_csv.write(str(month) + ';')
        date_csv.write(str(year) + ';')
        date_csv.write(str(weekday) + '\n')


# Extract information about card statistics from provided soup.
def add_card_stats(card_soup, card_ID):
    '''Extract information about card statistics from provided soup.'''

    # Get rows from the card information table
    card_info = card_soup.findAll("dd", {"class": "col-6 col-xl-7"})
    if len(card_info) == 0:
        logr('No card info found on current page')
        return

    # Get the attributes
    avg_1_price = card_info[-1].string.string[:-2].replace(',', '.')
    avg_7_price = card_info[-2].string.string[:-2].replace(',', '.')
    avg_30_price = card_info[-3].string.string[:-2].replace(',', '.')
    price_from = card_info[-5].string[:-2].replace(',', '.')
    available_items = card_info[-6].string

    # Logging
    logr('== Add card stats ==')
    logr('Card ID:       ' + str(card_ID))
    logr('Price from:    ' + str(price_from))
    logr('30-day avg:    ' + str(avg_30_price))
    logr('7-day avg:     ' + str(avg_7_price))
    logr('1-day avg:     ' + str(avg_1_price))
    logr('Amount:        ' + str(available_items))
    logr('Date ID:       ' + str(config.THIS_DATE_ID) + '\n')

    # Writing to local file
    with open('./data/card_stats.csv', 'a', encoding="utf-8") as card_csv:
        card_csv.write(str(card_ID) + ';')
        card_csv.write(str(price_from) + ';')
        card_csv.write(str(avg_30_price) + ';')
        card_csv.write(str(avg_7_price) + ';')
        card_csv.write(str(avg_1_price) + ';')
        card_csv.write(str(available_items) + ';')
        card_csv.write(str(config.THIS_DATE_ID) + '\n')


# Return whether stats given by card ID were saved that day.
def are_card_stats_saved_today(card_ID):
    '''Return whether stats given by card ID were saved that day.'''
    card_stats_df = load_df('card_stats')
    sm = card_stats_df[(card_stats_df['card_ID'] == card_ID) &
                       (card_stats_df['date_ID'] == config.THIS_DATE_ID)]

    if len(sm) > 0:
        return True
    return False


# Extract information about a card from provided soup.
def add_card(card_soup):
    '''Extract information about a card from provided soup.'''

    # Load the card and date dataframes
    card_df = load_df('card')

    # Get rows from the card information table
    card_info = card_soup.findAll("dd", {"class": "col-6 col-xl-7"})
    if len(card_info) == 0:
        logr('No card info found on current page')
        return

    # Get the attributes
    card_ID = len(card_df.index) + 1
    card_name = str(card_soup.find("h1")).split('<')[1][3:]
    expansion_name = card_info[1].find('span')['data-original-title']
    rarity = card_info[0].find('span')['data-original-title']

    # Logging
    logr('== Add card ==')
    logr('Card ID:       ' + str(card_ID))
    logr('Card:          ' + str(card_name))
    logr('Rarity:        ' + str(rarity))
    logr('Expansion:     ' + str(expansion_name) + '\n')

    # Writing to the file
    with open('./data/card.csv', 'a', encoding="utf-8") as card_csv:
        card_csv.write(str(card_ID) + ';')
        card_csv.write(card_name + ';')
        card_csv.write(expansion_name + ';')
        card_csv.write(rarity + '\n')


# Return a session-valid card ID given its name.
def get_card_ID(card_name):
    '''Return a session-valid card ID given its name.'''
    card_df = load_df('card')
    this_card = card_df[(card_df['card_name'] == card_name)]

    if len(this_card) == 0:
        return -1

    return int(this_card['card_ID'].values[0])


# Return whether a card with the same name is already saved.
def is_card_saved(card_name):
    '''Return whether a card with the same name is already saved.'''
    card_df = load_df('card')
    if card_name in card_df['card_name'].values:
        return True
    return False


# Extract information about the offers from provided card soup.
def add_offers(card_page):
    '''Extract information about the offers from provided card soup.'''
    table = card_page.find("div", {"class": "table "
                                   + "article-table "
                                   + "table-striped"})
    if table is None:
        logr("No offers found on page!")
        logr(f'Page title:  {card_page.find("title")}')
        return

    # Get static and list info from the page
    card_name = (str(card_page.find("div", {"class": "flex-grow-1"}))
                 .split(">")[2]).split("<")[0]
    sellers_info = table.findAll("span", {"class": "seller-info d-flex "
                                          + "align-items-center"})
    seller_names = []
    for seller_info in sellers_info:
        seller_names.append(seller_info.find("span", {"class": "d-flex "
                                             + "has-content-centered "
                                             + "mr-1"}))

    prices = table.findAll("span", {"class": "font-weight-bold color-primary "
                                    + "small text-right text-nowrap"})
    amounts = table.findAll("span", {"class":
                            "item-count small text-right"})
    attributes = table.findAll("div", {"class": "product-attributes col"})

    # Ensure the table has proper content
    if not (len(prices) / 2) == len(amounts) \
            == len(seller_names) == len(attributes):
        logr('The columns don\'t match in size!\n')
        return

    # Acquire the data row by row
    offers_dict = {"seller_ID": [], "price": [], "card_ID": [],
                   "card_condition": [], "language": [], "is_foiled": [],
                   "amount": [], "date_ID": []}
    for i, seller_name in enumerate(seller_names):
        card_attrs = []
        price = float(str(prices[2*i].string)[:-2].replace(".", "")
                      .replace(",", "."))

        # Get card attributes
        for attr in attributes[i].findAll("span"):
            if attr is not None:
                try:
                    card_attrs.append(attr["data-original-title"])
                except KeyError:
                    continue
            is_foiled = False
            foil = attributes[i].find("span", {"class":
                                               "icon st_SpecialIcon mr-1"})
            if foil is not None:
                if foil["data-original-title"] == 'Foil':
                    is_foiled = True

        # Interpret the attributes
        if len(card_attrs) < 2:
            card_attrs = ['', '']
            logr("Incomplete card attributes!")

        # Load the entry into the dictionary
        offers_dict['seller_ID'].append(get_seller_ID(seller_name.string))
        offers_dict['price'].append(price)
        offers_dict['card_ID'].append(get_card_ID(card_name))
        offers_dict['card_condition'].append(card_attrs[0])
        offers_dict['language'].append(card_attrs[1])
        offers_dict['is_foiled'].append(is_foiled)
        offers_dict['amount'].append(int(amounts[i].string))
        offers_dict['date_ID'].append(config.THIS_DATE_ID)

        for key, value in offers_dict.items():
            if len(value) == 0:
                logr("Faulty offer set! No entrys for key: " + key)
                return

    update_sale_offers(offers_dict)


# Take the gathered data and adjoin it to the local files
def update_sale_offers(offers_dict):

    # Load and drop today's sales data for this card
    saved = load_df('sale_offer')
    scraped = pd.DataFrame(offers_dict)
    this_card_today = saved[(saved['card_ID'] == scraped['card_ID'].values[0])
                            & (saved['date_ID'] == config.THIS_DATE_ID)]
    saved.drop(this_card_today.index, inplace=True)

    # Concatenate the remaining and new offers and save to file
    data = pd.concat([saved, scraped]).reset_index(drop=True).drop_duplicates()
    filename = f'sale_offer_{config.FILE_PART}.csv' if config.FILE_PART > 1 \
        else 'sale_offer.csv'
    data.to_csv(f'./data/{filename}', ';', index=False)

    # Log task finished
    logr(f"Done - {len(data) - len(saved)} sale offers saved  (before: "
         + f"{len(this_card_today)}, total: {len(data)})\n\n")


# Extract information about a seller from provided soup.
def add_seller(seller_soup):
    '''Extract information about a seller from provided soup.'''

    # Get rows from the seller information table on page
    seller_name = seller_soup.find("h1")

    # User not loaded correctly
    if seller_name is None:
        return False

    # Seller name
    seller_name = str(seller_name.string)

    # Seller ID
    seller_df = load_df('seller')
    seller_ID = len(seller_df.index) + 1

    # Type
    s_type = seller_soup.find("span",
                              {"class":
                               "ml-2 personalInfo-bold"}).string

    # Member since
    member_since = seller_soup.find("span",
                                    {"class": "ml-1 "
                                     + "personalInfo-light "
                                     + "d-none "
                                     + "d-md-block"}).string.split(' ')[-1]

    # Country
    country = seller_soup.find("div",
                               {"class":
                                "col-12 col-md-6"}) \
        .find("span")["data-original-title"]

    # Address
    address_div = seller_soup.findAll("div", {"class": "d-flex "
                                              + "align-items-center "
                                              + "justify-content-start "
                                              + "flex-wrap "
                                              + "personalInfo "
                                              + "col-8 "
                                              + "col-md-9"})[-1] \
        .findAll("p")
    address = ''
    for line in address_div:
        address = address + line.string + ', '
    address = address.strip(', ')
    if address == country:
        address = ''

    # Logging
    logr(f"Seller added:  {seller_name} [{seller_ID}]")

    # Writing
    with open('./data/seller.csv', 'a', encoding="utf-8") as seller_csv:
        seller_csv.write(str(seller_ID) + ';')
        seller_csv.write(seller_name + ';')
        seller_csv.write(s_type + ';')
        seller_csv.write(member_since + ';')
        seller_csv.write(country + ';')
        seller_csv.write(address + '\n')

    return True


# Return a seller ID given its name.
def get_seller_ID(seller_name):
    '''Return a seller ID given its name.'''
    seller_df = load_df('seller')
    if seller_df is None:
        return -1

    this_seller = seller_df[(seller_df['seller_name'] == seller_name)]

    if len(this_seller) == 0:
        return -1

    return int(this_seller['seller_ID'].values[0])


# Return a set of all sellers found in a card page.
def get_seller_names(card_soup):
    '''Return a set of all sellers found in a card page.'''
    names = set(map(lambda x: str(x.find("a").string
                                  if x.find("a") is not None
                                  else ""),
                    card_soup.findAll("span", {"class": "d-flex "
                                      + "has-content-centered " + "mr-1"})))
    names.remove('')
    return names
