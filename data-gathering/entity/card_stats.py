import globals as globals
from handlers.data_handler import load_df
from handlers.log_handler import log


# Extract information about card statistics from provided soup.
def add_card_stats(card_soup, card_ID):
    '''Extract information about card statistics from provided soup.'''

    # Get rows from the card information table
    card_info = card_soup.findAll("dd", {"class": "col-6 col-xl-7"})
    if len(card_info) == 0:
        log('No card info found on current page')
        return

    # Get the attributes
    avg_1_price = card_info[-1].string.string[:-2].replace(',', '.')
    avg_7_price = card_info[-2].string.string[:-2].replace(',', '.')
    avg_30_price = card_info[-3].string.string[:-2].replace(',', '.')
    price_from = card_info[-5].string[:-2].replace(',', '.')
    available_items = card_info[-6].string

    save_card_stats(card_ID, price_from, avg_30_price, avg_7_price,
                    avg_1_price, available_items)


# Save a single card statsistics to the dataframe in .csv file.
def save_card_stats(card_ID, price_from, avg_30_price, avg_7_price,
                    avg_1_price, available_items):
    '''Save a single card statsistics to the dataframe in .csv file.'''

    # Logging
    log('== Add card stats ==')
    log('Card ID:       ' + str(card_ID))
    log('Price from:    ' + str(price_from))
    log('30-day avg:    ' + str(avg_30_price))
    log('7-day avg:     ' + str(avg_7_price))
    log('1-day avg:     ' + str(avg_1_price))
    log('Amount:        ' + str(available_items))
    log('Date ID:       ' + str(globals.this_date_ID) + '\n')

    # Writing to local file
    with open('data/card_stats.csv', 'a', encoding="utf-8") as card_csv:
        card_csv.write(str(card_ID) + ';')
        card_csv.write(str(price_from) + ';')
        card_csv.write(str(avg_30_price) + ';')
        card_csv.write(str(avg_7_price) + ';')
        card_csv.write(str(avg_1_price) + ';')
        card_csv.write(str(available_items) + ';')
        card_csv.write(str(globals.this_date_ID) + '\n')


# Return whether stats given by card ID were saved that day.
def are_card_stats_saved_today(card_ID):
    '''Return whether stats given by card ID were saved that day.'''
    card_stats_df = load_df('card_stats')
    sm = card_stats_df[(card_stats_df['card_ID'] == card_ID) &
                       (card_stats_df['date_ID'] == globals.this_date_ID)]

    if len(sm) > 0:
        return True
    return False
