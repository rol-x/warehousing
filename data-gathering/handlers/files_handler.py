import os
from datetime import datetime

import config


# Prepare the local data and log files.
def prepare_files():
    '''Prepare the local data and log files.'''
    # Create main logs directory
    if not os.path.exists('./logs'):
        os.mkdir('./logs')

    # Create service logs directory
    if not os.path.exists(f'./logs/{config.NAME}'):
        os.mkdir(f'./logs/{config.NAME}')

    # Prepare a main log file
    prepare_main_log_file()

    # Create data directory
    if not os.path.exists('./data'):
        os.mkdir('./data')

    # Create sellers file
    with open('./data/seller.csv', 'a+', encoding="utf-8") as seller_csv:
        if not os.path.getsize('./data/seller.csv'):
            seller_csv.write('id;name;type;member_since;country;address\n')

    # Create cards file
    with open('./data/card.csv', 'a+', encoding="utf-8") as card_csv:
        if not os.path.getsize('./data/card.csv'):
            card_csv.write('id;name;expansion_name;rarity\n')

    # Create card stats file
    with open('./data/card_stats.csv', 'a+',
              encoding="utf-8") as card_stats_csv:
        if not os.path.getsize('./data/card_stats.csv'):
            card_stats_csv.write('card_id;price_from;monthly_avg;weekly_avg;'
                                 + 'daily_avg;available_items;date_id\n')

    # Create date file
    with open('./data/date.csv', 'a+', encoding="utf-8") as date_csv:
        if not os.path.getsize('./data/date.csv'):
            date_csv.write('id;day;month;year;weekday\n')

    # Create sale offers file
    with open('./data/sale_offer.csv', 'a+', encoding="utf-8") as so_csv:
        if not os.path.getsize('./data/sale_offer.csv'):
            so_csv.write('seller_id;price;card_id;card_condition;'
                         + 'language;is_foiled;amount;date_id\n')

    # Create expansion card names list file
    with open(f'./data/{config.EXPANSION_NAME}.txt', 'a+', encoding="utf-8"):
        pass


# Prepare the expansion cards list file.
def prepare_expansion_list_file(exp_filename):
    '''Prepare the expansion cards list file.'''
    with open('./data/' + exp_filename + '.txt', 'a+', encoding="utf-8"):
        pass


# Prepare the main log file.
def prepare_main_log_file():
    '''Prepare the main log file.'''
    config.MAIN_LOGNAME = datetime.now().strftime("%d%m%Y") + ".log"
    with open(f'./logs/{config.NAME}/{config.MAIN_LOGNAME}',
              "a+", encoding="utf-8") as main_logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        main_logfile.write("\n" + timestamp
                           + f": Service {config.NAME} is running.\n")


# Prepare the local log files for single run.
def prepare_single_log_file():
    '''Prepare the local log files for single run.'''
    config.RUN_LOGNAME = datetime.now().strftime("%d%m%Y_%H%M") + ".log"
    with open(f'./logs/{config.NAME}/{config.RUN_LOGNAME}',
              "a+", encoding="utf-8") as logfile:
        timestamp = datetime.now().strftime("%H:%M:%S")
        if os.path.getsize(f'./logs/{config.NAME}/{config.RUN_LOGNAME}'):
            logfile.write(timestamp + ": = Separate code execution = \n")
        else:
            logfile.write(timestamp + ": = Creation of this file = \n")
