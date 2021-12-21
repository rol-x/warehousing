"""Scrape the card market website to get all the neccessary data."""
import os
import time as tm

import config
from services import data_service as data
from services import flags_service as flags
from services import logs_service as logs
from services import web_service as web
from services.logs_service import log, logr

# TODO: Change singular to plural in entities use, not in model
# TODO: Cast id columns of card_stats to int.
# TODO: Check out '' seller adding fiasco.


# Main function
def main():

    # Setup
    logs.setup_logs()
    data.setup_data()
    flags.setup_flags()
    data.add_date()

    # Time the program execution
    data.schedule_the_run()

    # Create run log file and connect the webdriver
    logs.setup_run_logfile()
    web.connect_webdriver()
    web.driver.implicitly_wait(1.5)

    # Transform the data from .csv to pickle format for faster changes
    data.pickle_data()

    # Clean the local pickle data (pre-acquisition)
    removed = data.prune_local_data()
    logr(f"Local pickle data validated (removed {removed} records)\n")

    # Get card names and open each card's URL
    progress = 0
    card_list = web.get_card_names(config.EXPANSION_NAME)
    for card_name in card_list:
        progress += 1
        if progress < config.START_FROM:
            continue
        logs.log_progress(card_name, progress, len(card_list))
        web.driver.implicitly_wait(0)

        # Try to load the page 3 times
        for try_num in range(3):

            # Wait for the card page to be loaded
            start = tm.time()
            if not web.load_card_page(card_name):
                logr(f"                Page loading timed out. Retrying...\n")
                web.cooldown(try_num)
                continue
            web.cooldown(-6)  # 0.87 seconds

            # Keep pressing the Load More button
            logr("                Expanding page...")
            if not web.click_load_more_button():
                logr(f"                Expanding timed out. Retrying...")
                web.cooldown(try_num)
                continue
            logr(f"                Time: {round(tm.time() - start, 3)}\n")

            # Create a soup from the website source code
            card_soup = web.get_soup()

            # If the card page is valid, save it
            if web.is_valid_card_page(card_soup):
                break

            # Otherwise try again
            logr('Card page invalid. Retrying...')
            web.cooldown(try_num)

        # Save the card if not saved already
        if not data.is_card_saved(card_name):
            data.add_card(card_soup)

        # Save the card market statistics if not saved today
        card_id = data.get_card_id(card_name)
        data.add_card_stats(card_soup, card_id)

        # Get all sellers from the card page
        logr(" = Sellers = ")
        logr("Task - Updating sellers")
        seller_set = data.get_seller_names(card_soup)
        read_sellers = len(seller_set)

        # Investigate and add only not added sellers
        web.driver.implicitly_wait(2.5)
        start = tm.time()

        # Define loop-control variables and iterate over every seller
        new_sellers = 0
        seller_df = data.load_df('seller')
        to_add = [x for x in seller_set if x not in seller_df['name'].values]
        if len(to_add) > 1:
            logr(f"{len(to_add)} new sellers to be added.")
        for seller_name in to_add:

            # Try to get seller data from page
            for try_num in range(3):
                seller_soup = web.load_seller_page(seller_name)
                web.cooldown(-4 + 3 * try_num)   # 1.97s -> 6.67s -> 22.5s
                seller_soup = web.get_soup()
                updated = data.add_seller(seller_df, seller_soup)
                if updated is not None:
                    seller_df = updated
                    new_sellers += 1
                    break
                web.cooldown(-7 + 2 * try_num)  # 0.58s -> 1.31s -> 2.96s

        # Task finished
        data.save_new_sellers(seller_df, new_sellers, read_sellers, start)

        # Get all sale offers from the page
        logr(" = Offers = ")
        logr("Task - Updating sale offers")
        data.add_offers(card_soup)

        # In case of exception and a restart, save the progress
        config.START_FROM += 1
        web.cooldown(-4)

    # Log program task completion
    logr("All cards, sellers and sale offers acquired")
    config.START_FROM = 1

    # Clean the pickle data (post-acquisition)
    removed = data.prune_local_data()
    logr(f"Pickle data validated (removed {removed} records)\n")

    # Transform the data back from pickle to .csv format
    data.unpickle_data()
    logr(" = Program execution finished = ")


# Main function
if __name__ == '__main__':
    print("Started: data-gathering")
    tm.sleep(config.CONTAINER_DELAY)
    try:
        while True:
            main()
    except Exception as exception:
        log(exception)
        if os.path.exists("./.pickles"):
            log(" - Unpickling data. Container will restart in 30 minutes.")
        else:
            log(" - Container will restart in 30 minutes.")
        data.unpickle_data()
        tm.sleep(30 * 60)
        raise SystemExit from exception
