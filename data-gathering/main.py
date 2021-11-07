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
# TODO: Convert sale_offer mid-way for faster pickling?
# TODO: Look into seller adding and page expanding.
# TODO: Cast id columns of card_stats to int.
# TODO: Check out empty seller name fiasco.


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
    driver = web.connect_webdriver()
    driver.implicitly_wait(0.5)

    # Transform the data from .csv to pickle format for faster changes
    data.pickle_data()

    # Clean the local pickle data (pre-acquisition)
    removed = data.clean_pickles()
    logr(f"Local pickle data validated (removed {removed} records)\n")

    # Get card names and open each card's URL
    progress = 0
    card_list = web.get_card_names(driver, config.EXPANSION_NAME)
    for card_name in card_list:
        progress += 1
        if progress < config.START_FROM:
            continue
        logs.log_progress(card_name, progress, len(card_list))

        # Compose the card page url from the card's name
        card_url = config.BASE_URL + config.EXPANSION_NAME + '/' \
            + web.urlify(card_name)

        # Try to load the page 3 times
        for try_num in range(3):

            # Open the card page and extend the view maximally
            driver.get(card_url)
            logs.log_url(driver.current_url)
            logr("                Expanding page...\n")

            # If clicking the load more button returned False, wait and repeat
            if not web.click_load_more_button(driver):
                logr(f"Expanding the page timed out. Waiting to cool down.")
                driver.implicitly_wait(1.5)
                tm.sleep(30 * 2 ** try_num)
                continue

            # Create a soup from the website source code
            card_soup = web.create_soup(driver.page_source)

            # If the card page is valid, save it
            if web.is_valid_card_page(card_soup):
                break

            # Otherwise try again
            logr('Card page invalid. Retrying...')
            tm.sleep(3 ** (try_num + 1))

        # Save the card if not saved already
        if not data.is_card_saved(card_name):
            data.add_card(card_soup)

        # Save the card market statistics if not saved today
        card_id = data.get_card_id(card_name)
        if not data.are_card_stats_saved_today(card_id) or config.FORCE_UPDATE:
            data.add_card_stats(card_soup, card_id)
        else:
            logr(' = Card stats = ')
            logr(f"Card ID:  {card_id}")
            logr('Already saved today\n')

        # Get all sellers from the card page
        logr(" = Sellers = ")
        logr("Task - Updating sellers")
        sellers = data.get_seller_names(card_soup)

        # Investigate and add only not added sellers
        web.iterate_over_sellers(driver, sellers)

        # Get all sale offers from the page
        logr(" = Offers = ")
        logr("Task - Updating sale offers")
        data.add_offers(card_soup)
        tm.sleep(0.5)

    # Log program task completion
    logr("All cards, sellers and sale offers acquired")

    # Clean the pickle data (post-acquisition)
    removed = data.clean_pickles()
    logr(f"Pickle data validated (removed {removed} records)\n")

    # TODO: Fix error 16: resource busy
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
