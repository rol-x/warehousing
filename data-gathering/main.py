"""Scrape the card market website to get all the neccessary data."""
import time

import config
from entity.card import add_card, get_card_ID, is_card_saved
from entity.card_stats import add_card_stats, are_card_stats_saved_today
from entity.sale_offer import add_offers
from entity.seller import get_seller_names
from handlers import data_handler
from services.logs_service import log, log_daily, log_progress, log_url
from handlers.web_handler import (add_sellers_from_set, click_load_more_button,
                                  connect_webdriver, create_soup,
                                  get_card_names, is_valid_card_page,
                                  realistic_pause, reconnect, urlify)

# TODO: Change singular to plural in entities use, not in model
# TODO: Research refreshing connection to standalone webdriver
# TODO: Research waiting for elements in Selenium framework
# TODO: Distill packages from functions in handlers and entity folders


# Main function
def main():
    # Setup
    data_handler.prepare_files()
    data_handler.schedule_the_run()
    data_handler.prepare_single_log_file()
    data_handler.prepare_expansion_list_file(config.EXPANSION_NAME)
    driver = connect_webdriver()

    # Validate the local data (pre-acquisition)
    removed = data_handler.validate_local_data()
    log(f"Local data validated (removed {removed} records)\n")

    # Get card names and open each card's URL
    progress = 0
    card_list = get_card_names(driver, config.EXPANSION_NAME)
    for card_name in card_list:
        progress += 1
        if progress < config.START_FROM:
            continue
        log_progress(card_name, progress, len(card_list))

        # Compose the card page url from the card's name
        card_url = config.BASE_URL + config.EXPANSION_NAME + '/'
        card_url += urlify(card_name)

        # TODO: Selenium wait here
        # Try to load the page 3 times
        tries = 0
        while tries < config.MAX_TRIES:
            # Open the card page and extend the view maximally
            realistic_pause(config.WAIT_COEF)
            driver.get(card_url)
            log_url(driver.current_url)
            log("                Expanding page...\n")
            is_page_expanded = click_load_more_button(driver)
            if is_page_expanded:
                # Validate and save the parsed page content for later use
                card_soup = create_soup(driver.page_source)
                if is_valid_card_page(card_soup):
                    break
                log('Card page invalid')
                driver = reconnect(driver)
                log('Waiting and reconnecting...  (15 sec cooldown)')
                realistic_pause(15.0)
            else:
                log('Expanding the offers list timed out')
                driver = reconnect(driver)
                log('Waiting and reconnecting...  (15 sec cooldown)')
                realistic_pause(15.0)
                config.WAIT_COEF *= 1.1
            tries += 1

        # Save the card if not saved already
        if not is_card_saved(card_name):
            add_card(card_soup)

        # Save the card market statistics if not saved today
        card_ID = get_card_ID(card_name)
        if not are_card_stats_saved_today(card_ID):
            add_card_stats(card_soup, card_ID)
        else:
            log(' = Card stats = ')
            log(f"Card ID:  {card_ID}")
            log('Already saved today\n')

        # Get all sellers from the card page
        log(" = Sellers = ")
        log("Task - Updating sellers")
        sellers = get_seller_names(card_soup)

        # Investigate and add only not added sellers
        add_sellers_from_set(driver, sellers)

        # Get all sale offers from the page
        log(" = Offers = ")
        log("Task - Updating sale offers")
        add_offers(card_soup)

    # Log program task completion
    log("All cards, sellers and sale offers acquired")

    # Validate the local data (post-acquisition)
    removed = data_handler.validate_local_data()
    log(f"Local data validated (removed {removed} records)\n")
    log(" = Program execution finished = ")


# Main function
if __name__ == '__main__':
    print("Started: data-gathering")
    time.sleep(config.CONTAINER_DELAY)
    try:
        while True:
            main()
    except Exception as exception:
        log_daily(exception)
        raise SystemExit from exception
