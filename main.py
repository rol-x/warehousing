"""Scrape the card market website to get all the neccessary data."""
import etl.globals as globals
import etl.handlers.data_handler as data_handler
from etl.entity.card import add_card, get_card_ID, is_card_saved
from etl.entity.card_stats import add_card_stats, are_card_stats_saved_today
from etl.entity.date import add_date
from etl.entity.sale_offer import add_offers
from etl.entity.seller import get_seller_names
from etl.handlers.log_handler import log, log_progress, log_url
from etl.handlers.web_handler import (add_sellers_from_set,
                                      click_load_more_button, create_soup,
                                      create_webdriver, get_card_names,
                                      is_valid_card_page, realistic_pause,
                                      restart_webdriver, urlify)

# TODO: Look into handling of wild Firefox processes.
# TODO: Change singular to plural in entities use, not in model.

# Main function
if __name__ == "__main__":

    # Setup
    data_handler.prepare_files()
    globals.this_date_ID = add_date()
    data_handler.prepare_expansion_list_file(globals.expansion_name)
    driver = create_webdriver()

    # Validate the local data (pre-acquisition)
    removed = data_handler.validate_local_data()
    log(f"Local data validated (removed {removed} records)\n")

    # Get card names and open each card's URL
    progress = 0
    card_list = get_card_names(driver, globals.expansion_name)
    for card_name in card_list:
        progress += 1
        if progress < globals.start_from:
            continue
        log_progress(card_name, progress, len(card_list))

        # Compose the card page url from the card's name
        card_url = globals.base_url + globals.expansion_name + '/'
        card_url += urlify(card_name)

        # Try to load the page 3 times
        tries = 0
        while tries < globals.max_tries:
            # Open the card page and extend the view maximally
            realistic_pause(globals.wait_coef)
            driver.get(card_url)
            log_url(driver.current_url)
            log("                Expanding page...\n")
            is_page_expanded = click_load_more_button(driver)
            if is_page_expanded:
                # Validate and save the parsed page content for later use
                card_soup = create_soup(driver.page_source)
                if is_valid_card_page(card_soup):
                    break
                else:
                    log('Card page invalid')
                    driver = restart_webdriver(driver)
                    log('Waiting and reconnecting...  (cooldown for 20.0 sec)')
                    realistic_pause(20.0)
            else:
                log('Expanding the offers list timed out')
                driver = restart_webdriver(driver)
                log('Waiting and reconnecting...  (cooldown for 20.0 sec)')
                realistic_pause(20.0)
                globals.wait_coef *= 1.1
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

    # Close the webdriver
    driver.close()
    log("Webdriver closed")

    # Validate the local data (post-acquisition)
    removed = data_handler.validate_local_data()
    log(f"Local data validated (removed {removed} records)\n")
    log(" = Program execution finished = ")
