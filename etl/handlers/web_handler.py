import os
from random import normalvariate, random
from time import sleep, time

import etl.globals as globals
from bs4 import BeautifulSoup
from etl.entity.seller import add_seller
from etl.handlers.data_handler import load_df
from etl.handlers.log_handler import log, log_url
from selenium import common, webdriver
from selenium.webdriver.firefox.options import Options
from urllib3.exceptions import MaxRetryError


# Return the Firefox webdriver in headless mode.
def create_webdriver():
    '''Return the Firefox webdriver in headless mode.'''
    options = Options()
    options.headless = True
    try:
        driver = webdriver.Remote("http://" + globals.webdriver_hostname
                                + ":4444/wd/hub", options=options)
        log('Webdriver ready\n')
        return driver
    except MaxRetryError as exception:
        log(exception)
        log('The connection to remote webdriver failed. Check if the container is running.\n')
        log('If it is, check the hostname in etl/globals.py and other configuration settings.\n')
        os.system.exit('Webdriver connection failed - aborting.')


# Return the Firefox webdriver in headless mode.
def restart_webdriver(driver):
    '''Return the Firefox webdriver in headless mode.'''
    log('Restarting the webdriver')
    driver.close()
    realistic_pause(2*globals.wait_coef)
    driver = create_webdriver()
    return driver


# Use the BeautifulSoup module to parse the page content into soup.
def create_soup(page_source):
    '''Use the BeautifulSoup module to parse the page content into soup.'''
    soup = BeautifulSoup(page_source, 'html.parser')
    return soup


# Add every new seller from a set of seller names.
def add_sellers_from_set(driver, sellers):
    '''Add every new seller from a set of seller names.'''
    seller_df = load_df('seller')
    sellers_before = len(seller_df)
    read_sellers = len(sellers)

    # Define loop-control variables and iterate over every seller
    new_sellers = 0
    tries = 0
    seller_ok = False
    for seller_name in sellers:

        # Check if the record already exists
        if seller_name not in seller_df['seller_name'].values:

            # Try to get seller data from page
            while tries < globals.max_tries:
                realistic_pause(0.8*globals.wait_coef)
                driver.get(globals.users_url + seller_name)
                seller_soup = BeautifulSoup(driver.page_source, 'html.parser')
                seller_ok = add_seller(seller_soup)
                if seller_ok:
                    tries = globals.max_tries
                    new_sellers += 1
                else:
                    tries += 1
                    realistic_pause(globals.wait_coef)
            tries = 0

    # Log task finished
    total_sellers = sellers_before + new_sellers
    log(f"Done - {new_sellers} new sellers saved  (out of: "
        + f"{read_sellers}, total: {total_sellers})\n")


# Return a list of all cards found in the expansion cards list.
def get_card_names(driver, expansion_name):
    '''Return a list of all cards found in the expansion cards list.'''
    # Load the number of cards stored in a local file
    exp_filename = urlify(expansion_name)
    exp_file = open('data/' + exp_filename + '.txt', 'r', encoding="utf-8")
    saved_cards = exp_file.read().split('\n')[:-1]
    exp_file.close()
    log("Task - Getting all card names from current expansion")

    all_cards = []
    page_no = 1
    while True:
        # Separate divs that have card links and names
        driver.get(globals.base_url + expansion_name + '?site=' + str(page_no))
        log_url(driver.current_url)
        list_soup = BeautifulSoup(driver.page_source, 'html.parser')
        card_elements = list_soup.findAll("div", {"class": "col-10 col-md-8 "
                                                  + "px-2 flex-column "
                                                  + "align-items-start "
                                                  + "justify-content-center"})

        # Check if there are cards on the page
        if len(card_elements) == 0:
            log("Last page reached")
            break

        # Check if there is a saved complete list of cards from this expansion
        if get_card_hits(list_soup) == len(saved_cards):
            log(f"Done - All card names from {expansion_name} saved\n")
            return saved_cards

        for card in card_elements:
            # Ignore the table header
            if card.string == 'Name':
                continue

            # Append this card's name to all cards
            all_cards.append(str(card.string))

        # Advance to the next page
        page_no += 1
        realistic_pause(0.3*globals.wait_coef)

    # Save the complete cards list to a file
    exp_file = open('data/' + exp_filename + '.txt', 'w', encoding="utf-8")
    for card_name in all_cards:
        exp_file.write(card_name + '\n')
    exp_file.close()

    # Return the complete cards list
    log(f"Done - All card names from {expansion_name} saved\n")
    return all_cards


# Deplete the Load More button to have a complete list of card sellers.
def click_load_more_button(driver):
    '''Deplete the Load More button to have a complete list of card sellers.'''
    elapsed_t = 0.0
    start_t = time()
    realistic_pause(0.5*globals.wait_coef)
    while True:
        try:
            # Locate the button element
            load_more_button = driver \
                .find_element_by_xpath('//button[@id="loadMoreButton"]')

            # Confirm the button is not an empty object
            if load_more_button.text == "":
                return True

            # Click the button and wait
            driver.execute_script("arguments[0].click();", load_more_button)
            realistic_pause(0.2*globals.wait_coef)

            # Check for timeout
            elapsed_t = time() - start_t
            if elapsed_t > globals.button_timeout:
                return False

        # When there is no button
        except common.exceptions.NoSuchElementException:
            return True

        # Other related errors
        except common.exceptions.ElementNotVisibleException:
            return False
        except common.exceptions.NoSuchAttributeException:
            return False
        except common.exceptions.ErrorInResponseException:
            return False
        except common.exceptions.WebDriverException:
            return False
        except common.exceptions.InvalidSessionIdException:
            realistic_pause(globals.wait_coef)
            return False


# Return the given string in url-compatible form, like 'Spell-Snare'.
def urlify(name):
    '''Return the given string in url-compatible form, like 'Spell-Snare'.'''
    url_name = name.replace("'", "")       # Remove apostrophes
    url_name = url_name.replace("(", "")
    url_name = url_name.replace(")", "")   # Remove brackets
    url_name = url_name.replace("-", "")   # Remove dashes
    url_name = url_name.replace("/", "")   # Remove slashes
    url_name = url_name.replace(",", "")   # Remove commas
    url_name = url_name.replace(".", " ")  # Reduce periods
    url_name = url_name.replace(" ", "-")  # Change spaces to dashes
    return url_name


# Return whether the parsed page contains card info and offers info.
def is_valid_card_page(card_soup):
    '''Return whether the parsed page contains card info and offers info.'''
    card_info = card_soup.findAll("dd", {"class": "col-6 col-xl-7"})
    table = card_soup.find("div", {"class": "table "
                                   + "article-table "
                                   + "table-striped"})
    if len(card_info) > 0 and table is not None:
        return True
    return False


# Wait about mean_val seconds before proceeding to the rest of the code.
def realistic_pause(mean_val):
    '''Wait ~mean_val seconds before proceeding to the rest of the code.'''
    std_val = mean_val * random() * 0.3 + 0.1
    sleep_time = abs(normalvariate(mean_val, std_val)) + 0.2
    sleep(sleep_time)


# Return the number of cards in the search results.
def get_card_hits(list_soup):
    '''Return the number of cards in the search results.'''
    hits_div = list_soup.find("div", {"class": "row my-3 "
                                      + "align-items-center"})
    hits_str = str(hits_div.find("div", {"class": "col-auto "
                                         + "d-none d-md-block"}))
    return int(hits_str.split(">")[1].split(" ")[0])
