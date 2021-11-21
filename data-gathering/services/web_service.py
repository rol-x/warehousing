import time as tm

import config
from bs4 import BeautifulSoup
from selenium import common, webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.exceptions import MaxRetryError

from services.logs_service import log_url, logr

driver: webdriver


# Return the Firefox webdriver in headless mode.
def connect_webdriver():
    '''Return the Firefox webdriver in headless mode.'''
    options = Options()
    options.headless = True
    global driver
    try:
        driver = webdriver.Remote("http://" + config.WEBDRIVER_HOSTNAME
                                  + ":4444/wd/hub", options=options)
        logr('Webdriver connection ready\n')
    except MaxRetryError as exception:
        logr(exception)
        logr('The connection to remote webdriver failed. '
             + 'Check if the container is running.')
        logr('If it is, check the hostname in config.py '
             + 'and other connection settings.\n')
        raise SystemExit from exception


# Use the BeautifulSoup module to parse the page content into soup.
def get_soup():
    '''Use the BeautifulSoup module to parse the page content into soup.'''
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup


# Return a list of all cards found in the expansion cards list.
def get_card_names(expansion_name):
    '''Return a list of all cards found in the expansion cards list.'''
    # Load the number of cards stored in a local file
    exp_filename = urlify(expansion_name)
    with open('./data/' + exp_filename + '.txt', 'r',
              encoding="utf-8") as exp_file:
        saved_cards = [line.strip('\n') for line in exp_file.readlines()]
    logr("Task - Getting all card names from current expansion")

    driver.implicitly_wait(2.5)
    all_cards = []
    page_no = 1
    while True:
        # Separate divs that have card links and names
        driver.get(config.BASE_URL + expansion_name + '?site=' + str(page_no))
        log_url(driver.current_url)
        list_soup = BeautifulSoup(driver.page_source, 'html.parser')
        card_elements = list_soup.findAll("div", {"class": "col-10 col-md-8 "
                                                  + "px-2 flex-column "
                                                  + "align-items-start "
                                                  + "justify-content-center"})

        # Check if there are cards on the page
        if len(card_elements) == 0:
            logr("Last page reached")
            break

        # Check if there is a saved complete list of cards from this expansion
        if get_card_hits(list_soup) == len(saved_cards):
            logr(f"Done - All card names from {expansion_name} saved\n")
            return saved_cards

        for card in card_elements:
            # Ignore the table header
            if card.string == 'Name':
                continue

            # Append this card's name to all cards
            all_cards.append(str(card.string))

        # Advance to the next page
        page_no += 1

    driver.implicitly_wait(0.5)

    # Save the complete cards list to a file
    with open('./data/' + exp_filename + '.txt', 'w',
              encoding="utf-8") as exp_file:
        for card_name in all_cards:
            exp_file.write(card_name + '\n')

    # Return the complete cards list
    logr(f"Done - All card names from {expansion_name} saved\n")
    return all_cards


def load_card_page(card_name):
    CARD_INFO = '//dd[@class="col-6 col-xl-7"]'
    TABLE = '//div[@class="table article-table table-striped"]'
    driver.get(config.BASE_URL
               + config.EXPANSION_NAME + '/' + urlify(card_name))
    log_url(driver.current_url)
    try:
        WebDriverWait(driver, timeout=5, poll_frequency=0.5) \
            .until(EC.title_contains("MTG Singles | Cardmarket"))
        WebDriverWait(driver, timeout=5, poll_frequency=0.5) \
            .until(EC.presence_of_element_located((By.XPATH, CARD_INFO)))
        WebDriverWait(driver, timeout=5, poll_frequency=0.5) \
            .until(EC.presence_of_element_located((By.XPATH, TABLE)))
        return True

    except common.exceptions.TimeoutException:
        return False
    except Exception as exception:
        logr(exception)
        return False


# Deplete the Load More button to have a complete list of card sellers.
def click_load_more_button():
    '''Deplete the Load More button to have a complete list of card sellers.'''
    BUTTON = '//button[@id="loadMoreButton"]'
    SPINNER = '//div[@class="spinner"]'
    while True:
        try:
            WebDriverWait(driver, timeout=3, poll_frequency=0.3) \
                .until(EC.invisibility_of_element_located((By.XPATH, SPINNER)))

            button = WebDriverWait(driver, timeout=3, poll_frequency=0.3) \
                .until(EC.element_to_be_clickable((By.XPATH, BUTTON)))
            if button.text == "SHOW MORE RESULTS":
                button.click()

        # When there is no more to load
        except common.exceptions.TimeoutException:
            return True

        # Other related errors
        except common.exceptions.ElementNotVisibleException:
            return False
        except common.exceptions.NoSuchAttributeException:
            return False
        except common.exceptions.ErrorInResponseException:
            return False
        except Exception as exception:
            logr(exception)
            return False


def load_seller_page(seller_name):
    driver.get(config.USERS_URL + seller_name)


def cooldown(coefficient):
    tm.sleep(10 * 1.5 ** coefficient)


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


# Return the number of cards in the search results.
def get_card_hits(list_soup):
    '''Return the number of cards in the search results.'''
    hits_div = list_soup.find("div", {"class": "row my-3 "
                                      + "align-items-center"})
    hits_str = str(hits_div.find("div", {"class": "col-auto "
                                         + "d-none d-md-block"}))
    return int(hits_str.split(">")[1].split(" ")[0])
