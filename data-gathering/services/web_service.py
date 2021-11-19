import time as tm

import config
from bs4 import BeautifulSoup
from selenium import common, webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.exceptions import MaxRetryError

# Web service shouldn't do that
from services.data_service import add_seller, load_csv
from services.logs_service import log_url, logr


# Return the Firefox webdriver in headless mode.
def connect_webdriver():
    '''Return the Firefox webdriver in headless mode.'''
    options = Options()
    options.headless = True
    try:
        driver = webdriver.Remote("http://" + config.WEBDRIVER_HOSTNAME
                                  + ":4444/wd/hub", options=options)
        logr('Webdriver connection ready\n')
        return driver
    except MaxRetryError as exception:
        logr(exception)
        logr('The connection to remote webdriver failed. '
             + 'Check if the container is running.')
        logr('If it is, check the hostname in config.py '
             + 'and other connection settings.\n')
        raise SystemExit from exception


# Use the BeautifulSoup module to parse the page content into soup.
def create_soup(page_source):
    '''Use the BeautifulSoup module to parse the page content into soup.'''
    soup = BeautifulSoup(page_source, 'html.parser')
    return soup


# Add every new seller from a set of seller names.
def iterate_over_sellers(driver, sellers):
    '''Add every new seller from a set of seller names.'''
    seller_df = load_csv('seller')
    read_sellers = len(sellers)

    start = tm.time()

    # Define loop-control variables and iterate over every seller
    new_sellers = 0
    to_add = [name for name in sellers if name not in seller_df['name'].values]
    if len(to_add) > 1:
        logr(f"{len(to_add)} new sellers to be added.")

    for seller_name in to_add:

        # Try to get seller data from page
        for try_num in range(3):
            driver.get(config.USERS_URL + seller_name)
            cooldown(-4 + 3 * try_num)   # 1.97s -> 6.67s -> 22.5s
            seller_soup = create_soup(driver.page_source)
            updated = add_seller(seller_df, seller_soup)
            if updated is not None:
                seller_df = updated
                new_sellers += 1
                break
            cooldown(-7 + 2 * try_num)  # 0.58s -> 1.31s -> 2.96s

    # Task finished
    seller_df.to_csv(f'./data/seller.csv', compression='gzip',
                     sep=';', encoding='utf-8', index=False)
    logr(f"Done - {new_sellers} new sellers saved  (out of: "
         + f"{read_sellers}, total: {len(seller_df.index)})")
    logr(f"Time: {round(tm.time() - start, 3)}\n")


# Return a list of all cards found in the expansion cards list.
def get_card_names(driver, expansion_name):
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


def load_card_page(driver, card_url):
    CARD_INFO = '//dd[@class="col-6 col-xl-7"]'
    TABLE = '//div[@class="table article-table table-striped"]'
    driver.get(card_url)
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
def click_load_more_button(driver):
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
