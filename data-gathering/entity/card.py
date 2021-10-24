from handlers.data_handler import load_df
from services.logs_service import logr


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
