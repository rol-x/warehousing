from services.logs_service import log, logr
from services.data_service import load

name = 'card'
headers = "name, expansion, rarity"
args = "%s, %s, %s"
static = True

new_data = None
old_data = None


class Card(object):

    def __init__(self, card_soup):
        # Get rows from the card information table
        card_info = card_soup.findAll("dd", {"class": "col-6 col-xl-7"})
        if len(card_info) == 0:
            log('No card info found on current page')
            self.id = None
            self.name = None
            self.expansion_name = None
            self.rarity = None
            return

        # Get the attributes
        self.id = len(load('card').index) + 1
        self.name = str(card_soup.find("h1")).split('<')[1][3:]
        self.expansion_name = card_info[1].find('span')['data-original-title']
        self.rarity = card_info[0].find('span')['data-original-title']

    def __init__(self, id, name, expansion_name, rarity):
        self.id = id
        self.name = name
        self.expansion_name = expansion_name
        self.rarity = rarity

    def save_to_file(self):

        # Logging
        logr('== Add card ==')
        logr('Card ID:       ' + str(self.id))
        logr('Card:          ' + str(self.name))
        logr('Rarity:        ' + str(self.rarity))
        logr('Expansion:     ' + str(self.expansion_name) + '\n')

        # Writing to the file
        with open('./data/card.csv', 'a', encoding="utf-8") as card_csv:
            card_csv.write(str(self.id) + ';')
            card_csv.write(self.name + ';')
            card_csv.write(self.expansion_name + ';')
            card_csv.write(self.rarity + '\n')

    def get_id(self):
        return self.id

    def is_card_saved(self):
        return self.name in load('card')['name'].values()


def prepare_data():
    ...
