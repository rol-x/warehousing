"""Store and initialize global configuration of the program."""

# Variables for user custom program configuration
START_FROM = 1
FORCE_UPDATE = False
EXPANSION_NAME = 'Battlebond'

# Variables connected to a single run of code
DATE_ID = 0
MAIN_LOGNAME = 'other_main.log'
RUN_LOGNAME = 'other_run.log'

# Fixed variables
NAME = 'data-gathering'
BASE_URL = 'https://www.cardmarket.com/en/Magic/Products/Singles/'
USERS_URL = 'https://www.cardmarket.com/en/Magic/Users/'
WEBDRIVER_HOSTNAME = 'firefox_webdriver'
CONTAINER_DELAY = 10
HEADERS = {"date": {"id": "int",
                    "day": "int",
                    "month": "int",
                    "year": "int",
                    "weekday": "int"},

           "card": {"id": "int",
                    "name": "str",
                    "expansion": "str",
                    "rarity": "str"},

           "seller": {"id": "int",
                      "name": "str",
                      "type": "str",
                      "member_since": "int",
                      "country": "str",
                      "address": "str"},

           "card_stats": {"id": "int",
                          "card_id": "int",
                          "price_from": "float",
                          "available_items": "int",
                          "daily_avg": "float",
                          "weekly_avg": "float",
                          "monthly_avg": "float",
                          "date_id": "int"},

           "sale_offer": {"id": "int",
                          "seller_id": "int",
                          "price": "float",
                          "card_id": "int",
                          "card_condition": "str",
                          "card_language": "str",
                          "is_foiled": "bool",
                          "amount": "int",
                          "date_id": "int"}}
