"""Store and initialize global configuration of the program."""

# Variables for user custom program configuration
START_FROM = 264
FORCE_UPDATE = True
EXPANSION_NAME = 'Battlebond'

# Variables connected to a single run of code
THIS_DATE_ID = 0
MAIN_LOGNAME = 'other_main.log'
RUN_LOGNAME = 'other_run.log'

# Fixed variables
NAME = 'data-gathering'
BASE_URL = 'https://www.cardmarket.com/en/Magic/Products/Singles/'
USERS_URL = 'https://www.cardmarket.com/en/Magic/Users/'
WEBDRIVER_HOSTNAME = 'firefox_webdriver'
CONTAINER_DELAY = 10
HEADERS = {"date": ["id",
                    "day",
                    "month",
                    "year",
                    "weekday"],

           "card": ["id",
                    "name",
                    "expansion",
                    "rarity"],

           "seller": ["id",
                      "name",
                      "type",
                      "member_since",
                      "country",
                      "address"],

           "card_stats": ["id",
                          "card_id",
                          "price_from",
                          "monthly_avg",
                          "weekly_avg",
                          "daily_avg",
                          "available_items",
                          "date_id"],

           "sale_offer": ["id",
                          "seller_id",
                          "price",
                          "card_id",
                          "card_condition",
                          "card_language",
                          "is_foiled",
                          "amount",
                          "date_id"]}
