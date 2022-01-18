"""Store and initialize global configuration of the program."""

# Variables connected to this single run of the code
DATE_ID = 1
DB_CONN = None
MAIN_LOGNAME = 'other_main.log'
RUN_LOGNAME = 'other_run.log'
DB_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'mysql_database',
    'database': 'gathering',
    'raise_on_warnings': False
}

# Fixed variables
CONTAINER_DELAY = 30
NAME = 'database-manager'
HEADERS = {"date": "id, day, month, year, weekday",
           "card": "id, name, expansion, rarity",
           "seller": "id, name, type, member_since, country, address",
           "card_stats": "id, card_id, price_from, monthly_avg, "
                         + "weekly_avg, daily_avg, available_items, date_id",
           "sale_offer": "id, seller_id, price, card_id, card_condition, "
                         + "card_language, is_foiled, amount, date_id"}
