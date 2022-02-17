"""Store and initialize global configuration of the program."""

# Variables connected to a single run of the code
DB_CONN = None
NEW_CHECKSUM = ''
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

# SQL static queries
DROP = {"date": "DROP TABLE IF EXISTS date",
        "card": "DROP TABLE IF EXISTS card",
        "seller": "DROP TABLE IF EXISTS seller",
        "card_stats": "DROP TABLE IF EXISTS card_stats",
        "sale_offer": "DROP TABLE IF EXISTS sale_offer"}

CREATE = {"date": '''CREATE TABLE date (
                        id SMALLINT UNSIGNED PRIMARY KEY,
                        day TINYINT(2) UNSIGNED NOT NULL,
                        month TINYINT(2) UNSIGNED NOT NULL,
                        year SMALLINT(4) UNSIGNED NOT NULL,
                        weekday TINYINT(1) UNSIGNED NOT NULL)''',

          "card": '''CREATE TABLE card (
                        id SMALLINT UNSIGNED PRIMARY KEY,
                        name VARCHAR(50) NOT NULL,
                        expansion VARCHAR(30) NOT NULL,
                        rarity VARCHAR(20) NOT NULL)''',

          "seller": '''CREATE TABLE seller (
                        id SMALLINT UNSIGNED PRIMARY KEY,
                        name VARCHAR(50) NOT NULL,
                        type VARCHAR(12) NOT NULL,
                        member_since SMALLINT(4) UNSIGNED NOT NULL,
                        country VARCHAR(30) NOT NULL,
                        address VARCHAR(255))''',

          "card_stats": '''CREATE TABLE card_stats (
                        id MEDIUMINT UNSIGNED PRIMARY KEY,
                        card_id SMALLINT UNSIGNED NOT NULL,
                        price_from DECIMAL(7, 2) UNSIGNED NOT NULL,
                        monthly_avg DECIMAL(7, 2) UNSIGNED NOT NULL,
                        weekly_avg DECIMAL(7, 2) UNSIGNED NOT NULL,
                        daily_avg DECIMAL(7, 2) UNSIGNED NOT NULL,
                        available_items MEDIUMINT UNSIGNED NOT NULL,
                        date_id SMALLINT UNSIGNED NOT NULL)''',

          "sale_offer": '''CREATE TABLE sale_offer (
                        id INT UNSIGNED PRIMARY KEY,
                        seller_id SMALLINT UNSIGNED NOT NULL,
                        price DECIMAL(7, 2) UNSIGNED NOT NULL,
                        card_id SMALLINT UNSIGNED NOT NULL,
                        card_condition VARCHAR(20) NOT NULL,
                        card_language VARCHAR(10) NOT NULL,
                        is_foiled TINYINT(1) NOT NULL,
                        amount SMALLINT UNSIGNED NOT NULL,
                        date_id SMALLINT UNSIGNED NOT NULL)'''}
