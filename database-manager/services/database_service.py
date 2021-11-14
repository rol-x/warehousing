import time as tm

import config
import sqlalchemy

from services.logs_service import log


def connect_to_database():
    try:
        uri = 'mysql://{}:{}@{}'.format(config.USER, config.PWD, config.HOST)
        config.DB = sqlalchemy.create_engine(uri)
        # config.DB.execute("CREATE DATABASE gathering")
        # config.DB.execute("USE gathering")
    except Exception as exception:
        log(exception)
        log("Database connection failed")
        raise SystemExit from exception


def run_query(query):
    log(f"Running query: {query}")
    with config.DB.connect() as connection:
        result = connection.execute(query)
        try:
            for row in result:
                log(row)
        except Exception:
            ...


def run_queries_as_transaction(query_list):
    log(f"Running queries: {query_list}")
    with config.DB.begin() as connection:
        for query in query_list:
            connection.execute(query)
        log("Transaction completed")


# Setup empty database to be ready for data.
def setup_database():
    '''Setup empty database to be ready for data.'''

    # Schema
    run_query("USE gathering", silent=False)

    # Date table
    run_query("CREATE TABLE IF NOT EXISTS date( \
                        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                        day TINYINT UNSIGNED NOT NULL, \
                        month TINYINT UNSIGNED NOT NULL, \
                        year SMALLINT UNSIGNED NOT NULL, \
                        weekday TINYINT UNSIGNED)")

    # Card table
    run_query("CREATE TABLE IF NOT EXISTS card( \
                        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                        name VARCHAR(150) NOT NULL, \
                        expansion_name VARCHAR(60), \
                        rarity VARCHAR(10))")

    # Card stats table
    run_query("CREATE TABLE IF NOT EXISTS card_stats( \
                        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                        card_id INT UNSIGNED NOT NULL, \
                        price_from DECIMAL(7, 2) NOT NULL, \
                        monthly_avg DECIMAL(7, 2) NOT NULL, \
                        weekly_avg DECIMAL(7, 2) NOT NULL, \
                        daily_avg DECIMAL(7, 2) NOT NULL, \
                        available_items MEDIUMINT UNSIGNED NOT NULL, \
                        date_id INT UNSIGNED)")

    # Seller table
    run_query("CREATE TABLE IF NOT EXISTS seller( \
                        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                        name VARCHAR(150) NOT NULL, \
                        type VARCHAR(20), \
                        member_since SMALLINT UNSIGNED, \
                        country VARCHAR(100) NOT NULL, \
                        address VARCHAR(255))")

    # Sale offer table
    run_query("CREATE TABLE IF NOT EXISTS sale_offer( \
                        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                        seller_id INT UNSIGNED NOT NULL, \
                        price DECIMAL(7, 2) UNSIGNED NOT NULL, \
                        card_id INT UNSIGNED NOT NULL, \
                        card_condition VARCHAR(20) NOT NULL, \
                        language VARCHAR(20) NOT NULL, \
                        is_foiled BOOLEAN NOT NULL, \
                        amount SMALLINT UNSIGNED NOT NULL, \
                        date_id INT UNSIGNED NOT NULL)")

    # Show the tables in the schema
    log("Tables created.")


def insert_test_data():
    run_query("INSERT INTO date (day, month, year, weekday) \
                     VALUES (27, 9, 2021, 1)")

    run_query("INSERT INTO card (name, expansion_name, rarity) \
                     VALUES ('Test card', 'Battleborn', 'Mystic')")

    run_query("INSERT INTO card_stats (card_id, price_from, monthly_avg, \
                     weekly_avg, daily_avg, available_items, date_id) \
                     VALUES (1, 2.54, 2.33, 2.46, 2.51, 5267, 1)")

    run_query("INSERT INTO seller (name, type, member_since, country) \
                     VALUES ('PartyNator', 'Powerseller', 2015, 'US')")

    run_query("INSERT INTO sale_offer (seller_id, price, card_id, \
                     card_condition, language, is_foiled, amount, date_id) \
                     VALUES (1, 2.54, 1, 'Good', 'English', false, 3, 1)")
    log("Test data inserted.")


def test():
    run_query("SELECT name FROM card WHERE id=264")
    run_query("SELECT weekday FROM date WHERE id=45")
    run_query("SELECT date_id, count(card_id) FROM card_stats \
               GROUP BY date_id")
    run_query("SELECT name FROM seller WHERE id=5")
    run_query("SELECT * FROM sale_offer WHERE card_id=1 \
               AND date_id=45 AND seller_id=5")
    run_query("SHOW COLUMNS FROM sale_offer")
    run_query("SELECT * \
                FROM sale_offer so \
                JOIN seller s ON so.seller_id = s.id \
                JOIN card c ON so.card_id = c.id \
                JOIN date d ON so.date_id = d.id \
                JOIN card_stats cs ON so.card_id = cs.card_id \
                AND so.date_id = cs.date_id \
                WHERE s.id=1 AND d.id=45 AND c.id=5")
    tm.sleep(60)

    # Date        ->  every time, check the delta, update
    # Card        ->  check if needed, check the delta, update
    # Seller      ->  every time, check the delta, update
    # Card stats  ->  every time, slice current day, create or update on these
    # Sale offer  ->  every time, slice current day and card, create or update

    # Date OK  \                        Date OK   \
    #           -->  Card stats         Seller OK  -->  Sale offer
    # Card OK  /                        Card OK   /

    # Date FAIL or Card FAIL  -->  ROLLBACK {Faulty}, Card stats, Sale offer
    # Seller FAIL  -->  ROLLBACK Seller, Sale offer
    # {Any} FAIL  -->  ROLLBACK {Faulty}


def close_connection():
    config.DB.dispose()
