import config
import mysql.connector
import time

from services.logs_service import log


def connect_to_database():
    try:
        config.DATABASE = mysql.connector.connect(
                            host="mysql_database",
                            user="database-manager",
                            password="h4Rd_p4sSW0rD")
    except Exception as exception:
        log(exception)
        raise SystemExit from exception
    log("Database connection established")


def run_query(query, silent=False):
    log(f"Executing query: {query}")
    with config.DATABASE.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        if not silent:
            for row in results:
                log(row)
    return results


def run_queries(queries, silent=True):
    for query in queries:
        run_query(query, silent)
        time.sleep(2)


# Setup empty database to be ready for data.
def setup_database():
    '''Setup empty database to be ready for data.'''

    # Schema
    run_query("USE gathering")

    # Card table
    run_queries(["CREATE TABLE IF NOT EXISTS card( \
                            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                            name VARCHAR(150) NOT NULL, \
                            expansion_name VARCHAR(60), \
                            rarity VARCHAR(10))",
                 "INSERT INTO card (name, expansion_name, rarity) \
                     VALUES ('Test card', 'Battleborn', 'Mystic')"])
    # Date table
    run_queries(["CREATE TABLE IF NOT EXISTS date( \
                            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                            day TINYINT UNSIGNED NOT NULL, \
                            month TINYINT UNSIGNED NOT NULL, \
                            year SMALLINT UNSIGNED NOT NULL, \
                            dow TINYINT UNSIGNED)",
                 "INSERT INTO date (day, month, year, dow) \
                     VALUES (25, 10, 2021, 1)"])
    # Card stats table
    run_queries(["CREATE TABLE IF NOT EXISTS card_stats( \
                            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                            card_id INT UNSIGNED NOT NULL, \
                            price_from DECIMAL(7, 2) NOT NULL, \
                            monthly_avg DECIMAL(7, 2) NOT NULL, \
                            weekly_avg DECIMAL(7, 2) NOT NULL, \
                            daily_avg DECIMAL(7, 2) NOT NULL, \
                            available_items MEDIUMINT UNSIGNED NOT NULL, \
                            date_id INT UNSIGNED)",
                 "INSERT INTO card_stats (card_id, price_from, monthly_avg, \
                     weekly_avg, daily_avg, available_items, date_id) \
                     VALUES (1, 2.54, 2.33, 2.46, 2.51, 5267, 1)"])
    # Seller table
    run_queries(["CREATE TABLE IF NOT EXISTS seller( \
                            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                            name VARCAHR(150) NOT NULL, \
                            type VARCHAR(20), \
                            member_since SMALLINT UNSIGNED, \
                            country VARCHAR(100) NOT NULL, \
                            address VARCHAR(255))",
                 "INSERT INTO seller (name, type, member_since, country) \
                     VALUES ('PartyNator', 'Powerseller', 2015, 'US')"])

    # Sale offer table
    run_queries(["CREATE TABLE IF NOT EXISTS sale_offer( \
                            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                            seller_id INT UNSIGNED NOT NULL, \
                            price DECIMAL(7, 2) UNSIGNED NOT NULL, \
                            card_id INT UNSIGNED NOT NULL, \
                            card_condition VARCHAR(20) NOT NULL, \
                            language VARCHAR(20) NOT NULL, \
                            is_foiled BOOLEAN NOT NULL, \
                            amount SMALLINT UNSIGNED NOT NULL, \
                            date_id INT UNSIGNED NOT NULL)",
                 "INSERT INTO sale_offer (seller_id, price, card_id, \
                     card_condition, language, is_foiled, amount, date_id) \
                     VALUES (1, 2.54, 1, 'Good', 'English', false, 3, 1)"])

    run_query("SHOW TABLES")


def run_update():
    # Create a connection to the database
    log("Database update here")

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
    config.DATABASE.close()
