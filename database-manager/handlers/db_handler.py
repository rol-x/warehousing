import time

import config
import mysql.connector
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


def run_fetch_query(query, silent=False):
    if not silent:
        log(f"Executing query: {query}")
    with config.DATABASE.cursor(buffered=True) as cursor:
        cursor.execute(query)
        if not silent:
            for row in cursor.fetchall():
                log(row)
        columns = [column for column in cursor.description]
        values = cursor.fetchall()
    return {"columns": columns, "values": values}


def run_query(query, silent=True):
    if not silent:
        log(f"Executing query: {query}")
    with config.DATABASE.cursor(buffered=True) as cursor:
        cursor.execute(query)


# Setup empty database to be ready for data.
def setup_database():
    '''Setup empty database to be ready for data.'''

    # Schema
    run_query("USE gathering", silent=False)

    # Add persistence
    run_fetch_query("SHOW TABLES")

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
    run_fetch_query("SHOW TABLES")


def insert_test_data():
    run_query("INSERT INTO date (day, month, year, weekday) \
                     VALUES (25, 10, 2021, 1)")

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
    insert_test_data()
    run_fetch_query("SELECT * FROM card")
    run_fetch_query("SELECT * FROM date")
    run_fetch_query("SELECT * FROM card_stats")
    run_fetch_query("SELECT * FROM seller")
    run_fetch_query("SELECT * FROM sale_offer")
    run_fetch_query("SHOW COLUMNS FROM sale_offer")
    run_fetch_query("SELECT * \
                     FROM sale_offer so \
                     JOIN seller s ON so.seller_id = s.id \
                     JOIN card c ON so.card_id = c.id \
                     JOIN date d ON so.date_id = d.id \
                     JOIN card_stats cs ON so.card_id = cs.card_id \
                     AND so.date_id = cs.date_id")
    time.sleep(60)


def get_database_content():
    date = run_fetch_query("SELECT * FROM date")
    card = run_fetch_query("SELECT * FROM card")
    card_stats = run_fetch_query("SELECT * FROM card_stats")
    seller = run_fetch_query("SELECT * FROM seller")
    sale_offer = run_fetch_query("SELECT * FROM sale_offer")
    return {"date": date, "card": card, "card_stats": card_stats,
            "seller": seller, "sale_offer": sale_offer}


def run_update(deltas):
    # Create a connection to the database
    log("Database update here")
    date_d = deltas.get('date')
    card_d = deltas.get('card')
    card_stats_d = deltas.get('card_stats')
    seller_d = deltas.get('seller')
    sale_offer_d = deltas.get('sale_offer')

    for id in date_d.keys():
        insert_into_date(date_d[id].get('day'),
                         date_d[id].get('month'),
                         date_d[id].get('year'),
                         date_d[id].get('weekday'))
        break

    for id in card_d.keys():
        insert_into_card(card_d[id].get('name'),
                         card_d[id].get('expansion_name'),
                         card_d[id].get('rarity'))
        break

    for id in card_stats_d.keys():
        insert_into_card_stats(card_stats_d[id].get('card_id'),
                               card_stats_d[id].get('price'),
                               card_stats_d[id].get('monthly_avg'),
                               card_stats_d[id].get('weekly_avg'),
                               card_stats_d[id].get('daily_avg'),
                               card_stats_d[id].get('available_items'),
                               card_stats_d[id].get('date_id'))
        break

    for id in seller_d.keys():
        insert_into_seller(seller_d[id].get('name'),
                           seller_d[id].get('type'),
                           seller_d[id].get('memner_since'),
                           seller_d[id].get('country'),
                           seller_d[id].get('address'))
        break

    for id in sale_offer_d.keys():
        insert_into_sale_offer(sale_offer_d[id].get('seller_id'),
                               sale_offer_d[id].get('price'),
                               sale_offer_d[id].get('card_id'),
                               sale_offer_d[id].get('card_condition'),
                               sale_offer_d[id].get('card_language'),
                               sale_offer_d[id].get('is_foiled'),
                               sale_offer_d[id].get('amount'),
                               sale_offer_d[id].get('date_id'))
        break

    # Date        ->  every time, check the delta, update
    # Card        ->  check if needed, check the delta, update
    # Seller      ->  every time, check the delta, update
    # Card stats  ->  every time, slice current day, create or update on these
    # Sale offer  ->  every time, slice current day and card, create or update

    # Date OK  \                        Date OK   \
    #           -->  Card stats         Seller OdowK  -->  Sale offer
    # Card OK  /                        Card OK   /

    # Date FAIL or Card FAIL  -->  ROLLBACK {Faulty}, Card stats, Sale offer
    # Seller FAIL  -->  ROLLBACK Seller, Sale offer
    # {Any} FAIL  -->  ROLLBACK {Faulty}


def insert_into_date(day, month, year, weekday):
    run_query("INSERT INTO date (day, month, year, weekday) VALUES "
              + f"({day}, {month}, {year}, {weekday})")


def insert_into_card(name, expansion_name, rarity):
    run_query("INSERT INTO card (name, expansion_name, rarity) VALUES "
              + f"({name}, {expansion_name}, {rarity})")


def insert_into_card_stats(card_id, price_from, monthly_avg,
                           weekly_avg, daily_avg, available_items, date_id):
    run_query("INSERT INTO card_stats (card_id, price_from, monthly_avg, \
                     weekly_avg, daily_avg, available_items, date_id) VALUES "
              + f"({card_id}, {price_from}, {monthly_avg}, {weekly_avg}, \
                  {daily_avg}, {available_items}, {date_id})")


def insert_into_seller(name, type, member_since, country, address):
    run_query("INSERT INTO seller (name, type, member_since, country, \
        address) VALUES "
              + f"({name}, {type}, {member_since}, {country}, {address})")


def insert_into_sale_offer(seller_id, price, card_id, card_condition,
                           language, is_foiled, amount, date_id):
    run_query("INSERT INTO sale_offer (seller_id, price, card_id, \
                     card_condition, language, is_foiled, \
                         amount, date_id) VALUES "
              + f"({seller_id}, {price}, {card_id}, {card_condition}, \
                  {language}, {is_foiled}, {amount}, {date_id})")


def close_connection():
    config.DATABASE.close()
