import time as tm

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
        values = cursor.fetchall()
        columns = cursor.column_names
        if not silent:
            for row in values:
                log(row)
    return {"columns": columns, "values": values}


def run_query(query, silent=True):
    if not silent:
        log(f"Executing query: {query}")
    with config.DATABASE.cursor(buffered=True) as cursor:
        cursor.execute(query)


def run_insert_query(entity, values):
    query = f"INSERT INTO {entity.name} ({entity.headers}) \
              VALUES ({entity.args})"
    with config.DATABASE.cursor() as cursor:
        cursor.executemany(query, values)
        config.DATABASE.commit()
        log(f"{cursor.rowcount} was inserted.")


def run_delete_query(entity, values):
    query = f"DELETE FROM {entity.name} a WHERE a.id = %s"
    with config.DATABASE.cursor() as cursor:
        cursor.executemany(query, values)
        config.DATABASE.commit()
        log(f"{cursor.rowcount} was deleted.")


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
    tm.sleep(60)


def table_content(entity):
    return run_fetch_query(f"SELECT * FROM {entity}", silent=True)


def table_content_since(entity, date_index):
    dates = "("
    for date_id in date_index:
        dates += f"{date_id}, "
    dates = dates.strip(', ') + ")"

    return run_fetch_query(f"SELECT * FROM {entity} \
                             WHERE {entity}.date_id IN {dates}",
                           silent=True)


def insert_date(day, month, year, weekday):
    run_query(f"INSERT INTO date (day, month, year, weekday) \
                     VALUES ({day}, {month}, {year}, {weekday})")


def insert_card(name, expansion_name, rarity):
    run_query(f"INSERT INTO card (name, expansion_name, rarity) \
                     VALUES ({name}, {expansion_name}, {rarity})")


def insert_card_stats(card_id, price_from, monthly_avg, weekly_avg,
                      daily_avg, available_items, date_id):
    run_query(f"INSERT INTO card_stats (card_id, price_from, monthly_avg, \
                     weekly_avg, daily_avg, available_items, date_id) \
                     VALUES ({card_id}, {price_from}, {monthly_avg}, \
                     {weekly_avg}, {daily_avg}, {available_items}, {date_id})")


def insert_seller(name, type, member_since, country):
    run_query(f"INSERT INTO seller (name, type, member_since, country) \
                     VALUES ({name}, {type}, {member_since}, {country})")


def insert_row(entity, values):
    query = "INSERT INTO " + entity + "(???) VALUES (???)"


def remove_row(entity, id):
    run_query(f"DELETE FROM {entity} a WHERE a.id = {id}")


def run_update(new_data):
    # Create a connection to the database
    log("Database update here")
    date_d = new_data.get('date')
    card_d = new_data.get('card')
    card_stats_d = new_data.get('card_stats')
    seller_d = new_data.get('seller')
    sale_offer_d = new_data.get('sale_offer')

    for id in date_d.keys():
        log(date_d[id])
        insert_date(date_d[id].get('day'),
                    date_d[id].get('month'),
                    date_d[id].get('year'),
                    date_d[id].get('weekday'))
        break

    for id in card_d.keys():
        log(card_d[id])
        insert_card(card_d[id].get('name'),
                    card_d[id].get('expansion_name'),
                    card_d[id].get('rarity'))
        break

    for id in card_stats_d.keys():
        log(card_stats_d[id])
        insert_card_stats(card_stats_d[id].get('card_id'),
                          card_stats_d[id].get('price'),
                          card_stats_d[id].get('monthly_avg'),
                          card_stats_d[id].get('weekly_avg'),
                          card_stats_d[id].get('daily_avg'),
                          card_stats_d[id].get('available_items'),
                          card_stats_d[id].get('date_id'))
        break

    for id in seller_d.keys():
        log(seller_d[id])
        insert_seller(seller_d[id].get('name'),
                      seller_d[id].get('type'),
                      seller_d[id].get('memner_since'),
                      seller_d[id].get('country'),
                      seller_d[id].get('address'))
        break

    for id in sale_offer_d.keys():
        log(sale_offer_d[id])
        # insert_sale_offer(sale_offer_d[id].get('seller_id'),
        #                   sale_offer_d[id].get('price'),
        #                   sale_offer_d[id].get('card_id'),
        #                   sale_offer_d[id].get('card_condition'),
        #                   sale_offer_d[id].get('card_language'),
        #                   sale_offer_d[id].get('is_foiled'),
        #                   sale_offer_d[id].get('amount'),
        #                   sale_offer_d[id].get('date_id'))
        break

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
