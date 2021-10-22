import config
import mysql.connector
import time

from handlers.log_handler import log


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
        time.sleep(3)


def setup_database():
    run_query("SHOW DATABASES")
    run_queries(["USE gathering",
                 "CREATE TABLE IF NOT EXIST card( \
                            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                            name VARCHAR(50), \
                            expansion_name VARCHAR(20), \
                            rarity VARCHAR(20))",
                 "INSERT INTO card (name, expansion_name, rarity) \
                     VALUES ('Test card', 'Battleborn', 'Mystic')",
                 "SELECT * FROM card"])


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
