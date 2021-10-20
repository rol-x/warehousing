from mysql.connector import connect, Exception
from log_handler import log
import mysql.connector


def connect_to_database():
    database = mysql.connector.connect(
                host="localhost",
                user="yourusername",
                password="yourpassword")
    return database.cursor()


def run_query(query):
    try:
        with connect(
            host="localhost",
            user="database-manager",
            password="P@ssword"
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
    except Exception as exception:
        log(exception)
        raise Exception
    return result


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
