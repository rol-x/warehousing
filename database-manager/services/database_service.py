import config
import sqlalchemy

from services.logs_service import log


def connect_to_database():
    try:
        uri = 'mysql://{}:{}@{}'.format(config.USER, config.PWD, config.HOST)
        config.DB = sqlalchemy.create_engine(uri)
        config.DB.execute("CREATE DATABASE IF NOT EXISTS gathering")
        config.DB.execute("USE gathering")
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


# Close the database connection
def close_connection():
    config.DB.dispose()
