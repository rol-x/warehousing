import config
import mysql.connector

from services.logs_service import log


def connect_to_database():
    config.DB_CONN = mysql.connector.connect(**config.DB_CONFIG)
    config.DB_CONN.autocommit = False
    with config.DB_CONN.cursor() as cursor:
        cursor.execute("USE gathering")


def update_table(table_name):
    cursor = config.DB_CONN.cursor()
    try:
        drop = config.DROP[table_name]
        create = config.CREATE[table_name]
        load = f'''LOAD DATA
                   INFILE '/var/lib/mysql-files/{table_name}.csv'
                   REPLACE
                   INTO TABLE {table_name}
                   FIELDS TERMINATED BY ';'
                   IGNORE 1 LINES'''

        cursor.execute(drop)
        cursor.execute(create)
        cursor.execute(load)

        config.DB_CONN.commit()
    except mysql.connector.Error as error:
        log(f"Failed to update table {table_name}: {error}")
        config.DB_CONN.rollback()
    finally:
        cursor.close()


def select_table(table_name, conditions=""):
    cursor = config.DB_CONN.cursor()
    select = f"SELECT * FROM {table_name} " + conditions
    cursor.execute(select)
    return cursor.fetchall(), cursor.column_names


# Close the database connection
def close_connection():
    config.DB_CONN.close()
