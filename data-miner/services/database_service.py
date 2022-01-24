import config
import mysql.connector

from services.logs_service import log


def connect_to_database():
    config.DB_CONN = mysql.connector.connect(**config.DB_CONFIG)
    with config.DB_CONN.cursor() as cursor:
        cursor.execute("USE gathering")


def check_database():
    cursor = config.DB_CONN.cursor(buffered=True)
    check = """SELECT COUNT(*) AS tables_number
               FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = 'gathering'"""
    tables = -1
    try:
        cursor.execute(check)
        tables = cursor.fetchone()[0]
    except mysql.connector.Error as error:
        log(f"Failed to fetch the number of tables.")
        log(error)
    finally:
        log(f"Tables number: {tables}")
        cursor.close()
        return tables


def set_current_date():
    cursor = config.DB_CONN.cursor(buffered=True)
    date = "SELECT MAX(date_id) as this_date FROM sale_offer"
    try:
        cursor.execute(date)
        config.DATE_ID = cursor.fetchone()[0]
    except mysql.connector.Error as error:
        log(f"Failed to fetch current date_id.")
        log(error)
    finally:
        cursor.close()


def create_table_last_two_weeks():
    cursor = config.DB_CONN.cursor()
    drop = "DROP TABLE IF EXISTS last_two_weeks"
    create = "CREATE TABLE last_two_weeks \
             AS \
             SELECT \
                    so.seller_id AS seller_id, \
                    so.card_id AS card_id, \
                    cs.date_id AS date_id, \
                    so.price AS price, \
                    cs.weekly_avg AS weekly_avg, \
                    so.card_condition AS card_condition, \
                    so.card_language AS card_language, \
                    so.is_foiled AS is_foiled \
             FROM \
             sale_offer so \
             INNER JOIN card_stats cs ON so.card_id = cs.card_id \
                AND so.date_id = cs.date_id \
             WHERE cs.date_id>=" + str(config.DATE_ID - 14)
    try:
        cursor.execute(drop)
        cursor.execute(create)
        log("Created table last_two_weeks")
    except mysql.connector.Error as error:
        log(f"Error creating table last_two_weeks.")
        log(error)
    finally:
        cursor.close()


def create_table_offers_today():
    cursor = config.DB_CONN.cursor()
    drop = "DROP TABLE IF EXISTS offers_today"
    create = "CREATE TABLE offers_today AS \
              SELECT * FROM sale_offer \
              WHERE date_id=" + str(config.DATE_ID)
    try:
        cursor.execute(drop)
        cursor.execute(create)
        log("Created table offers_today")
    except mysql.connector.Error as error:
        log(f"Error creating table offers_today.")
        log(error)
    finally:
        cursor.close()


# Close the database connection
def close_connection():
    config.DB_CONN.close()
