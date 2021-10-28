import time

import config
import services.flags_service as flags
from entity import card as Card
from entity import card_stats as CardStats
from entity import date as Date
from entity import sale_offer as SaleOffer
from entity import seller as Seller
from handlers import db_handler, file_handler
from services.logs_service import log, setup_logs, setup_main_logfile

# TODO: Write simple database connection, create empty tables if there are none
# TODO: Check local data against database contents & decide how much to update
# TODO: Write database updating section for inserting and updating data
# TODO: Verify the integrity of the database before and after the update
# TODO: Complete main logging and run logging


def load_database_data(new_dates):

    # Read the date, card and seller data from the database
    date = file_handler.transform_database_data(
        db_handler.get_table_content('date'))
    card = file_handler.transform_database_data(
        db_handler.get_table_content('card'))
    seller = file_handler.transform_database_data(
        db_handler.get_table_content('seller'))

    # Determine which dates are new to the database
    update_dates = file_handler.compare_dates(date, new_dates)
    log("Update dates: ")
    log(update_dates)

    # Load the relevant slices from the database
    card_stats_slice = file_handler.transform_database_data(
        db_handler.get_table_content_since_date('card_stats', update_dates))
    sale_offer_slice = file_handler.transform_database_data(
        db_handler.get_table_content_since_date('sale_offer', update_dates))

    return {"date": date, "card": card, "seller": seller,
            "card_stats": card_stats_slice, "sale_offer": sale_offer_slice}


# Main function
def main():
    # Set the current run log filename
    setup_logs()

    # Create the file for database files checksum
    flags.create_database_checksum_file()

    # Setup the file to log to
    setup_main_logfile()

    # Wait until change in files is detected and any updates are finished
    file_handler.wait_for_new_data()

    # Copy data directory to temporary location to prevent mid-update changes
    file_handler.isolate_data()

    # Connect to MySQL server and set the connection as a global variable
    db_handler.connect_to_database()

    # Ensure proper database and tables exist
    db_handler.setup_database()

    # Test the setup
    # db_handler.insert_test_data()

    # Read the local data from the files
    new_data = file_handler.load_isolated_data()
    Date.new_data = new_data['date']
    Card.new_data = new_data['card']
    Seller.new_data = new_data['seller']
    CardStats.new_data = new_data['card_stats']
    SaleOffer.new_data = new_data['sale_offer']

    # Read the database data from a relevant date_id onwards
    old_data = load_database_data(new_data['date'])
    Date.old_data = old_data['date']
    Card.old_data = old_data['card']
    Seller.old_data = old_data['seller']
    CardStats.old_data = old_data['card_stats']
    SaleOffer.old_data = old_data['sale_offer']

    # Prepare the data for database insertion
    Date.prepare_data()
    Card.prepare_data()
    Seller.prepare_data()
    CardStats.prepare_data()
    SaleOffer.prepare_data()

    tables = [Date, Card, Seller, CardStats, SaleOffer]

    # Iterate over every table
    for entity in tables:
        log("Currently in table: %s" % entity.name)

        # Get the relevant differences between datasets
        to_be_deleted, to_be_inserted = \
            file_handler.calculate_deltas(entity.old_data, entity.new_data)

        # Save indices of rows to delete from the database
        tbd_values = []
        for row in to_be_deleted:
            tbd_values.append(tuple(row[0]))

        # Save the data that needs to be inserted to the table
        tbi_values = []
        for row in to_be_inserted:
            tbi_values.append(tuple(row[1:]))

        if tbd_values:
            db_handler.run_delete_query(entity, tbd_values)
        if tbi_values:
            db_handler.run_insert_query(entity, tbi_values)

        log(f"Data in {entity.name} table updated.")

        r = db_handler.run_fetch_query(f"SELECT * FROM {entity.name} LIMIT 5")
        count = db_handler.run_fetch_query(f"SELECT COUNT(*) \
                                             FROM {entity.name} LIMIT 5")
        log(count.values())
        log(r.values())
        time.sleep(6)

    # Close the connection to the database
    db_handler.close_connection()

    # Update the database files checksum stored locally
    flags.save_database_checksum(config.NEW_CHECKSUM)

    # Remove temporary database data files
    file_handler.clean_up()


# Main function
if __name__ == '__main__':
    print("Started: database-manager")
    time.sleep(config.CONTAINER_DELAY)
    try:
        while True:
            main()
    except Exception as exception:
        log(exception)
        raise SystemExit from exception
