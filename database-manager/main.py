import time

import config
import services.flags_service as flags
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
    db_handler.insert_test_data()

    # Read the local data from the files
    new_data = file_handler.load_isolated_data()

    # Read the database data from a relevant date_id onwards
    old_data = load_database_data(new_data['date'])

    # Iterate over every table
    for entity in new_data.keys:

        # Get the relevant differences between datasets
        to_be_deleted, to_be_inserted = \
            file_handler.calculate_deltas(old_data.get(entity),
                                          new_data.get(entity))

    # Find and drop the rows to delete by inspecting the index of TBD rows
    for row in to_be_deleted:
        db_handler.remove_row(entity, row[0])

    # Insert all the missing data
    for row in to_be_inserted:
        db_handler.insert_row(entity, row[1:])

    time.sleep(20)

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
