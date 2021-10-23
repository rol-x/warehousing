import time

import config
import services.flags_service as flags
from handlers import db_handler, file_handler
from services.logs_service import log, setup_logs

# TODO: Write simple database connection, create empty tables if there are none
# TODO: Check local data against database contents & decide how much to update
# TODO: Write database updating section for inserting and updating data
# TODO: Verify the integrity of the database before and after the update


# Main function
def main():
    # Set the current run log filename
    setup_logs()

    # Create the file for database files checksum
    flags.create_database_checksum_file()

    # Wait until change in files is detected and any updates are finished
    file_handler.wait_for_new_data()

    # Copy data directory to temporary location to prevent mid-update changes
    file_handler.isolate_data()

    # Connect to MySQL server and set the connection as a global variable
    db_handler.connect_to_database()

    # Ensure proper database and tables exist
    db_handler.setup_database()

    # Take the new data and load the differences into the database
    db_handler.run_update()

    # Close the connection to the database
    db_handler.close_connection()

    # Update the database files checksum stored locally
    flags.save_database_checksum(config.NEW_CHECKSUM)
    log("New database checksum saved: %s" % config.NEW_CHECKSUM)

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
