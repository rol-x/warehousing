import time

import config
from handlers import db_handler, file_handler
from handlers.log_handler import log, setup_logging

# TODO: Write simple database connection, create empty tables if there are none
# TODO: Check local data against database contents & decide how much to update
# TODO: Write database updating section for inserting and updating data
# TODO: Verify the integrity of the database before and after the update


# Main function
def main():
    # Set the current run log filename
    setup_logging()

    # Create the file for database files checksum
    file_handler.setup_database_checksum()

    # Wait until change in files is detected and any updates are finished
    file_handler.wait_for_new_data()

    # Copy data directory to temporary location to prevent mid-update changes
    file_handler.isolate_data()

    # Test query
    cursor = db_handler.connect_to_database()
    cursor.execute("SHOW DATABASES")
    for row in cursor:
        log(row)

    # Something
    db_handler.run_update()

    # Close the connection to the database
    cursor.close()

    # Remove temporary database data files
    file_handler.clean_up()

    # Update the database files checksum stored locally
    file_handler.save_database_checksum(config.NEW_CHECKSUM)


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
