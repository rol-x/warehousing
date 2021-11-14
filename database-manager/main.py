import os
import time as tm

import config
from services import data_service as data
from services import database_service as db
from services import flags_service as flags
from services import logs_service as logs
from services.logs_service import log

# TODO: Connect to database, create empty tables if there are none
# TODO: Check local data against database contents & decide how much to update
# TODO: Verify the integrity of the database before and after the update


# Main function
def main():
    # Set the logs directory and main log file up
    logs.setup_logs()

    # Set the flags directory and database checksum file up
    flags.setup_flags()

    # Ensure at least one proper data-gathering run is completed
    data.ensure_complete_dataset()

    # Wait until new verified dataset is present
    while True:
        log(flags.calculate_checksum('./data'))
        log(flags.get_database_checksum())

        log(len(flags.calculate_checksum('./data')))
        log(len(flags.get_database_checksum()))

        log(flags.calculate_checksum('./data')
            == flags.get_database_checksum())

        # Check if there are differences between database and local files
        if flags.calculate_checksum('./data') == flags.get_database_checksum():
            log(" - Newest data already in database. Waiting 30 minutes.")
            tm.sleep(30 * 60)
            continue

        # Check if ready, validated dataset is waiting for us to register
        if flags.calculate_checksum('./data') \
                in flags.get_validated_checksums():
            log(" - Verified new data available for database update.")
            break

        # Some change in files was detected, ensure it's a proper dataset
        log(" - New data found, but is not complete. Waiting 15 minutes.")
        tm.sleep(15 * 60)

    # Copy data directory to temporary location to prevent mid-update changes
    data.isolate_data()
    log("Data isolated.")

    config.NEW_CHECKSUM = flags.calculate_checksum('./.data')
    log("Checksum: %s" % config.NEW_CHECKSUM)

    # Connect to MySQL server and set the connection as a global variable
    db.connect_to_database()
    log("Database connection established")
    db.run_query("USE gathering")

    # Read the local data from the files
    new_data = data.load_isolated_data()
    for table_name, dataframe in new_data.items():
        start = tm.time()
        data.update_table(table_name, dataframe)
        log(f"Updated {table_name} in {round(tm.time() - start, 3)} seconds.")

    for table_name in new_data.keys():
        table = data.select_table(table_name)
        log("Table: %s" % table_name)
        log(table.info())
        log(table.describe())
        tm.sleep(3)

    tm.sleep(10)
    db.test()

    # Close the connection to the database
    db.close_connection()
    log("Connection closed.")

    # Update the database files checksum stored locally
    flags.save_database_checksum(config.NEW_CHECKSUM)
    log("Checksum: %s set." % config.NEW_CHECKSUM)

    # Remove temporary database data files
    data.clean_up()
    log("Cleaned up.")


# Main function
if __name__ == '__main__':
    print("Started: database-manager")
    tm.sleep(config.CONTAINER_DELAY)
    try:
        while True:
            main()
    except Exception as exception:
        log(exception)
        log(" - Container will restart in 10 minutes.")
        tm.sleep(10 * 60)
        raise SystemExit from exception
