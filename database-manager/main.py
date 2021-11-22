import time as tm

import config
from services import data_service as data
from services import database_service as db
from services import flags_service as flags
from services import logs_service as logs
from services.logs_service import log

# TODO: Fix trying to expand the page when it's not ready (and waiting 20 s.)


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
        log(f"Local files checksum: {flags.calculate_checksum('./data')}")
        log(f"Database checksum: {flags.get_database_checksum()}")

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

    # Read the local data from the files
    new_data = data.load_isolated_data()
    for table_name, dataframe in new_data.items():
        start = tm.time()
        data.update_table(dataframe, table_name)
        log(f"Updated {table_name} in {round(tm.time() - start, 3)} seconds.")

    for table_name in new_data.keys():
        table = data.select_table(table_name)
        log("Table selected: %s" % table_name)

    # Generate new tables
    seller = data.select_table('seller')
    for s_type in seller['type'].unique():
        data.update_table(seller[seller['type'] == s_type], s_type)

    card = data.select_table('card')
    for rarity in card['rarity'].unique():
        data.update_table(card[card['rarity'] == rarity], rarity)

    offers = data.select_table('sale_offer')
    for date_id in offers['date_id'].unique():
        data.update_table(offers[offers['date_id'] == date_id],
                          'sale_offer_' + str(date_id))

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
