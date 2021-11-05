import time as tm

import config
from entity import card as Card
from entity import card_stats as CardStats
from entity import date as Date
from entity import sale_offer as SaleOffer
from entity import seller as Seller
from services import data_service as data
from services import db_service as db
from services import flags_service as flags
from services import logs_service as logs
from services.logs_service import log

# TODO: Write simple database connection, create empty tables if there are none
# TODO: Check local data against database contents & decide how much to update
# TODO: Write database updating section for inserting and updating data
# TODO: Verify the integrity of the database before and after the update
# TODO: Complete main logging and run logging


def load_database_data(new_dates):

    # Read the date, card and seller data from the database
    date = data.load_database_frame(db.table_content('date'))
    card = data.load_database_frame(db.table_content('card'))
    seller = data.load_database_frame(db.table_content('seller'))

    # Determine which dates are new to the database
    update_dates = data.compare_dates(date, new_dates)
    log("Update dates: ")
    log(update_dates)

    # Load the relevant slices from the database
    card_stats_slice = data.load_database_frame(
        db.table_content_since('card_stats', update_dates))
    sale_offer_slice = data.load_database_frame(
        db.table_content_since('sale_offer', update_dates))

    return {"date": date, "card": card, "seller": seller,
            "card_stats": card_stats_slice, "sale_offer": sale_offer_slice}


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
    config.NEW_CHECKSUM = flags.calculate_checksum('./.data')
    log("Checksum: %s" % config.NEW_CHECKSUM)

    # Connect to MySQL server and set the connection as a global variable
    db.connect_to_database()

    # Ensure proper database and tables exist
    db.setup_database()

    # Test the setup
    # db_service.insert_test_data()

    # Read the local data from the files
    new_data = data.load_isolated_data()
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
            data.calculate_deltas(entity.old_data, entity.new_data)

        # Save indices of rows to delete from the database
        tbd_values = []
        for row in to_be_deleted:
            tbd_values.append(tuple(row[0]))

        # Save the data that needs to be inserted to the table
        tbi_values = []
        for row in to_be_inserted:
            tbi_values.append(tuple(row[1:]))

        if tbd_values:
            db.run_delete_query(entity, tbd_values)
        if tbi_values:
            db.run_insert_query(entity, tbi_values)

        log(f"Data in {entity.name} table updated.")

        r = db.run_fetch_query(f"SELECT * FROM {entity.name} LIMIT 5")
        count = db.run_fetch_query(f"SELECT COUNT(*) \
                                             FROM {entity.name} LIMIT 5")
        log(count.values())
        log(r.values())
        tm.sleep(6)

    # Close the connection to the database
    db.close_connection()

    # Update the database files checksum stored locally
    flags.save_database_checksum(config.NEW_CHECKSUM)

    # Remove temporary database data files
    data.clean_up()


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
