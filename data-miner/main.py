import time as tm

import config
from services import database_service as db
from services import flags_service as flags
from services import logs_service as logs
from services.logs_service import log

# CREATE THE VIEWS FROM server.js HERE


# Main function
def main():
    # Set the logs directory and main log file up
    logs.setup_logs()

    # Wait until new verified dataset is present
    while True:
        if flags.get_database_checksum() != flags.get_validated_checksums()[-1]:  # noqa
            log(" - New data found to be loaded into the database. Waiting 10 minutes.")  # noqa
            tm.sleep(10 * 60)
            continue

        # Connect to the database and get current date_id
        db.connect_to_database()
        log("Connected to the database.")

        # Set current date ID
        db.set_current_date()
        log("Date ID: %s" % config.DATE_ID)

        # Create data marts
        db.create_view_V1()
        db.create_view_offers_today()

        # Close the connection to the database
        db.close_connection()
        log("Connection closed.")

        log("- Job is done. Waiting 60 minutes.")
        tm.sleep(60 * 60)


# Main function
if __name__ == '__main__':
    print("Started: data-miner")
    tm.sleep(config.CONTAINER_DELAY)
    try:
        while True:
            main()
    except Exception as exception:
        log(exception)
        log(" - Container will restart in 30 minutes.")
        tm.sleep(30 * 60)
        raise SystemExit from exception
