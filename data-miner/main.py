import time as tm
import warnings

import config
from services import database_service as db
from services import flags_service as flags
from services import logs_service as logs
from services import mining_service as miner
from services.logs_service import log

warnings.filterwarnings("ignore")


# Main function
def main():
    # Set the logs directory and main log file up
    logs.setup_logs()

    # Wait until new verified dataset is present
    while True:
        if flags.get_database_checksum() != flags.get_validated_checksums()[-1]:  # noqa
            log(" - New data found to be loaded into the database. Waiting 5 minutes.")  # noqa
            tm.sleep(5 * 60)
            continue

        # Connect to the database and get current date_id
        db.connect_to_database()
        log("Connected to the database.")

        while db.check_database() < 5:
            log("Waiting for the database to be ready...")
            tm.sleep(2 * 60)

        # Set current date ID
        db.set_current_date()
        log("Date ID: %s" % config.DATE_ID)

        # Create data marts
        db.create_table_last_two_weeks()
        db.create_table_offers_today()

        # Load all the tables for analysis
        date, card, seller, card_stats, sale_offer = miner.load_all()

        # Close the connection to the database
        db.close_connection()
        log("Connection closed.")

        # Perform the analysis
        miner.setup_analysis_directory()
        miner.analyze(date, card, seller, card_stats, sale_offer)

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
