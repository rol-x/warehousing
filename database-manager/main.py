import time

from handlers.file_handler import register_change
from handlers.log_handler import log, setup_logging

# TODO: Write simple database connection, create empty tables if there are none
# TODO: Check local data against database contents & decide how much to update
# TODO: Write database updating section for inserting and updating data
# TODO: Verify the integrity of the database before and after the update


# Main function
def main():
    # Set the current run log filename
    setup_logging()

    # Wait until change in files is detected and any updates are finished
    register_change()

    # Create a connection to the database
    log("Database update here")


# Main function
if __name__ == '__main__':
    time.sleep(20)
    try:
        while True:
            main()
    except Exception as exception:
        log(exception)
        raise SystemExit
