import time

from handlers.file_handler import register_change
from handlers.log_handler import log

# TODO: Figure out whether sleep is working properly, or possible alternatives
# TODO: Fill out main function

# Main function
if __name__ == '__main__':
    time.sleep(60)
    while True:
        # Wait until change in files is detected and any updates are finished
        register_change()

        # Create a connection to the database
        log("Database update here")

        # Verify the integrity of the database

        # Load the files into pandas dataframes

        # Analyze the data validity of the data

        # Analyze how the data relates to the database

        # Send and update the data in the database

        # Verify the integrity of the database

        # Close the database connection

        # End program execution
