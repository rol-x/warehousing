import time

from handlers.file_handler import generate_data_hash, save_hash
from handlers.log_handler import log


# Detect changes in data directory based on calculated checksums
def register_change():
    last_hash = open("data-checksum.sha1", "r").readline()
    this_hash = generate_data_hash()

    # Every thirty minutes check whether the two hashes differ
    while(this_hash == last_hash):
        time.sleep(60 * 30)
        this_hash = generate_data_hash()

    # Every thirty seconds check whether the update flag is active
    log("Change in data files detected")
    while(open("./data/update_flag", "r").readline() == '1'):
        time.sleep(30)

    # On finished update or data migration the files are static
    this_hash = generate_data_hash()
    log("Changes ready to commence")
    log("Old hash: " + last_hash)
    log("New hash: " + this_hash)
    log("Saving new hash to the file")
    save_hash(this_hash)
    return


# TODO: Figure out whether sleep is working properly, or possible alternatives

# Main function
if __name__ == '__main__':
    time.sleep(60)
    while True:
        register_change()
        log("Database update here")
