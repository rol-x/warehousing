from time import sleep

from handlers.file_handler import generate_data_hash, save_hash
from handlers.log_handler import log


# Detect changes in data directory
def run_checker():
    old_hash = open("data-checksum.sha1", "r").readline()
    new_hash = generate_data_hash()
    while(new_hash == old_hash):
        sleep(3600 * 1000)
        new_hash = generate_data_hash()
    log("File change detected")
    log("Old hash: " + old_hash)
    log("New hash: " + new_hash)
    log("Saving new hash to the file")
    save_hash(new_hash)
    return


# Main function
if __name__ == '__main__':
    run_checker()
    data_hash = generate_data_hash()
