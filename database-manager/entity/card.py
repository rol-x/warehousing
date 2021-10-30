from services.logs_service import log

name = 'card'
headers = "name, expansion_name, rarity"
args = "%s, %s, %s"
static = True

new_data = None
old_data = None


def prepare_data():
    if new_data is None:
        raise Exception("No data after loading")
    log(" === [CARD] New data === ")
    log(new_data.describe())
    log(new_data.info())
    log(new_data.head())
    log(new_data.tail())
    log(" === [CARD] Old data === ")
    log(old_data.describe())
    log(old_data.info())
    log(old_data.head())
    log(old_data.tail())
