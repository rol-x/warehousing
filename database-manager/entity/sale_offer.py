from services.logs_service import log

name = 'sale_offer'
headers = "id, price, card_id, card_condition, \
            language, is_foiled, amount, date_id"
args = "%s, %s, %s, %s, %s, %s, %s, %s"
static = False

new_data = None
old_data = None


def prepare_data():
    if new_data is None:
        raise Exception("No data after loading")
    log(" === [SALE OFFER] New data === ")
    log(new_data.describe())
    log(new_data.info())
    log(new_data.head())
    log(new_data.tail())
    log(" === [SALE OFFER] Old data === ")
    log(old_data.describe())
    log(old_data.info())
    log(old_data.head())
    log(old_data.tail())
