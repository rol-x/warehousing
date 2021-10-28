from services.logs_service import log

name = 'sale_offer'
headers = "seller_id, price, card_id, card_condition, \
            language, is_foiled, amount, date_id"
args = "%s, %s, %s, %s, %s, %s, %s, %s"
static = False

new_data = None
old_data = None


def prepare_data():
    log("Preparing data inside %s module" % name)
    if new_data is None:
        raise Exception("No data after loading")
    log(new_data.columns)
    for col in new_data.columns:
        log(type(new_data[col]))
    log("----- ^ N E W ^ -------- v O L D v -----")
    log(old_data.columns)
    for col in old_data.columns:
        log(type(old_data[col]))
