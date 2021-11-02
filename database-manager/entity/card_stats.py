from services.logs_service import log

name = 'card_stats'
headers = "card_id, price_from, monthly_avg, weekly_avg, \
            daily_avg, available_items, date_id"
args = "%s, %s, %s, %s, %s, %s, %s"
static = False

new_data = None
old_data = None


def prepare_data():
    if new_data is None:
        raise Exception("No data after loading")
    log(" === [CARD STATS] New data === ")
    log(new_data.describe())
    log(new_data.info())
    log(new_data.head())
    log(new_data.tail())
    log(" === [CARD STATS] Old data === ")
    log(old_data.describe())
    log(old_data.info())
    log(old_data.head())
    log(old_data.tail())
