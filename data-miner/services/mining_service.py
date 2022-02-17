import config
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from services.logs_service import log


def load_table(table_name: str):
    df = pd.DataFrame()
    try:
        cursor = config.DB_CONN.cursor()
        if table_name == 'sale_offer':
            raise Exception("Table over 10M rows needs different approach.")
        select = f"SELECT * FROM {table_name}"
        cursor.execute(select)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    except Exception as exception:
        log(f"SQL query failed. {exception}")
        log("Reading from local storage...")
        df = pd.read_csv(f'./data/{table_name}.csv',
                         compression='gzip', sep=';', encoding="utf-8")
    finally:
        log("Successfully read table " + table_name)
        return df


def load_all():
    date = load_table("date")
    card = load_table("card")
    seller = load_table("seller")
    card_stats = load_table("card_stats")
    sale_offer = load_table("sale_offer")
    return date, card, seller, card_stats, sale_offer
