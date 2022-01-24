import config
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas.io.sql as psql
import seaborn as sns
from sqlalchemy import create_engine

from services.logs_service import log

engine = create_engine('mysql://root:root@mysql_database/gathering')


def load_instant():
    with engine.connect() as conn, conn.begin():
        sale_offer = pd.read_sql_table("sale_offer", conn)
    return sale_offer


def load_sale_offer():
    chunk_size = 100000
    offset = 0
    dfs = []
    conn = engine.connect()
    while True:
        sql = "SELECT * FROM sale_offer \
            LIMIT %d OFFSET %d ORDER BY id" % (chunk_size, offset)
        dfs.append(psql.read_sql_query(sql, conn))
        offset += chunk_size
        log("Chunk appended. Total rows: %s" % offset)
        if len(dfs[-1]) < chunk_size:
            break
    full_df = pd.concat(dfs)
    return full_df

# for chunk in pd.read_sql_query
#   'SELECT * FROM %s' % table_name, engine, chunksize=5):
#    print(chunk)
