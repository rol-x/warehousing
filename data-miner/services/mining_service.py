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


def analyze(date, card, seller, card_stats, sale_offer):
    date_t = pd.concat([date['id'], pd.to_datetime(date.drop(['id', 'weekday'], axis=1)), date['weekday']], axis=1).rename(columns={0: "date"})  # noqa
    log(card.groupby('rarity').count())
    log(seller.groupby('type').count()[['id', 'address']].sort_values(by='id', ascending=False))  # noqa
    log(seller.groupby('type').mean().sort_values(by='member_since'))

    f, axs = plt.subplots(1, 2, figsize=(18, 6))
    seller.groupby('member_since').count()['id'].plot(ax=axs[0], kind='bar')
    seller.groupby('member_since').count()['id'].cumsum().plot(ax=axs[1])

    f2, axes2 = plt.subplots(2, 1, figsize=(15, 10))
    sns.countplot(x=seller['type'], hue=seller['member_since'], ax=axes2[0])
    axes2[0].get_legend().remove()

    non_private = seller[seller['type'] != 'Private']
    sns.countplot(x=non_private['type'], hue=non_private['member_since'], ax=axes2[1])  # noqa
    axes2[1].get_legend().remove()

    plt.figure(figsize=(16, 8))
    seller.groupby('country').count().sort_values(by='id', ascending=False)['id'].plot(kind='bar')  # noqa

    log(seller[seller['type'] == 'Private'].groupby('country').count().sort_values(by='id', ascending=False).head(10))  # noqa
    log(seller[seller['type'] == 'Professional'].groupby('country').count().sort_values(by='id', ascending=False).head(10))  # noqa
    log(seller[seller['type'] == 'Powerseller'].groupby('country').count().sort_values(by='id', ascending=False).head(10))  # noqa

    valid = card_stats.groupby('date_id').count()['id'] == 264
    valid_card_stats = card_stats[card_stats['date_id'].isin(valid.index[valid == True])]  # noqa
    prices = pd.merge(valid_card_stats, date_t,
                      left_on='date_id', right_on='id') \
        .groupby('date').mean()[['price_from', 'monthly_avg', 'weekly_avg', 'daily_avg', 'available_items']]  # noqa
    prices['total_average'] = prices['daily_avg'].mean()
    plt.figure(figsize=(14, 8))
    ax = sns.lineplot(
        x='date',
        y='value',
        hue='variable',
        data=pd.melt(prices.reset_index().drop(['available_items', 'price_from'], axis=1), 'date'))  # noqa
    ax.lines[3].set_linestyle("--")
    plt.show()

    p = sns.kdeplot(
        data=prices[['price_from', 'daily_avg', 'weekly_avg', 'monthly_avg']])
    p.figure.set_size_inches(15, 5)

    plt.figure(figsize=(14, 8))
    ax = sns.lineplot(data=prices['available_items'])
    plt.show()

    p1 = pd.merge(card_stats, card, left_on='card_id', right_on='id')
    stats = pd.merge(p1, date_t, left_on='date_id', right_on='id') \
        .drop(['id_x', 'date_id', 'id_y', 'expansion', 'id', 'weekday'], axis=1).set_index('date')  # noqa

    cards = stats.reset_index().groupby(['name']).mean()
    log(cards.sort_values(by='available_items', ascending=False))

    price_supply = cards.drop('card_id', axis=1)
    price_supply['av_items_rel'] = price_supply['available_items'] / \
        price_supply['available_items'].sum()
    price_supply['market_rarity'] = (1 / 264) / price_supply['av_items_rel']
    price_supply.sort_values(by='market_rarity').corr()

    sns.heatmap(price_supply.sort_values(
        by='market_rarity').corr(), cmap='coolwarm')

    log(cards.sort_values(by='daily_avg', ascending=False))

    log(cards[cards['price_from'] < 0.10])

    log(sale_offer.groupby('date_id').count())

    log(sale_offer['card_condition'].value_counts())

    decode = {"NM": "Near Mint", "EX": "Excellent", "MT": "Mint",
              "GD": "Good", "LP": "Light Played", "PO": "Poor", "PL": "Played"}
    sale_offer['card_condition'] = sale_offer['card_condition'].apply(
        lambda x: decode[x] if decode.get(x) else x)

    good_offers = sale_offer[((sale_offer['card_condition'] == 'Near Mint')
                             | (sale_offer['card_condition'] == 'Mint')
                             | (sale_offer['card_condition'] == 'Excellent'))
                             & (sale_offer['card_language'] == 'English')]

    card_id = 186
    card_price_history = good_offers[good_offers['card_id'] == card_id] \
        .groupby('date_id').mean().drop(['id', 'seller_id', 'card_id'], axis=1)
    log(card_price_history.sort_values(by='price'))
