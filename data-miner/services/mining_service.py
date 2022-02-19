import os

import config
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from services.logs_service import log


# Create directory for graphs and tables
def setup_analysis_directory():
    if not os.path.exists('./analysis'):
        os.mkdir('./analysis')

    if not os.path.exists('./analysis/cards'):
        os.mkdir('./analysis/cards')


def load_table(table_name: str):
    df = pd.DataFrame()
    try:
        cursor = config.DB_CONN.cursor()
        if config.OVER_10M and table_name == 'sale_offer':
            raise Exception("Tables over 10M rows need different approach.")
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


# Load all dataframes
def load_all():
    date = load_table("date")
    card = load_table("card")
    seller = load_table("seller")
    card_stats = load_table("card_stats")
    sale_offer = load_table("sale_offer")
    return date, card, seller, card_stats, sale_offer


# Perform the analysis and draw conclusions
def analyze(date, card, seller, card_stats, sale_offer):

    # Generate transformed date dataframe with datetime attribute
    date_t = pd.concat([date['id'], pd.to_datetime(date.drop(['id', 'weekday'], axis=1)), date['weekday']], axis=1).rename(columns={0: "date"})  # noqa
    log("Done: Date table with datetime")

    # Plot how many cards are in each rarity category
    save_cards_rarity(card)
    log("Done: Cards rarity")

    # Compare the amount of seller addresses and number of sellers per type
    save_seller_addresses(seller)
    log("Done: Sellers number and addresses")

    # Present the site's populatiry over the years
    save_popularity(seller)
    log("Done: Site popularity")

    # Show new sellers per year divided by type
    save_seller_types_per_year(seller)
    log("Done: Seller types per year")

    # Order the countries by the most sellers
    save_seller_countries(seller)
    log("Done: Seller countries")

    # Prepare helper tables with card stats and prices
    valid = card_stats.groupby('date_id').count()['card_id'] == 264
    valid_card_stats = card_stats[card_stats['date_id'].isin(valid.index[valid == True])]  # noqa
    prices = valid_card_stats.merge(date_t, left_on='date_id', right_on='id')
    items = prices[['date', 'available_items']].groupby('date').sum()
    prices = prices[['date', 'price_from', 'monthly_avg', 'weekly_avg', 'daily_avg']].groupby('date').mean()
    prices['total_average'] = prices['daily_avg'].mean()
    log("Done: Valid card stats and prices tables")

    # Plot the changes in price stats
    save_price_chart(prices)
    log("Done: Price chart")

    # Print the available items amount over time
    save_available_items(items)
    log("Done: Available items")

    # Calculate new factor: market rarity
    cards = card_stats.merge(date_t, left_on='date_id', right_on='id')[['card_id', 'price_from', 'daily_avg', 'weekly_avg', 'monthly_avg', 'available_items']]
    cards = cards.astype({'card_id': 'int32', 'daily_avg': 'float', 'weekly_avg': 'float', 'monthly_avg': 'float', 'available_items': 'int32'})
    cards = cards.groupby(['card_id']).mean()
    cards['market_rarity'] = cards['available_items'].mean() / cards['available_items']
    log("Done: Market rarity")

    # Visualize the correlations between attributes
    save_heatmap(cards)
    log("Done: Correlation heatmap")

    # Narrow the search to selected features
    good_offers = sale_offer[((sale_offer['card_condition'] == 'NM')
                             | (sale_offer['card_condition'] == 'MT')
                             | (sale_offer['card_condition'] == 'EX'))
                             & (sale_offer['card_language'] == 'English')]
    log("Done: Good offers helper table")

    for card_id in card.id:
        save_card_best_prices(card_id, good_offers, date_t)
        log("Done: Card " + str(card_id))


def save_cards_rarity(card):
    plt.figure(figsize=(10, 5))
    plt.bar(card.groupby('rarity').count()['id'].index, card.groupby('rarity').count()['id'])
    plt.savefig('analysis/card_rarity.jpg', bbox_inches='tight')


def save_seller_addresses(seller):
    seller_types = seller.groupby('type').count()[['id', 'address']].sort_values(by='id', ascending=False)
    X_axis = np.arange(len(seller_types.index))
    plt.figure(figsize=(8, 4))
    plt.bar(X_axis - 0.2, seller_types.address, 0.4, label = 'With address')
    plt.bar(X_axis + 0.2, seller_types.id, 0.4, label = 'All users')
    plt.xticks(X_axis, seller_types.index)
    plt.xlabel("Types of sellers")
    plt.ylabel("Number of sellers / addresses")
    plt.title("Amount of addresses vs types of sellers")
    plt.legend()
    plt.savefig('analysis/seller_addresses.jpg')


def save_popularity(seller):
    f, axs = plt.subplots(1, 2, figsize=(18, 6))
    seller.groupby('member_since').count()['id'].plot(ax=axs[0], kind='bar')
    seller.groupby('member_since').count()['id'].cumsum().plot(ax=axs[1])
    plt.savefig('analysis/site_popularity.jpg')


def save_seller_types_per_year(seller):
    f, axes = plt.subplots(2, 1, figsize=(15, 10))
    sns.countplot(x=seller['type'], hue=seller['member_since'], ax=axes[0])
    axes[0].get_legend().remove()
    non_private = seller[seller['type'] != 'Private']
    sns.countplot(x=non_private['type'], hue=non_private['member_since'], ax=axes[1])
    axes[1].get_legend().remove()
    plt.savefig('analysis/seller_type_popularity.jpg')


def save_seller_countries(seller):
    plt.figure(figsize=(16, 8))
    seller.groupby('country').count().sort_values(by='id', ascending=False)['id'].plot(kind='bar')
    plt.savefig('analysis/seller_countries.jpg')


def save_price_chart(prices):
    plt.figure(figsize=(14, 8))
    ax = sns.lineplot(
        x='date',
        y='value',
        hue='variable',
        data=pd.melt(prices.reset_index().drop(['price_from'], axis=1), 'date'))  # noqa
    ax.lines[3].set_linestyle("--")
    plt.savefig('analysis/price_chart.jpg')


def save_price_distributions(prices):
    log(prices.dtypes)
    p = sns.kdeplot(data=prices[['daily_avg', 'monthly_avg', 'price_from']])
    p.figure.set_size_inches(15, 5)
    plt.savefig('analysis/price_distribution.jpg')


def save_available_items(items):
    plt.figure(figsize=(14, 8))
    ax = sns.lineplot(data=items['available_items'])
    plt.savefig('analysis/available_items.jpg')


def save_heatmap(price_supply):
    sns.heatmap(price_supply.corr(), cmap='coolwarm')
    plt.savefig('analysis/supply_heatmap.jpg')


def save_card_best_prices(card_id, good_offers, date_t):
    card_price_history = good_offers[good_offers['card_id'] == card_id].groupby('date_id').mean().drop(['id', 'seller_id', 'card_id'], axis=1)
    top50 = card_price_history.merge(date_t, left_on='date_id', right_on='id').sort_values(by='price').head(50).reset_index(drop=True)
    top30 = top50.head(30)
    top10 = top50.head(10)
    plt.figure(figsize=(9, 5))
    plt.xticks(rotation=20)
    plt.scatter(top50.date, top50.price)
    plt.scatter(top30.date, top30.price, s=30)
    plt.scatter(top10.date, top10.price, s=50)
    plt.savefig('analysis/cards/card_' + str(card_id) + ".jpg")
