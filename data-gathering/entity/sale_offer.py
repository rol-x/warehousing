import globals
import numpy as np
import pandas as pd
from entity.card import get_card_ID
from entity.seller import get_seller_ID
from handlers.data_handler import load_df
from handlers.log_handler import log


# Extract information about the offers from provided card soup.
def add_offers(card_page):
    '''Extract information about the offers from provided card soup.'''
    table = card_page.find("div", {"class": "table "
                                   + "article-table "
                                   + "table-striped"})
    if table is None:
        log("No offers found on page!")
        log(f'Page title:  {card_page.find("title")}')
        return

    # Get static and list info from the page
    card_name = (str(card_page.find("div", {"class": "flex-grow-1"}))
                 .split(">")[2]).split("<")[0]
    sellers_info = table.findAll("span", {"class": "seller-info d-flex "
                                          + "align-items-center"})
    seller_names = []
    for seller_info in sellers_info:
        seller_name = seller_info.find("span", {"class": "d-flex "
                                                + "has-content-centered "
                                                + "mr-1"})
        seller_names.append(seller_name)

    prices = table.findAll("span", {"class": "font-weight-bold color-primary "
                                    + "small text-right text-nowrap"})
    amounts = table.findAll("span", {"class":
                            "item-count small text-right"})
    attributes = table.findAll("div", {"class": "product-attributes col"})

    # Get the card ID (dependent on card_name and this_date_ID)
    card_ID = get_card_ID(card_name)

    # Ensure the table has proper content
    if not (len(prices) / 2) == len(amounts) \
            == len(seller_names) == len(attributes):
        log('The columns don\'t match in size!\n')
        return

    # Acquire the data row by row
    offers_dict = {"seller_ID": [], "price": [], "card_ID": [],
                   "card_condition": [], "language": [], "is_foiled": [],
                   "amount": [], "date_ID": []}
    for i in range(len(seller_names)):
        offer_attrs = []
        price = float(str(prices[2*i].string)[:-2].replace(".", "")
                      .replace(",", "."))
        amount = int(amounts[i].string)
        seller_name = seller_names[i].string

        # Get card attributes
        for attr in attributes[i].findAll("span"):
            if attr is not None:
                try:
                    offer_attrs.append(attr["data-original-title"])
                except KeyError:
                    continue
            is_foiled = False
            foil = attributes[i].find("span", {"class":
                                               "icon st_SpecialIcon mr-1"})
            if foil is not None:
                if foil["data-original-title"] == 'Foil':
                    is_foiled = True

        # Interpret the attributes
        if len(offer_attrs) >= 2:
            condition = offer_attrs[0]
            card_lang = offer_attrs[1]
        else:
            condition = np.nan
            card_lang = np.nan
            log("Incomplete card attributes!")

        # Load the entry into the dictionary
        seller_ID = get_seller_ID(seller_name)
        offers_dict['seller_ID'].append(seller_ID)
        offers_dict['price'].append(price)
        offers_dict['card_ID'].append(card_ID)
        offers_dict['card_condition'].append(condition)
        offers_dict['language'].append(card_lang)
        offers_dict['is_foiled'].append(is_foiled)
        offers_dict['amount'].append(amount)
        offers_dict['date_ID'].append(globals.this_date_ID)

    update_offers(offers_dict)


# Take new offers and rewrite the dataframe with them today.
def update_offers(offers_dict):
    '''Take new offers and rewrite the dataframe with them today.'''

    # Load and drop today's sales data for this card
    all = load_df('sale_offer')
    read = pd.DataFrame(offers_dict)
    this_card_today = all[(all['card_ID'] == read['card_ID'].values[0])
                          & (all['date_ID'] == globals.this_date_ID)]
    all.drop(this_card_today.index, inplace=True)

    # Concatenate the remaining and new offers and save to file
    new_all = pd.concat([all, read]).reset_index(drop=True).drop_duplicates()
    filename = 'sale_offer{suffix}.csv' \
        .format(suffix=f"_{globals.file_part}"
                if globals.file_part > 1 else "")
    new_all.to_csv(f'data/{filename}', ';', index=False)

    # Log task finished
    log(f"Done - {len(new_all) - len(all)} sale offers saved  (before: "
        + f"{len(this_card_today)}, total: {len(new_all)})\n\n")
