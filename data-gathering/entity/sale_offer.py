import config
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
        seller_names.append(seller_info.find("span", {"class": "d-flex "
                                             + "has-content-centered "
                                             + "mr-1"}))

    prices = table.findAll("span", {"class": "font-weight-bold color-primary "
                                    + "small text-right text-nowrap"})
    amounts = table.findAll("span", {"class":
                            "item-count small text-right"})
    attributes = table.findAll("div", {"class": "product-attributes col"})

    # Ensure the table has proper content
    if not (len(prices) / 2) == len(amounts) \
            == len(seller_names) == len(attributes):
        log('The columns don\'t match in size!\n')
        return

    # Acquire the data row by row
    offers_dict = {"seller_ID": [], "price": [], "card_ID": [],
                   "card_condition": [], "language": [], "is_foiled": [],
                   "amount": [], "date_ID": []}
    for i, seller_name in enumerate(seller_names):
        card_attrs = []
        price = float(str(prices[2*i].string)[:-2].replace(".", "")
                      .replace(",", "."))

        # Get card attributes
        for attr in attributes[i].findAll("span"):
            if attr is not None:
                try:
                    card_attrs.append(attr["data-original-title"])
                except KeyError:
                    continue
            is_foiled = False
            foil = attributes[i].find("span", {"class":
                                               "icon st_SpecialIcon mr-1"})
            if foil is not None:
                if foil["data-original-title"] == 'Foil':
                    is_foiled = True

        # Interpret the attributes
        if len(card_attrs) < 2:
            card_attrs = ['', '']
            log("Incomplete card attributes!")

        # Load the entry into the dictionary
        offers_dict['seller_ID'].append(get_seller_ID(seller_name.string))
        offers_dict['price'].append(price)
        offers_dict['card_ID'].append(get_card_ID(card_name))
        offers_dict['card_condition'].append(card_attrs[0])
        offers_dict['language'].append(card_attrs[1])
        offers_dict['is_foiled'].append(is_foiled)
        offers_dict['amount'].append(int(amounts[i].string))
        offers_dict['date_ID'].append(config.THIS_DATE_ID)

        for key, value in offers_dict.items():
            if len(value) == 0:
                log("Faulty offer set! No entrys for key: " + key)
                return

    update_sale_offers(offers_dict)


# Take the gathered data and adjoin it to the local files
def update_sale_offers(offers_dict):

    # Load and drop today's sales data for this card
    saved = load_df('sale_offer')
    scraped = pd.DataFrame(offers_dict)
    this_card_today = saved[(saved['card_ID'] == scraped['card_ID'].values[0])
                            & (saved['date_ID'] == config.THIS_DATE_ID)]
    saved.drop(this_card_today.index, inplace=True)

    # Concatenate the remaining and new offers and save to file
    data = pd.concat([saved, scraped]).reset_index(drop=True).drop_duplicates()
    filename = f'sale_offer_{config.FILE_PART}.csv' if config.FILE_PART > 1 \
        else 'sale_offer.csv'
    data.to_csv(f'./data/{filename}', ';', index=False)

    # Log task finished
    log(f"Done - {len(data) - len(saved)} sale offers saved  (before: "
        + f"{len(this_card_today)}, total: {len(data)})\n\n")
