from handlers.data_handler import load_df
from services.logs_service import log


# Extract information about a seller from provided soup.
def add_seller(seller_soup):
    '''Extract information about a seller from provided soup.'''

    # Get rows from the seller information table on page
    seller_name = seller_soup.find("h1")

    # User not loaded correctly
    if seller_name is None:
        return False

    # Seller name
    seller_name = str(seller_name.string)

    # Seller ID
    seller_df = load_df('seller')
    seller_ID = len(seller_df.index) + 1

    # Type
    s_type = seller_soup.find("span",
                              {"class":
                               "ml-2 personalInfo-bold"}).string

    # Member since
    member_since = seller_soup.find("span",
                                    {"class": "ml-1 "
                                     + "personalInfo-light "
                                     + "d-none "
                                     + "d-md-block"}).string.split(' ')[-1]

    # Country
    country = seller_soup.find("div",
                               {"class":
                                "col-12 col-md-6"}) \
        .find("span")["data-original-title"]

    # Address
    address_div = seller_soup.findAll("div", {"class": "d-flex "
                                              + "align-items-center "
                                              + "justify-content-start "
                                              + "flex-wrap "
                                              + "personalInfo "
                                              + "col-8 "
                                              + "col-md-9"})[-1] \
        .findAll("p")
    address = ''
    for line in address_div:
        address = address + line.string + ', '
    address = address.strip(', ')
    if address == country:
        address = ''

    # Logging
    log(f"Seller added:  {seller_name} [{seller_ID}]")

    # Writing
    with open('./data/seller.csv', 'a', encoding="utf-8") as seller_csv:
        seller_csv.write(str(seller_ID) + ';')
        seller_csv.write(seller_name + ';')
        seller_csv.write(s_type + ';')
        seller_csv.write(member_since + ';')
        seller_csv.write(country + ';')
        seller_csv.write(address + '\n')

    return True


# Return a seller ID given its name.
def get_seller_ID(seller_name):
    '''Return a seller ID given its name.'''
    seller_df = load_df('seller')
    if seller_df is None:
        return -1

    this_seller = seller_df[(seller_df['seller_name'] == seller_name)]

    if len(this_seller) == 0:
        return -1

    return int(this_seller['seller_ID'].values[0])


# Return a set of all sellers found in a card page.
def get_seller_names(card_soup):
    '''Return a set of all sellers found in a card page.'''
    names = set(map(lambda x: str(x.find("a").string
                                  if x.find("a") is not None
                                  else ""),
                    card_soup.findAll("span", {"class": "d-flex "
                                      + "has-content-centered " + "mr-1"})))
    names.remove('')
    return names
