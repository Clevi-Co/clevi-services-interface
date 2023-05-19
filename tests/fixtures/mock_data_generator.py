import uuid
from db_interface.items import LocationItem, ProductItem, GeoPoint, StoreItem, ProductStoreDataItem
from scrapy.exporters import JsonLinesItemExporter
from faker import Faker
import random
from datetime import datetime
import json
import sys
import os
# from functools import partial
# from geopy import Nominatim
# sys.path.append(os.getcwd())
# sys.path.append(f'{os.getcwd()}/scrapy_celery_worker')

fake = Faker('it_IT')
ALL_MARKETS = ("carrefour", "mercato", "esselunga",
               "conad", "pam", "lidl", "crai")
ALL_UNITS = (("g", "/kg"), ("ml", "/l"))
OUTPUT_PATH = os.getcwd()

product_names: list[str] = []
product_variants: list[str] = []
product_categories: dict[dict[list[str]]] = {}


def generate_geo_point() -> GeoPoint:
    address: list[str] = [x.strip()
                          for x in fake.address().replace('\n', ',').split(',')]
    address[1] = address[1].split(' ')[0]  # remove eventual appartment number
    # address: list[str] = [x.strip() for x in fake.address().replace('\n', ',').split(',')]

    return GeoPoint(
        postal_code=address[-2],
        city=address[-1].split('(')[0].strip(),
        long=str(fake.longitude()),
        lat=str(fake.latitude()),
        address=' '.join(address[0:-2]),
        state_code=address[-1].split('(')[1].split(')')[0].strip(),
        country_code=fake.current_country_code()
    )


def generate_location_item(postal_codes: list[str], market: str, store_ids: list[str]) -> LocationItem:
    postal_codes.sort()
    markets = {
        market: store_ids
    }
    return LocationItem(
        postal_codes=postal_codes,
        markets=markets
    )


def generate_product_item(market: str, id=None) -> ProductItem:
    # if len(product_names) == 0 or len(product_variants) == 0:
    # inizialize_lists()

    brand = fake.company()
    unit_value = random.randint(1, 1000)
    unit_text = random.choice(ALL_UNITS)[0]
    description = f"{brand} - {random.choice(product_names)} {random.choice(product_variants)} - {unit_value}{unit_text}"
    categories = generate_categories()
    if id is None:
        code = uuid.uuid4().hex
        _id = f"{code}_{market}"
    else:
        code = id.split('_')[0]
        _id = id
    return ProductItem(
        _id=_id,
        code=code,
        ean=code,
        description=description,
        market=market,
        brand=brand,
        unit_value=unit_value,
        unit_text=unit_text,
        image_urls=[fake.image_url()],
        categories=categories,
        sales_denomination=fake.sentence(nb_words=3),
    )


def generate_categories():
    # if len(product_categories) == 0:
    # inizialize_lists()

    lvl1 = random.choice(list(product_categories.keys()))
    lvl2 = random.choice(list(product_categories[lvl1].keys()))
    lvl3 = random.choice(product_categories[lvl1][lvl2])
    return [lvl3, lvl2, lvl1]


def generate_store_item(market: str) -> StoreItem:
    # products_data = {}
    # store_products: list[ProductItem] = random.sample(
    #     products, random.randint(100 if len(products) > 100 else 1, len(products)))

    # for product in store_products:
    #     price = random.randint(50, 3000)
    #     discounted_price = random.randint(25, price) / 100
    #     price /= 100
    #     discount_rate = (1 - (discounted_price / price)) * 100
    #     label_value = discounted_price / (product.unit_value / 1000)
    #     label_text = [x for x in ALL_UNITS if x[0] == product.unit_text][0][1]

    #     product_to_add = ProductStoreData(
    #         _id=product._id,
    #         code=product.code,
    #         market=product.market,
    #         price=price,
    #         discounted_price=discounted_price,
    #         discount_rate=round(discount_rate, 2),
    #         label=f"{round(label_value, 2)}{label_text}",
    #         product_page_uri=fake.uri()
    #     )
    #     products_data[product._id] = product_to_add

    geo_point = generate_geo_point()
    store_id = random.randint(0, 99999)
    service = random.choice(["delivery", "pickup"])
    _id = f"{store_id}_{market}_{service}"

    return StoreItem(
        _id=_id,
        store_id=store_id,
        name=f"{market} - {geo_point.city} - {geo_point.address}",
        market=market,
        service=service,
        scrape_parameters={'_id': _id, 'store_id': store_id},
        geo_point=geo_point,
    )


def generate_product_store_data_item(store_universal_id: str, store_id: str, market: str) -> ProductStoreDataItem:
    code = uuid.uuid4().hex
    price = random.randint(50, 3000)
    discounted_price = random.randint(25, price) / 100
    price /= 100
    discount_rate = (1 - (discounted_price / price)) * 100
    unit_value = random.randint(1, 1000)
    label_value = discounted_price / (unit_value / 1000)
    product_page_uri = fake.uri()
    product_id = f"{str(code)}_{str(market)}"
    # all the data that are needed by other scraping modes of the same market
    scrape_parameters = {
        "_id": code,
        "product_page_uri": product_page_uri
    }

    """
    code = product["code"]
    price = product["price"]
    discounted_price = product["dicounted_price"]
    discount_rate = product["discounted_price"]
    label_value = product["label_value"]
    product_page_uri = product["uri"]
    product_id = product["id"]
    """

    return ProductStoreDataItem(
        code=code,
        store_universal_id=store_universal_id,
        store_id=store_id,
        market=market,
        price=round(price, 2),
        discounted_price=round(discounted_price, 2),
        discount_rate=round(discount_rate, 2),
        label=round(label_value, 2),
        product_page_uri=product_page_uri,
        scrape_parameters=scrape_parameters,
        product_id=product_id
    )


def generate_fixture(geo_points_count: int, stores_count: int, products_count: int, market: str) -> tuple[LocationItem, list[StoreItem], list[ProductItem], list[ProductStoreDataItem]]:
    """Generate a certain number of random data for a market"""
    inizialize_lists()

    geo_points = []
    for _ in range(geo_points_count):
        geo_points.append(generate_geo_point())

    store_items = []
    for _ in range(stores_count):
        store_items.append(generate_store_item(market))

    location = generate_location_item([x.postal_code for x in geo_points], market, [
                                 x._id for x in store_items])

    product_store_data_items = []
    for store in store_items:
        for _ in range(products_count):
            product_store_data_items.append(
                generate_product_store_data_item(store._id, store.store_id, market))

    product_items = []
    for id in set([x.product_id for x in product_store_data_items]):
        product_items.append(generate_product_item(market, id))

    return location, store_items, product_items, product_store_data_items


def inizialize_lists():
    """Get products' names, variants and categories from their respective files"""

    dir_path = os.path.dirname(os.path.realpath(__file__))

    # random products taken from chat gpt
    with open(dir_path + "/product_names.txt", encoding="utf8") as names_file:
        for name in names_file:
            product_names.append(name.strip())

    # variants to append to the product name
    product_variants.append("")  # no variant option
    with open(dir_path + "/product_variants.txt", encoding="utf8") as variants_file:
        for variant in variants_file:
            product_variants.append(variant.strip())

    global product_categories
    with open(dir_path + "/product_categories.json", encoding="utf8") as categories_file:
        product_categories = json.loads(str(categories_file.read()))


inizialize_lists()

if __name__ == "__main__":
    market = random.choice(ALL_MARKETS)
    location, products, store_items = generate_fixture(5, 50, 1000, market)

    with open(OUTPUT_PATH + "\\test_locations.json", "ab") as fw:
        fw.truncate(0)
        json_exporter_location = JsonLinesItemExporter(fw)
        json_exporter_location.export_item(location)

    with open(f"{OUTPUT_PATH}\\test_products_raw_{market}.json", "ab") as fw:
        fw.truncate(0)
        for product in products:
            json_exporter_location = JsonLinesItemExporter(fw)
            json_exporter_location.export_item(product)

    with open(OUTPUT_PATH + "\\test_product_store_data_item.json", "ab") as fw:
        fw.truncate(0)
        for store_item in store_items:
            json_exporter_location = JsonLinesItemExporter(fw)
            json_exporter_location.export_item(store_item)
