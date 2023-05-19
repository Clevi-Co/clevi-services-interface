from fixtures.mock_data_generator import generate_geo_point, generate_store_item, generate_product_item, generate_product_store_data_item
from db_interface import DbInterface
# from algolia_handler import load_to_algolia
from db_interface.items import GeoPoint, LocationItem, ProductItem, StoreItem, ProductStoreDataItem
import sys
import os
import json
sys.path.append(os.getcwd())

POSTAL_CODES_COUNT = 5
STORES_PER_MARKET = 50
PRODUCTS_PER_STORE = 100

db_handler = None


def test_load_to_db(mongo_db):
    global db_handler
    if db_handler == None:
        db_handler = DbInterface(db_connection=mongo_db,
                               debug=True, is_mock=True)
        db_handler.configure_indexes()

    # Generate mock data and upload them tho the db
    markets = ["crai", "lidl", "pam"]
    geo_points: list[GeoPoint] = []
    for _ in range(POSTAL_CODES_COUNT):
        geo_points.append(generate_geo_point())
    location_item: LocationItem = LocationItem(
        postal_codes=[gp.postal_code for gp in geo_points],
        markets={}
    )

    store_items: list[StoreItem] = []
    for market in markets:
        location_item.markets = {}
        current_market_stores = []
        for _ in range(STORES_PER_MARKET):
            store: StoreItem = generate_store_item(market)
            current_market_stores.append(store)
            if market not in location_item.markets:
                location_item.markets[market] = []
            location_item.markets[market].append(store._id)
        store_items += current_market_stores
        db_handler.upsert_store_items(current_market_stores, location_item)

    product_store_data_items = []
    for store in store_items:
        current_product_store_data_items: list[ProductStoreDataItem] = []
        for _ in range(PRODUCTS_PER_STORE):
            product_store_data: ProductStoreDataItem = generate_product_store_data_item(
                store._id, store.store_id, store.market)
            current_product_store_data_items.append(product_store_data)
        db_handler.insert_temporal_products_data(
            current_product_store_data_items, store._id)
        product_store_data_items += current_product_store_data_items

    product_items: list[ProductItem] = []
    for product_id in set([psd.product_id for psd in product_store_data_items]):
        product_items.append(generate_product_item(
            product_id.split("_")[1], product_id))
    db_handler.upsert_product_items(product_items)

    # Read the data back from the db
    product_items_collection = db_handler.db[db_handler.collection_name_products]
    location_items_collection = db_handler.db[db_handler.collection_name_locations]
    store_items_collection = db_handler.db[db_handler.collection_name_stores]
    product_store_data_items_collection = db_handler.db[
        db_handler.collection_name_product_stores_data]

    it_products = product_items_collection.find()
    db_product_items: list[dict] = [p for p in it_products]

    it_locations = location_items_collection.find()
    db_location_items: list[dict] = [l for l in it_locations]

    it_store_item = store_items_collection.find()
    db_store_items: list[dict] = [s for s in it_store_item]

    it_product_store_data_items = product_store_data_items_collection.find()
    db_product_store_data_items: list[dict] = [
        psd for psd in it_product_store_data_items]

    # save the collected data on json files for eventual manual inspection
    with open(f"products.json", "w") as f_out:
        for p in db_product_items:
            p["last_updated"] = p["last_updated"].isoformat()
            f_out.write(json.dumps(p))

    with open(f"locations.json", "w") as f_out:
        for l in db_location_items:
            l["_id"] = str(l["_id"])
            l["last_updated"] = l["last_updated"].isoformat()
            f_out.write(json.dumps(l))

    with open(f"stores.json", "w") as f_out:
        for s in db_store_items:
            s["last_updated"] = s["last_updated"].isoformat()
            if "last_scraped" in s:
                s["last_scraped"] = s["last_scraped"].isoformat()
            f_out.write(json.dumps(s))

    with open(f"product_store_data.json", "w") as f_out:
        for psd in db_product_store_data_items:
            psd["_id"] = str(psd["_id"])
            psd["last_updated"] = psd["last_updated"].isoformat()
            f_out.write(json.dumps(psd))

    # Check the consistency of the data

    # lines used to purposely cause errors to see if they are detected
    # db_location_items[0]["markets"][markets[0]][0] = "fail"
    # db_location_items[0]["markets"][markets[0]].append("fail")
    # db_store_items[0]["_id"] = "fail"
    # db_product_items.pop(0)
    # db_product_store_data_items[0]["store_universal_id"] = "fail"

    location_stores_ids = set()
    for loc_stores_ids in db_location_items[0]["markets"].values():
        for store_id in loc_stores_ids:
            assert store_id not in location_stores_ids, f"Store '{store_id}' appears multiple times in location_item"
            location_stores_ids.add(store_id)

    store_ids = set()
    for store in db_store_items:
        assert store["_id"] not in store_ids, f"Store '{store['_id']}' appears multiple times in store_item"
        assert store["_id"] in location_stores_ids, f"Store '{store['_id']}' is in store_item but doesn't appear in location_item"
        store_ids.add(store["_id"])

    for store_id in location_stores_ids:
        assert store_id in store_ids, f"Store '{store_id}' is in location_item but doesn't appear in store_item"

    product_ids = set()
    for product in db_product_items:
        assert product["_id"] not in product_ids, f"Product '{product['_id']}' appears multiple times in product_item"
        product_ids.add(product["_id"])

    for product_store_data in db_product_store_data_items:
        assert product_store_data[
            "product_id"] in product_ids, f"Product store data '{product_store_data['_id']}' has a non-existent product_id '{product_store_data['product_id']}'"
        assert product_store_data[
            "store_universal_id"] in store_ids, f"Product store data '{product_store_data['_id']}' has a non-existent store_universal_id '{product_store_data['store_universal_id']}'"


# def test_load_to_algolia(mongo_db):
#     global db_handler
#     if db_handler == None:
#         db_handler = DbHandler(db_connection=mongo_db, debug=True)
#
#         market = "conad"
#         location, products, store_items = generate_fixture(
#             geo_points_count, stores_count, products_count, market)
#         db_products, _, _ = load_and_read_db(
#             db_handler, products, location, store_items, market)
#     else:
#         products_collection = db_handler.db[db_handler.collection_name_products]
#         it_products = products_collection.find()
#         db_products = [p for p in it_products]
#
#     load_to_algolia(db_products)
