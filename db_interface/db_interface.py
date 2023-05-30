from db_interface.items import ProductStoreDataItem, ProductItem, LocationItem, StoreItem
import pymongo
from pymongo import UpdateOne
import logging
from dotenv import load_dotenv
import os
from dataclasses import asdict
from datetime import datetime, timedelta
from bson.son import SON
from math import sqrt, cos, radians
import pandas as pd
import csv


class DbInterface():

    def __init__(self, db_connection=None, debug=False, is_mock=False):
        load_dotenv()
        COLLECTION_NAME_PRODUCTS = os.getenv(
            "COSMOS_COLLECTION_NAME_PRODUCTS")
        COLLECTION_NAME_LOCATIONS = os.getenv(
            "COSMOS_COLLECTION_NAME_LOCATIONS")
        COLLECTION_NAME_STORES = os.getenv("COSMOS_COLLECTION_NAME_STORES")
        COLLECTION_NAME_PRODUCT_STORES_DATA = os.getenv(
            "COSMOS_COLLECTION_NAME_PRODUCT_STORES_DATA")

        if COLLECTION_NAME_PRODUCTS is None or COLLECTION_NAME_LOCATIONS is None or COLLECTION_NAME_STORES is None or COLLECTION_NAME_PRODUCT_STORES_DATA is None:
            raise Exception(
                "NO ENV variables found. COLLECTION_NAME_PRODUCTS, COLLECTION_NAME_LOCATIONS, COLLECTION_NAME_STORES or COLLECTION_NAME_PRODUCT_STORES_DATA are missing")

        if db_connection is None:
            MONGO_DATABASE = os.getenv("MONGO_DATABASE")
            COSMOS_CONNECTION_STRING = os.getenv("COSMOS_CONNECTION_STRING")

            if MONGO_DATABASE is None or COSMOS_CONNECTION_STRING is None:
                raise Exception(
                    "NO ENV variables found. MONGO_DATABASE or COSMOS_CONNECTION_STRING are missing")

            client = pymongo.MongoClient(COSMOS_CONNECTION_STRING)
            self.db = client[MONGO_DATABASE]
        else:
            self.db = db_connection

        self.debug = debug
        self.is_mock = is_mock
        # if self.debug:
        #     self.collection_name_products = COLLECTION_NAME_PRODUCTS_TEST
        #     self.collection_name_locations = COLLECTION_NAME_LOCATIONS_TEST
        #     self.collection_name_stores = COLLECTION_NAME_STORES_TEST
        # else:
        self.collection_name_products = COLLECTION_NAME_PRODUCTS
        self.collection_name_locations = COLLECTION_NAME_LOCATIONS
        self.collection_name_stores = COLLECTION_NAME_STORES
        self.collection_name_product_stores_data = COLLECTION_NAME_PRODUCT_STORES_DATA

    def configure_indexes(self):
        self.db[self.collection_name_locations].create_index(
            [("postal_codes", 1)])

        self.db[self.collection_name_stores].create_index(
            [("market", 1), ("last_updated", -1)])

        try:
            self.db.validate_collection(
                self.collection_name_product_stores_data)
        except pymongo.errors.OperationFailure:  # If the collection doesn't exist
            if not self.is_mock:
                self.db.create_collection(self.collection_name_product_stores_data, timeseries={
                    "timeField": "last_updated",
                    "metaField": "_id",
                    "granularity": "minutes"
                })
        self.db[self.collection_name_product_stores_data].create_index(
            [("timeseries_meta.product_id", 1), ("timeseries_meta.store_universal_id", 1), ("last_updated", 1)])

    def upsert_store_items(self, items: list[StoreItem], location_item: LocationItem):
        # Index configuration
        try:
            self.db.validate_collection(self.collection_name_locations)
            self.db.validate_collection(self.collection_name_stores)
        except pymongo.errors.OperationFailure:
            logging.warning(
                f"Collection {self.collection_name_locations} or {self.collection_name_stores} doesn't exist and will be created")
            self.configure_indexes()

        # Upload

        last_updated = datetime.utcnow()
        bulk_updates = []
        store_ids = set()
        market = items[0].market
        for item in items:
            item = asdict(item)
            item_filter = {'_id': item['_id']}
            item["last_updated"] = last_updated
            if item["_id"] in store_ids:
                item_set = {"$push": {"services": {"$each": item["services"]}}}
            else:
                store_ids.add(item["_id"])
                item_set = {"$set": item}
            if market != item["market"]:
                raise ValueError(
                    "Found two different market values for two different stores. Each store in the list should have the same market value.")
            bulk_updates.append(UpdateOne(item_filter, item_set, upsert=True))
        self.db[self.collection_name_stores].bulk_write(
            bulk_updates, ordered=False)

        store_ids = list(store_ids)
        location_filter = {"postal_codes": location_item.postal_codes}
        location_set = {"$set": {
            f"markets.{market}": store_ids,
            "postal_codes": location_item.postal_codes,
            "last_updated": datetime.utcnow(),
        }}
        self.db[self.collection_name_locations].update_one(
            location_filter, location_set, upsert=True)

        if self.debug:
            self._print_req_info("UPSERT ITEMS REQ INFO")

    def upsert_product_items(self, items: list[ProductItem]):
        try:
            self.db.validate_collection(self.collection_name_products)
        except pymongo.errors.OperationFailure:
            logging.warning(
                f"Collection {self.collection_name_products} doesn't exist and will be created")
            self.configure_indexes()

        bulk_updates = []
        last_updated = datetime.utcnow()

        for item in items:
            item = asdict(item)
            item_filter = {'_id': item['_id']}
            item["last_updated"] = last_updated
            item_set = {"$set": item}
            bulk_updates.append(UpdateOne(item_filter, item_set, upsert=True))
        self.db[self.collection_name_products].bulk_write(
            bulk_updates, ordered=False)

        if self.debug:
            self._print_req_info("UPSERT ITEMS REQ INFO")

    def insert_temporal_products_data(self, items: list[ProductStoreDataItem], store_universal_id: str):
        """Insert the list of scraped product data to the database and also update the value `last_scraped` of the StoreItem identified by `store_universal_id`
        """

        # Index configuration

        try:
            self.db.validate_collection(
                self.collection_name_product_stores_data)
        except pymongo.errors.OperationFailure:  # If the collection doesn't exist
            logging.warning(
                f"Collection {self.collection_name_product_stores_data} doesn't exist and will be created")
            self.configure_indexes()
        # self.db[self.collection_name_product_stores_data].create_index(
            # [("timeseries_meta.product_id", 1), ("timeseries_meta.store_universal_id", 1), ("last_updated", 1)])

        # Upload

        last_updated = datetime.utcnow()
        items_transformed = []
        for item in items:
            item = asdict(item)
            item["timeseries_meta"] = {}
            item["timeseries_meta"]["product_id"] = item["product_id"]
            item["timeseries_meta"]["store_universal_id"] = item["store_universal_id"]
            item["last_updated"] = last_updated
            items_transformed.append(item)
        self.db[self.collection_name_product_stores_data].insert_many(
            items_transformed, ordered=False
        )

        # Update StoreItem `last_scraped` value
        self.db[self.collection_name_stores].update_one(
            {'_id': store_universal_id}, {'$set': {'last_scraped': last_updated}})

        if self.debug:
            self._print_req_info("UPSERT ITEMS REQ INFO")

    def insert_cap(self, items: list[dict]):
        # Index configuration
        self.misc_db[self.COLLECTION_NAME_POSTAL_CODES].create_index(
            [("postal_code", 1)])

        # Upload
        res = self.misc_db[self.COLLECTION_NAME_POSTAL_CODES].insert_many(
            items, ordered=False, )
        logging.info(res)

    def _find_ids_chunks(self, collection, ids: list, id_field: str = '_id', project_mongo: dict = {},
                         chunk_size: int = 100):
        res = []
        for i in range(0, len(ids), chunk_size):
            ids_chunk = list(ids[i:i + chunk_size])
            res_chunk = list(collection.find(
                {id_field: {'$in': ids_chunk}},
                project_mongo
            ))
            res.extend(res_chunk)
        return res

    def get_market_products(self, market: str):
        it_products = self.db[self.collection_name_products].find({
            "market": market
        })
        return list(it_products)

    def get_store_products_ids(self, store_id: str):
        products_ids = self.db[self.collection_name_product_stores_data].distinct(
            'timeseries_meta.product_id', {'timeseries_meta.store_universal_id': store_id})
        return list(products_ids)

    def get_market_stores(self, market: str):
        it_stores = self.db[self.collection_name_stores].find({
            "market": market
        })
        return list(it_stores)

    def get_most_recent_products(self, store_id: str, product_ids: list[str]):
        pipeline = [
            {"$match": {"timeseries_meta.store_universal_id": store_id,
                        "timeseries_meta.product_id": {"$in": product_ids}}},
            {"$sort": SON([("last_updated", -1)])},
            {"$group": {"_id": "$timeseries_meta.product_id",
                        "last_updated": {"$first": "$last_updated"}}},
            {"$project": SON(
                {("_id", 0), ("timeseries_meta.product_id", 0), ("last_updated", 1)})}
        ]
        most_recent_date = list(
            self.db[self.collection_name_product_stores_data].aggregate(pipeline))

        if len(most_recent_date) == 0:
            logging.warning(
                f'No products_data_found for store {store_id} for ids {product_ids}')
            return []
        it_products_data = self.db[self.collection_name_product_stores_data].find({
            "timeseries_meta.store_universal_id": store_id,
            "timeseries_meta.product_id": {"$in": product_ids},
            "last_updated": {"$gte": most_recent_date[0]['last_updated']}
        })
        return list(it_products_data)

    def get_markets(self, filter_mongo: dict = {}, project_mongo: dict = {}):
        cursor_markets = self.misc_db[self.COLLECTION_NAME_MARKETS]
        return list(cursor_markets.find(filter_mongo, project_mongo))

    def get_available_markets(self, postal_code: str, lat: float, lon: float):
        """Fetch all markets that are available for the input postal_code
        Each market is characterized by the list of stores and some other meta information
        """

        def _compute_distance_fast(lat1, lon1, lat2, lon2):
            R = 6371  # radius of the earth in km
            x = (radians(lon2) - radians(lon1)) * \
                cos(0.5 * (radians(lat2) + radians(lat1)))
            y = radians(lat2) - radians(lat1)
            d = R * sqrt(x * x + y * y)
            return round(d, 2)

        filter_locations = {'postal_codes': postal_code}
        cursor_locations = self.db[self.COLLECTION_NAME_LOCATIONS].find(
            filter_locations, {'_id': -1, 'markets': 1})

        markets = {}
        market_names = set()

        store_ids = set()
        for location in cursor_locations:
            for market, market_store_ids in location['markets'].items():
                store_ids.update(market_store_ids)
                markets[market] = {
                    'stores': [],
                    'meta': {}
                }

        projection_stores = {
            '_id': 1,
            'market': 1,
            'name': 1,
            'geo_point': 1,
            'service': 1
        }
        chunk_size = 1000
        store_ids = list(store_ids)
        stores = self._find_ids_chunks(self.db[self.COLLECTION_NAME_STORES], store_ids, project_mongo=projection_stores,
                                       chunk_size=chunk_size)
        for store in stores:
            market_name = store['market']
            market_names.add(market_name)
            geo_point = store.get('geo_point')
            if geo_point is not None:
                lat_store = geo_point.get('lat')
                lon_store = geo_point.get('long')
                store['distance'] = _compute_distance_fast(
                    lat, lon, lat_store, lon_store)
            else:
                store['distance'] = 9999
            markets[market_name]['stores'].append(store)

        market_names = list(market_names)
        markets_info = self.get_markets(
            {'name_lower': {'$in': market_names}}, {'_id': 0})
        for market_info in markets_info:
            market_name = market_info['name_lower']
            markets[market_name]['meta'] = market_info
            markets[market_name]['stores'].sort(key=lambda x: x['distance'])

        return markets

    def get_prices(self, product_ids: list[str], store_id: str):
        store = self.db[self.COLLECTION_NAME_STORES].find_one(
            {'_id': store_id}, {'_id': 1, 'last_scraped': 1})
        if store is None:
            raise KeyError(f'Store {store_id} not found in the db')

        filter_products_data = {
            'timeseries_meta.product_id': {'$in': product_ids},
            'timeseries_meta.store_universal_id': store['_id'],
            'last_updated': {'$gte': store['last_scraped']}
        }
        products_data = self.db[self.COLLECTION_NAME_PRODUCT_STORES_DATA].find(filter_products_data,
                                                                               {
                                                                                   '_id': 0,
                                                                                   'product_id': 1,
                                                                                   'price': 1,
                                                                                   'discounted_price': 1,
                                                                                   'discount_rate': 1,
                                                                                   'label': 1,
                                                                                   'product_page_uri': 1
                                                                               })

        prices_data = {}
        for p in products_data:
            product_id = p.pop('product_id')
            prices_data[product_id] = p

        return prices_data

    def get_geo_points(self, filter: dict = {}):
        cursor_geo_points = self.misc_db[self.COLLECTION_NAME_GEO_POINTS]
        return list(cursor_geo_points.find(filter))

    def get_markets(self, filter: dict = {}):
        cursor_markets = self.misc_db[self.COLLECTION_NAME_MARKETS]
        return list(cursor_markets.find(filter))

    def get_stores(self, filter: dict = {}):
        cursor_stores = self.db[self.COLLECTION_NAME_STORES]
        return list(cursor_stores.find(filter))

    def get_products_data_by_store(self, universal_store_id: str) -> list[dict]:
        """Given the unique id of a store, it returns the list of products data scraped for it
        Each item returned is defined by the fields _id, last_updated and scrape_parameters
        """
        cursor_product_store_data = self.db[self.COLLECTION_NAME_PRODUCT_STORES_DATA]
        # distinct_products = cursor_product_store_data.find({'timeseries_meta.store_universal_id': universal_store_id}).distinct('timeseries_meta.product_id')
        distinct_products = cursor_product_store_data.aggregate([
            {"$match": {"timeseries_meta.store_universal_id": universal_store_id}},
            # Group documents by product_id and the most recent last_updated for each group
            {"$group": {
                "_id": "$timeseries_meta.product_id",
                "last_updated": {"$max": "$last_updated"},
                "scrape_parameters": {"$first": "$scrape_parameters"}}
             },
        ])
        return list(distinct_products)

    def get_products_to_scrape(self, market: str, date_hard: datetime):
        """Return every fast-scraped product since `date_hard` that has not been hard-scraped
        """
        cursor_distinct_products_fast = self.db[self.COLLECTION_NAME_PRODUCT_STORES_DATA].aggregate([
            {"$match": {"$and": [{"market": market}, {
                "last_updated": {"$gte": date_hard}}]}},
            # Group documents by product_id
            {"$group": {
                "_id": "$timeseries_meta.product_id",
                "last_updated": {"$max": "$last_updated"},
                "scrape_parameters": {"$first": "$scrape_parameters"}}
             },
        ])
        cursor_product_ids_scraped = self.db[self.COLLECTION_NAME_PRODUCTS].distinct(
            "_id", {"market": market})
        products_ids_scraped = set(cursor_product_ids_scraped)

        products_scrape_parameters = []
        count_fast = 0
        for product in cursor_distinct_products_fast:
            count_fast += 1
            if product['_id'] not in products_ids_scraped:
                products_scrape_parameters.append(product['scrape_parameters'])

        logging.info(
            f"Product prices scraped today: {count_fast}\nProduct that needs to be hard scraped: {len(products_scrape_parameters)}")
        return products_scrape_parameters

    def get_product_store_data_to_dump(self, days_to_skip: int) -> list[dict]:
        """
        Get the data older than 'days_to_skip' days ago
        """

        date = datetime.now() - timedelta(days=days_to_skip)
        product_store_data = self.db[self.collection_name_product_stores_data].find(
            {"last_updated": {"$lte": datetime(date.year, date.month, date.day)}},
            {
                "_id": 0,
                "price": 1,
                "discounted_price": 1,
                "product_id": 1,
                "store_universal_id": 1,
                "last_updated": 1
            }
        )
        return list(product_store_data)

    def extract_brand_data(self, brand_filter):
        """Dump a csv file with the price data of products specified by the brand filter for each store (NO historical, therefore the couple 'store' and 'product_id' is univoque)
        """
        prod_id_list = list(
            self.db[self.COLLECTION_NAME_PRODUCTS].distinct('_id', brand_filter))

        pipeline = [
            {'$match': {'timeseries_meta.product_id': {'$in': prod_id_list}}},
            {'$group': {
                '_id': {'product_id': '$timeseries_meta.product_id',
                        'store_universal_id': '$timeseries_meta.store_universal_id'},
                'last_updated': {'$max': '$last_updated'},
                'product_id': {'$first': '$product_id'},
                'price': {'$first': '$price'},
                'discounted_price': {'$first': '$discounted_price'},
                'discount_rate': {'$first': '$discount_rate'},
                'label': {'$first': '$label'}
            }},
            {'$project': {
                '_id': 0,
                'product_id': '$_id.product_id',
                'store_universal_id': '$_id.store_universal_id',
                'last_updated': 1,
                'price': 1,
                'discounted_price': 1,
                'discount_rate': 1,
                'label': 1
            }}
        ]

        result = self.db[self.COLLECTION_NAME_PRODUCT_STORES_DATA].aggregate(
            pipeline)

        df = pd.DataFrame(result)
        # df["year"] = df["last_updated"].apply(lambda x: x.year)
        # df["month"] = df["last_updated"].apply(lambda x: x.month)
        # df["day"] = df["last_updated"].apply(lambda x: x.day)

        # stores = list(self.db[self.COLLECTION_NAME_STORES].find(
        #     {"$and": [{'_id':{'$in': df["store_universal_id"].to_list()}}, {"$neq": {"meta.lockers": True}}]},
        #     {'_id': 1, 'geo_point': 1}
        # ))
        chunk_size = 100
        stores = []
        for i in range(0, len(df["store_universal_id"]), chunk_size):
            ids_chunk = list(df["store_universal_id"][i:i + chunk_size])
            stores_chunk = list(self.db[self.COLLECTION_NAME_STORES].find(
                {"$and": [{'_id': {'$in': ids_chunk}},
                          {"meta.lockers": {"$ne": True}}]},
                {'_id': 1, 'geo_point': 1}
            ))
            stores.extend(stores_chunk)

        geopoints = {store['_id']: store['geo_point'] for store in stores}

        # "meta.locker" che deve essere False e sul campo "service" che deve essere "pickup"

        df["provincia"] = df['store_universal_id'].apply(
            lambda x: geopoints[x]['state_code'] if geopoints.get(x) else None)
        df["CAP"] = df['store_universal_id'].apply(
            lambda x: geopoints[x]['postal_code'] if geopoints.get(x) else None)
        df["address"] = df['store_universal_id'].apply(
            lambda x: geopoints[x]['address'] if geopoints.get(x) else None)

        # if internal:
        df["lat"] = df['store_universal_id'].apply(
            lambda x: geopoints[x]['lat'] if geopoints.get(x) else None)
        df["long"] = df['store_universal_id'].apply(
            lambda x: geopoints[x]['long'] if geopoints.get(x) else None)

        products = []
        for i in range(0, len(df["product_id"]), chunk_size):
            ids_chunk = list(df["product_id"][i:i + chunk_size])
            products_chunk = list(self.db[self.COLLECTION_NAME_PRODUCTS].find(
                {'_id': {'$in': ids_chunk}},
                {'_id': 1, 'market': 1, 'sales_denomination': 1, 'unit_value': 1, 'unit_text': 1, 'ean': 1,
                 'description': 1}
            ))
            products.extend(products_chunk)

        prod_info = {prod['_id']: {'market': prod['market'], 'sales_denomination': prod['sales_denomination'],
                                   'unit_value': prod['unit_value'],
                                   'unit_text': prod['unit_text'], 'ean': prod['ean'],
                                   'description': prod["description"]} for prod in products}

        df["market"] = df['product_id'].apply(
            lambda x: prod_info[x]['market'] if prod_info[x] else None)
        df["sales_denomination"] = df['product_id'].apply(
            lambda x: prod_info[x]['sales_denomination'] if prod_info[x] else None)
        df["unit_value"] = df['product_id'].apply(
            lambda x: prod_info[x]['unit_value'] if prod_info[x] else None)
        df["unit_text"] = df['product_id'].apply(
            lambda x: prod_info[x]['unit_text'] if prod_info[x] else None)
        df["ean"] = df['product_id'].apply(
            lambda x: prod_info[x]['ean'] if prod_info[x] else None)
        df["description"] = df['product_id'].apply(
            lambda x: prod_info[x]['description'] if prod_info[x] else None)

        df["sales_denomination"].replace('\n', '. ', regex=True, inplace=True)
        df["description"].replace('\n', '. ', regex=True, inplace=True)

        df["unitary_price"] = df["label"].apply(lambda x: x.split(' ')[1])
        df["unitary_measure"] = df["label"].apply(lambda x: x.split(' ')[3])

        df["store_type"] = df["store_universal_id"].apply(
            lambda x: x.split('_')[-1])
        df["store_universal_id"] = df["store_universal_id"].apply(
            lambda x: x.split('_')[0] + '_' + x.split('_')[1])

        try:
            df.sort_values(["last_updated", "ean", "store_universal_id", "market"],
                           ascending=[False, False, False, False],
                           inplace=True)
        except Exception as err:
            logging.error(err)

        # if not internal:
        #     df = df[["market", "provincia", "CAP", "store_universal_id",
        #                 "address", "store_type", "ean","description", "sales_denomination", "unit_value", "unit_text",
        #                 "price", "discounted_price", "discount_rate", "label", "unitary_price", "unitary_measure"]]

        df.to_csv("res.csv", index=False, quoting=csv.QUOTE_ALL)
        logging.info('File saved as res.csv')

    def delete_product_store_date(self, days_to_skip: int):
        date = datetime.now() - timedelta(days=days_to_skip)
        return self.db[self.collection_name_product_stores_data].delete_many({"last_updated": {"$lte": datetime(date.year, date.month, date.day)}})

    def _print_req_info(self, text: str = ""):
        """Log last MongoDB request info together with a text"""

        try:
            response = self.db.command('getLastRequestStatistics')
            logging.info(text)
            logging.info(response)
        except pymongo.errors.OperationFailure:
            logging.info(
                "No usage statistics available, probably this is not an online instance of the database")

    # This method makes no sense, since return the last store scraped

    # def get_last_scraped_stores(self, market: str, number_of_stores):
    #     most_recent_date = self.db[self.COLLECTION_NAME_STORES].aggregate([
    #         {"$match": {"market": market}},
    #         {"$sort": {"last_scraped": -1}},
    #         {"$group": {"_id": None, "last_scraped": {"$max": "$last_scraped"}}}
    #     ]).next()["last_scraped"]
    #     most_recent_stores = self.db[self.COLLECTION_NAME_STORES].find(
    #         {'last_scraped': most_recent_date})
    #     return list(most_recent_stores)
