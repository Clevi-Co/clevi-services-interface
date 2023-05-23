# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from dataclasses import dataclass, fields
from typing import Optional
import inspect
import hashlib


def from_dict_to_dataclass(cls, data):
    return cls(
        **{
            key: (data[key] if val.default ==
                  val.empty else data.get(key, val.default))
            for key, val in inspect.signature(cls).parameters.items()
        }
    )


@dataclass
class GeoPoint:
    postal_code: str
    city: str
    long: str
    lat: str
    address: str = ""
    state_code: str = ""
    country_code: str = ""


def compute_location_id(geo_points: list[GeoPoint]) -> str:
    if isinstance(geo_points[0], dict):
        if geo_points[0].get("postal_code") is None:
            raise TypeError("In order to compute the location id a list of GeoPoint or a list of dict with the field `postal_code` is needed")
        postal_codes = list(map(lambda x: x["postal_code"], geo_points))
    else:
        postal_codes = list(map(lambda x: x.postal_code, geo_points))
    postal_codes.sort()

    location_id = hashlib.md5()
    for p in postal_codes:
        location_id.update(bytes(str(p), encoding='utf-8'))
    return location_id.hexdigest()


@dataclass
class ProductStoreDataItem:
    # the UID used by the market (it is unique for a specific market)
    code: str
    market: str
    price: float
    discounted_price: float
    discount_rate: float
    label: str
    product_page_uri: str
    # all the data that are needed by other scraping modes of the same market
    scrape_parameters: dict
    product_id: str = None
    # Both store_id and store_universal_id are automatically set in pipelines.py based on spider.input_params
    store_id: str = None
    store_universal_id: str = None


@dataclass
class StoreItem:
    store_id: str
    name: str
    market: str
    service: str
    # all the data that are needed by other scraping modes of the same market
    scrape_parameters: dict
    # if a store only do delivery this will be None
    geo_point: Optional[GeoPoint] = None
    meta: Optional[dict] = None
    # it is automatically added in `pipeline.py`
    _id: str = None


@dataclass
class LocationItem:
    """Location items containing geospatial informations and scraped stores for each market

    This object is automatically created in `pipelines.py`
    """
    # it needs to be sorted 
    postal_codes: list[str]
    # <market_name>: list[<store_ids>]
    markets: dict[str, list[str]] = None


@dataclass
class ProductItem:
    """Product Item
    If a new field is needed, try adding it to the meta field without extending the class
    """

    # this create issues when trying to create mock fake products data
    # def __post_init__(self):
    #     force_none_fields = ('objectID', 'stores')
    #     for field in fields(self):
    #         value = getattr(self, field.name)
    #         if field.name in force_none_fields:
    #             if value is not None:
    #                 raise TypeError(
    #                     f"The field `{field.name}` must not be set. It is automatically set in `pipeline.py`")

    # the UID used by the market (it is unique for a specific market)
    code: str
    # European Article Number
    ean: str
    description: str
    market: str
    brand: str
    # ex: 500
    unit_value: float
    # ex: g
    unit_text: str
    image_urls: list[str]
    categories: list[str]
    sales_denomination: str
    # all information one can find like characteristics, ingredients, allergens, certification, etc..
    informations: dict[str, str] = None
    meta: Optional[dict] = None
    # this code is unique globally. It is formatted as `{market}_{code}` and it is automatically added in `pipeline.py`
    _id: str = None
