import typing
import urllib.parse

import geojson

from isb_lib.core import (
    MEDIA_GEO_JSON,
    MEDIA_JSON,
    parsed_datetime_from_isamples_format,
    datetimeToSolrStr,
)
from isb_web import config

STAC_FEATURE_TYPE = "Feature"
STAC_COLLECTION_TYPE = "Collection"
STAC_VERSION = "1.0.0"
COLLECTION_ID = "isamples-stac-collection"
COLLECTION_DESCRIPTION = """The Internet of Samples (iSamples) is a multi-disciplinary and multi-institutional
project funded by the National Science Foundation to design, develop, and promote service infrastructure to uniquely,
consistently, and conveniently identify material samples, record metadata about them, and persistently link them to
other samples and derived digital content, including images, data, and publications."""
COLLECTION_TITLE = "iSamples Stac Collection"
COLLECTION_LICENSE = "CC-BY-4.0"


def stac_item_from_solr_dict(
    solr_dict: typing.Dict, stac_url_prefix: str, thing_url_prefix: str
) -> typing.Optional[typing.Dict]:
    """

    Args:
        solr_dict: The solr document to transform to a STAC item (https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md)
        stac_url_prefix: The URL prefix to the stac url
        thing_url_prefix: The URL to the things url

    Returns:
        A dictionary representing the stac item corresponding to the thing document
    """
    latitude = solr_dict.get("producedBy_samplingSite_location_latitude")
    longitude = solr_dict.get("producedBy_samplingSite_location_longitude")
    if latitude is None or longitude is None:
        # Don't have location, can't make a stac item
        return None

    point = geojson.Point([longitude, latitude])
    identifier = solr_dict.get("id")
    stac_item = {
        "stac_version": STAC_VERSION,
        "stac_extensions": [],
        "type": STAC_FEATURE_TYPE,
        "id": identifier,
        "geometry": point,
        "bbox": [longitude, longitude, latitude, latitude],
    }
    result_time = solr_dict.get("producedBy_resultTime")

    if result_time is None:
        # don't have a creation date in the document, pull from the source timestamp instead
        result_time = solr_dict.get("sourceUpdatedTime")
    stac_item["properties"] = {"datetime": result_time}

    links_list = [
        {
            "rel": "self",
            "href": item_href(identifier),
            "type": MEDIA_GEO_JSON,
        }
    ]
    stac_item["links"] = links_list

    assets_dict = {
        "data": {
            "href": thing_href(identifier),
            "type": MEDIA_JSON,
            "roles": ["data"],
        }
    }
    stac_item["assets"] = assets_dict

    return stac_item


def thing_href(identifier: str):
    return f"/{config.Settings().thing_url_path}/{identifier}"


def item_href(identifier: str):
    return f"/{config.Settings().stac_item_url_path}/{identifier}.json"


def collection_href(offset: int, limit: int, authority: typing.Optional[str]) -> str:
    href = "/stac_collection/"
    query_params = {}
    if offset > 0:
        query_params["offset"] = offset
    if limit > 0:
        query_params["limit"] = limit
    if authority is not None:
        query_params["authority"] = authority
    query = urllib.parse.urlencode(query_params)
    return href + f"?{query}"


def stac_collection_from_solr_dicts(
    solr_dicts: typing.List[typing.Dict],
    has_next: bool,
    offset: int,
    limit: int,
    authority: typing.Optional[str],
) -> typing.Optional[typing.Dict]:
    # Iterate through the records, creating a collection per
    # https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md
    # Need to calculate the bounding box, which is bbox = min Longitude , min Latitude , max Longitude , max Latitude

    self_href = collection_href(offset, limit, authority)
    links = [
        {
            "rel": "root",
            "href": self_href,
            "type": MEDIA_JSON,
            "title": COLLECTION_TITLE,
        }
    ]
    extent = {}
    # Loop through the records to produce links and determine the spatial and temporal extents
    min_lat = None
    max_lat = None
    min_lon = None
    max_lon = None
    min_time = None
    max_time = None
    for item_dict in solr_dicts:
        links.append(
            {"rel": "item", "href": item_href(item_dict["id"]), "type": MEDIA_GEO_JSON}
        )
        time_str = item_dict.get("producedBy_resultTime") or item_dict.get(
            "sourceUpdatedTime"
        )
        parsed_date = parsed_datetime_from_isamples_format(time_str)
        if (
            min_lat is None
            or item_dict["producedBy_samplingSite_location_latitude"] < min_lat
        ):
            min_lat = item_dict["producedBy_samplingSite_location_latitude"]
        if (
            max_lat is None
            or item_dict["producedBy_samplingSite_location_latitude"] > max_lat
        ):
            max_lat = item_dict["producedBy_samplingSite_location_latitude"]
        if (
            min_lon is None
            or item_dict["producedBy_samplingSite_location_longitude"] < min_lon
        ):
            min_lon = item_dict["producedBy_samplingSite_location_longitude"]
        if (
            max_lon is None
            or item_dict["producedBy_samplingSite_location_longitude"] > max_lon
        ):
            max_lon = item_dict["producedBy_samplingSite_location_longitude"]
        if min_time is None or parsed_date < min_time:
            min_time = parsed_date
        if max_time is None or parsed_date > max_time:
            max_time = parsed_date
    if offset > 0:
        previous_href = collection_href(offset - limit, limit, authority)
        links.append({"rel": "previous", "href": previous_href, "type": MEDIA_JSON})
    if has_next:
        next_href = collection_href(offset + limit, limit, authority)
        links.append({"rel": "next", "href": next_href, "type": MEDIA_JSON})

    spatial_dict = {"bbox": [[min_lon, min_lat, max_lon, max_lat]]}
    extent["spatial"] = spatial_dict
    temporal_dict = {
        "interval": [[datetimeToSolrStr(min_time), datetimeToSolrStr(max_time)]]
    }
    extent["temporal"] = temporal_dict

    stac_collection = {
        "id": COLLECTION_ID,
        "type": STAC_COLLECTION_TYPE,
        "stac_version": STAC_VERSION,
        "description": COLLECTION_DESCRIPTION,
        "title": COLLECTION_TITLE,
        "extent": extent,
        "license": COLLECTION_LICENSE,
        "links": links,
    }
    return stac_collection
