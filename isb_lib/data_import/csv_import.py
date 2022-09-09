import datetime
from typing import Optional

from frictionless import Package, Resource
from pathlib import Path
import os.path
import json

from sqlmodel import Session

import isb_web.config
from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import save_or_update_thing

p = Path(__file__)
SCHEMA_JSON = {}
schema_json_path = os.path.join(p.parent, "isamples_simple_schema.json")
with open(schema_json_path) as schema_json_file:
    SCHEMA_JSON = json.load(schema_json_file)


def create_isamples_package(file_path: str) -> Package:
    """
    Opens the specified file and return it as a list of iSamples Core record dictionaries
    Args:
        file_path: The path to the file to open

    Returns: A list of dictionaries containing the records
    """
    data_resource = Resource(source=file_path, schema=SCHEMA_JSON, trusted=True)
    package = Package(resources=[data_resource], name="isamples", title="isamples", id="isamples", trusted=True)
    return package


def unflatten_csv_row(row: dict) -> dict:
    """
    The frictionless schema specifies things in roughly the solr format.  We need to transform these into the nested
    dictionaries that we publish for sitemaps in the iSB Core Format.  See isb_lib.core._coreRecordAsSolrDoc, as that
    is the logical inverse of this and the two need to be kept in sync.
    Args:
        row: The row dictionary from the flattened csv file

    Returns:
        The row dictionary expanded out to the iSB Core Format
    """
    unflattened_row = dict(row)
    produced_by_dict: dict = {}
    unflattened_row["producedBy"] = produced_by_dict
    produced_by_sampling_site_dict: dict = {}
    produced_by_dict["samplingSite"] = produced_by_sampling_site_dict
    produced_by_sampling_site_location_dict: dict = {}
    produced_by_sampling_site_dict["location"] = produced_by_sampling_site_location_dict
    curation_dict: dict = {}
    unflattened_row["curation"] = curation_dict

    row_id = unflattened_row.pop("id")
    unflattened_row["sampleidentifier"] = row_id
    unflattened_row["@id"] = row_id
    for k, v in row.items():
        if k.startswith("producedBy"):
            unflattened_row.pop(k)
            if "location" in k:
                k = k.replace("producedBy_samplingSite_location_", "")
                if k == "elevationInMeters":
                    k = "elevation"
                produced_by_sampling_site_location_dict[k] = v
            elif "samplingSite" in k:
                produced_by_sampling_site_dict[k.replace("producedBy_samplingSite_", "")] = v
            else:
                produced_by_dict[k.replace("producedBy_", "")] = v
        elif k.startswith("curation"):
            unflattened_row.pop(k)
            if k == "curation_location":
                k = "curationLocation"
            elif k == "curation_accessContraints":
                k = "accessConstraints"
            else:
                k = k.replace("curation_", "")
            curation_dict[k] = v
    return unflattened_row


def things_from_isamples_package(session: Optional[Session], package: Package, max_entries: int = -1) -> list[Thing]:
    first_resource: Resource = package.resources[0]
    num_things = 0
    url = f"file://{first_resource.path}"
    authority_id = isb_web.config.Settings().authority_id
    things = []
    for row in first_resource:
        if 0 < max_entries <= num_things:
            break
        thing = Thing()
        thing.id = row["id"]
        thing.resolved_status = 200
        thing.tcreated = datetime.datetime.now()
        thing.resolved_url = url
        thing.authority_id = authority_id
        thing.resolved_content = unflatten_csv_row(row)
        if session is not None:
            save_or_update_thing(session, thing)
        things.append(thing)
    return things


def isb_core_dicts_from_isamples_package(package: Package, max_entries: int = -1) -> list[dict]:
    first_resource: Resource = package.resources[0]
    num_dicts = 0
    dicts = []
    for row in first_resource:
        if 0 < max_entries <= num_dicts:
            break
        dicts.append(unflatten_csv_row(row))
    return dicts
