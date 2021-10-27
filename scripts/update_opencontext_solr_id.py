import datetime
import logging
import time
import typing
import click
import click_config_file
import requests

import isb_lib.core
from sqlalchemy import select
from sqlalchemy import update
import igsn_lib.models
import igsn_lib.models.thing

url = "http://localhost:8984/solr/isb_core_records/"

def _fixed_opencontext_id(id: typing.AnyStr) -> typing.AnyStr:
    return isb_lib.normalized_id(id)

@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-s", "--solr_url", default=None, help="Solr index URL"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnositcs on 9999"
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, solr_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity, heart_rate)
    page_size = 5000
    count = 0
    rsession = requests.session()
    records = isb_lib.core.opencontext_fetch_broken_id_records(url, page_size, rsession)
    while len(records) > 0:
        copies = []
        ids_to_delete = []
        for record in records:
            existing_id = record["id"]
            ids_to_delete.append(existing_id)
            record_copy = record.copy()
            record_copy["id"] = _fixed_opencontext_id(record["id"])
            record_copy.pop("_version_", None)
            record_copy.pop("producedBy_samplingSite_location_bb__minY", None)
            record_copy.pop("producedBy_samplingSite_location_bb__minX", None)
            record_copy.pop("producedBy_samplingSite_location_bb__maxY", None)
            record_copy.pop("producedBy_samplingSite_location_bb__maxX", None)
            copies.append(record_copy)
        isb_lib.core.solr_delete_records(rsession, ids_to_delete, url)
        isb_lib.core.solrAddRecords(rsession, copies, url)
        count += page_size
        logging.info(f"going to next page, count is {count}")
        records = isb_lib.core.opencontext_fetch_broken_id_records(url, page_size, rsession)
    print(f"num records is {count}")
    isb_lib.core.solrCommit(rsession, url)

"""
Updates existing OpenContext records in a Things db to have their id column stripped of the n2t prefix.
"""
if __name__ == "__main__":
    main()
