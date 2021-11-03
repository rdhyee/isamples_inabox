import typing

import click
import requests
import logging

import isb_lib.core
import isb_web
from isb_web import isb_solr_query


def insert_latlon_fields_for_record(record: typing.Dict) -> bool:
    # Don't do the same work twice…
    if record.get("producedBy_samplingSite_location_latitude") is not None or record.get("producedBy_samplingSite_location_longitude") is not None or record.get("searchText") is not None:
        return False
    latlon = record.get("producedBy_samplingSite_location_ll")
    if latlon is not None:
        lat, lon = latlon.split(",")
        record["producedBy_samplingSite_location_latitude"] = float(lat)
        record["producedBy_samplingSite_location_longitude"] = float(lon)
    return True


@click.command()
@click.pass_context
def main(ctx):
    solr_url = isb_web.config.Settings().solr_url
    isb_lib.core.things_main(ctx, None, solr_url, "INFO", False)
    offset = 0
    batch_size = 50000
    rsession = requests.session()
    while True:
        solr_records = isb_solr_query.solr_records_for_sitemap(
            rsession, None, offset, batch_size, None
        )
        if len(solr_records) == 0:
            break
        records_to_add = []
        for record in solr_records:
            if insert_latlon_fields_for_record(record):
                records_to_add.append(record)
        isb_lib.core.solrAddRecords(rsession, solr_records, solr_url)
        isb_lib.core.solrCommit(rsession, solr_url)
        offset += batch_size
        logging.info(f"Just finished {offset} records")


if __name__ == "__main__":
    main()
