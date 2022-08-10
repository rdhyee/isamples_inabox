import typing

import click
import requests
import logging

from isamples_metadata.GEOMETransformer import GEOMETransformer
from sqlmodel import Session

import isb_lib
import isb_lib.core
import isb_web
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO


def compute_geome_permitting_information(session: Session) -> dict:
    batch_size = 10000
    ark_to_permit_information = {}
    try:
        thing_iterator = isb_lib.core.ThingRecordIterator(
            session,
            authority_id="GEOME",
            page_size=batch_size,
            offset=0
        )
        # thing = get_thing_with_id(session, "IGSN:NHB002GWT")
        for thing in thing_iterator.yieldRecordsByPage():
            resolved_content = thing.resolved_content
            parent = resolved_content.get("parent")
            permit_information = parent.get("permitInformation")
            if permit_information is not None:
                print(f"permitInformation is {permit_information}")
                record = resolved_content.get("record")
                ark_to_permit_information[record["bcid"]] = permit_information
                # children = resolved_content.get("children")
                # for child in children:
                #     ark_to_permit_information[child["bcid"]] = permit_information
    finally:
        print("yo!")
    return ark_to_permit_information


def mutate_record(record: typing.Dict, permitting_information: typing.Dict) -> typing.Optional[typing.Dict]:
    # Do whatever work is required to mutate the record to update things…
    record_permit_info = permitting_information.get(record.get("id"))
    if record_permit_info is None:
        return None
    record_copy = record.copy()
    freetext = GEOMETransformer.parse_permit_freetext(record_permit_info)
    record_copy["authorizedBy"] = freetext["authorizedBy"]
    return record_copy


@click.command()
@click.pass_context
def main(ctx):
    """Starting point template for reindexing the iSB Core Solr schema.  Solr URL is contained in a file called
    isb_web_config.env that should be located in the same directory as this file."""
    solr_url = isb_web.config.Settings().solr_url
    db_url = isb_web.config.Settings().database_url
    isb_lib.core.things_main(ctx, db_url, solr_url)
    session = SQLModelDAO(db_url).get_session()
    isb_lib.core.things_main(ctx, db_url, solr_url, "INFO", False)
    geome_permit_information = compute_geome_permitting_information(session)
    total_records = 0
    batch_size = 10000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, "source:GEOME", batch_size, 0, "id asc")
    for record in iterator:
        mutated_record = mutate_record(record, geome_permit_information)
        if mutated_record is not None:
            current_mutated_batch.append(mutated_record)
        if len(current_mutated_batch) == batch_size:
            save_mutated_batch(current_mutated_batch, rsession, solr_url)
            current_mutated_batch = []
        total_records += 1
    if len(current_mutated_batch) > 0:
        # handle the remainder
        save_mutated_batch(current_mutated_batch, rsession, solr_url)

    logging.info(f"Finished iterating, visited {total_records} records")


def save_mutated_batch(current_mutated_batch, rsession, solr_url):
    logging.info(f"Going to save {len(current_mutated_batch)} records")
    isb_lib.core.solrAddRecords(rsession, current_mutated_batch, solr_url)
    isb_lib.core.solrCommit(rsession, solr_url)
    logging.info(f"Just saved {len(current_mutated_batch)} records")


if __name__ == "__main__":
    main()
