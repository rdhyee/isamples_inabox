import logging
from typing import Optional

import click
import requests

import isb_lib.core
import isb_web.config
import isb_lib.sesar_adapter
from isamples_metadata.Transformer import geo_to_h3
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator


@click.command()
@click.pass_context
def main(ctx):
    solr_url = isb_web.config.Settings().solr_url
    isb_lib.core.things_main(ctx, None, solr_url)
    add_h3_values(solr_url)


def add_h3_values(solr_url: str):
    total_records = 0
    batch_size = 50000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(
        rsession, "producedBy_samplingSite_location_h3:*", batch_size, 0, "id asc"
    )
    for record in iterator:
        mutated_record = mutate_record(record)
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


def mutate_record(record: dict) -> Optional[dict]:
    # Do whatever work is required to mutate the record to update things…
    record_copy = record.copy()
    for index in range(0, 15):
        h3_at_resolution = geo_to_h3(
            record.get("producedBy_samplingSite_location_latitude"),
            record.get("producedBy_samplingSite_location_longitude"),
            index,
        )
        field_name = f"producedBy_samplingSite_location_h3_{index}"
        record_copy[field_name] = h3_at_resolution
    return record_copy


"""
Adds h3 values at different resolutions
"""
if __name__ == "__main__":
    main()
