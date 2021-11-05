import typing

import click
import requests
import logging

import isb_lib.core
import isb_web
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator


def mutate_record(record: typing.Dict) -> typing.Optional[typing.Dict]:
    # Do whatever work is required to mutate the record to update things…
    return None


@click.command()
@click.pass_context
def main(ctx):
    """Starting point template for reindexing the iSB Core Solr schema.  Solr URL is contained in a file called
    isb_web_config.env that should be located in the same directory as this file."""
    solr_url = isb_web.config.Settings().solr_url
    isb_lib.core.things_main(ctx, None, solr_url, "INFO", False)
    total_records = 0
    batch_size = 50000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, None, batch_size, 0, "id asc")
    for record in iterator:
        mutated_record = mutate_record(record)
        if mutated_record is not None:
            current_mutated_batch.append(mutated_record)
        if len(current_mutated_batch) == batch_size:
            isb_lib.core.solrAddRecords(rsession, current_mutated_batch, solr_url)
            isb_lib.core.solrCommit(rsession, solr_url)
            logging.info(f"Just saved {len(current_mutated_batch)} records")
        total_records += 1
    logging.info(f"Finished iterating, visited {total_records} records")


if __name__ == "__main__":
    main()
