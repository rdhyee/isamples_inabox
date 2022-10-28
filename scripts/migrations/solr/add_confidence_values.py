import logging
from typing import Optional

import click
import requests

import isb_lib.core
import isb_web.config
import isb_lib.sesar_adapter
from isamples_metadata.Transformer import Transformer
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator


@click.command()
@click.pass_context
def main(ctx):
    solr_url = isb_web.config.Settings().solr_url
    isb_lib.core.things_main(ctx, None, solr_url)
    add_confidence_values(solr_url)


def add_confidence_values(solr_url: str):
    total_records = 0
    batch_size = 10000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, "*:*", batch_size, 0, "id asc")
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


def _insert_confidence_values(record: dict, category_str: str, confidence_str: str) -> bool:
    categories: Optional[list] = record.get(category_str)
    if categories is None or record.get(confidence_str) is not None:
        return False
    else:
        confidences = []
        for _ in categories:
            confidences.append(Transformer.RULE_BASED_CONFIDENCE)
        record[confidence_str] = confidences
        return True


def mutate_record(record: dict) -> Optional[dict]:
    # Do whatever work is required to mutate the record to update things…
    record_copy = record.copy()
    changed = _insert_confidence_values(record_copy, "hasContextCategory", "hasContextCategoryConfidence")
    changed = changed or _insert_confidence_values(record_copy, "hasMaterialCategory", "hasMaterialCategoryConfidence")
    changed = changed or _insert_confidence_values(record_copy, "hasSpecimenCategory", "hasSpecimenCategoryConfidence")
    return record_copy if changed else None


"""
Adds rule-based confidence value for existing solr records
"""
if __name__ == "__main__":
    main()
