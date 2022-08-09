import logging
from typing import Optional

import click
import requests
from sqlmodel import Session

import isb_lib.core
import isb_web.config
from isb_lib.sesar_adapter import fullIgsn
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO


@click.command()
@click.pass_context
def main(ctx):
    db_url = isb_web.config.Settings().database_url
    solr_url = isb_web.config.Settings().solr_url
    isb_lib.core.things_main(ctx, db_url, solr_url)
    session = SQLModelDAO(db_url).get_session()
    add_sesar_relations(session, solr_url)


def add_sesar_relations(session, solr_url):
    igsn_to_parent_igsn = compute_sesar_parent_relations(session)
    total_records = 0
    batch_size = 10000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, "source:SESAR", batch_size, 0, "id asc")
    for record in iterator:
        mutated_record = mutate_record(record, igsn_to_parent_igsn)
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


def compute_sesar_parent_relations(session: Session) -> dict:
    batch_size = 10000
    igsn_to_parent_igsn = {}
    try:
        thing_iterator = isb_lib.core.ThingRecordIterator(
            session,
            authority_id=isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID,
            page_size=batch_size,
            offset=0
        )
        # thing = get_thing_with_id(session, "IGSN:NHB002GWT")
        for thing in thing_iterator.yieldRecordsByPage():
            parent = thing.resolved_content.get("description").get("parentIdentifier")
            if parent is not None:
                igsn_to_parent_igsn[fullIgsn(thing.resolved_content.get("igsn"))] = fullIgsn(parent)
    finally:
        print("yo!")
    return igsn_to_parent_igsn


def mutate_record(record: dict, igsn_to_parent_igsn: dict) -> Optional[dict]:
    # Do whatever work is required to mutate the record to update things…
    parent_igsn = igsn_to_parent_igsn.get(record.get("id"))
    if parent_igsn is None:
        return None
    record_copy = record.copy()
    relations = []
    relation_dict = {
        "relation_target": parent_igsn,
        "relation_type": "subsample",
        "id": f"{record['id']}_subsample_{parent_igsn}"
    }
    relations.append(relation_dict)
    record_copy["relations"] = relations
    return record_copy


"""
Adds SESAR embedded parent relations
"""
if __name__ == "__main__":
    main()
