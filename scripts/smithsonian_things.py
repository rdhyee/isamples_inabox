import json
import typing

import click
import click_config_file
import isb_lib.core
import csv
import isb_lib.smithsonian_adapter
import logging
import datetime

from isamples_metadata import SmithsonianTransformer
from isb_lib import smithsonian_adapter
from isb_lib.models.thing import Thing
from isb_lib.smithsonian_adapter import SmithsonianItem
from isb_web.sqlmodel_database import SQLModelDAO, all_thing_primary_keys

BATCH_SIZE = 10000
num_inserts = 0
num_updates = 0
current_existing_things_batch = []
current_new_things_batch = []
all_ids = set()


def load_smithsonian_entries(db_session, file_path, start_from=None):
    primary_keys_by_id = all_thing_primary_keys(db_session, smithsonian_adapter.SmithsonianItem.AUTHORITY_ID)
    with open(file_path, newline="") as csvfile:
        csvreader = csv.reader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
        num_newer = 0
        column_headers = next(csvreader)
        for i, current_values in enumerate(csvreader):
            if i > 0 and i % BATCH_SIZE == 0:
                save_to_db(db_session, i, num_newer)
            # Otherwise iterate over the keys and make source JSON
            current_record = {}
            newer_than_start_from, thing_id = process_keys(column_headers, current_record, current_values, start_from)
            if newer_than_start_from:
                num_newer += 1
                thing_dict = thing_dict_for_db(current_record, file_path, thing_id, primary_keys_by_id)
                if thing_id in primary_keys_by_id:
                    current_existing_things_batch.append(thing_dict)
                else:
                    current_new_things_batch.append(thing_dict)
            all_ids.add(thing_id)

        # get the remainder
        save_to_db(db_session, i, num_newer)
        print(f"Done.  Num inserts={num_inserts}, num updates={num_updates}, num_unique_ids={len(all_ids)}")


def process_keys(column_headers, current_record, current_values, start_from):
    newer_than_start_from = False
    thing_id = None
    for index, key in enumerate(column_headers):
        if key == "id":
            thing_id = isb_lib.normalized_id(current_values[index])
        elif key == "modified":
            modified_date = datetime.datetime.strptime(current_values[index], "%Y-%m-%d %H:%M:%S")
            if start_from is None or modified_date >= start_from:
                newer_than_start_from = True
            else:
                return False, None
        try:
            current_record[key] = current_values[index]
        except IndexError as e:
            # This is expected, as the file we're processing is a join of multiple data sources.  Log it and
            # move on
            isb_lib.core.getLogger().info(
                "Ran into an index error processing input: %s", e
            )
    return newer_than_start_from, thing_id


def thing_dict_for_db(resolved_content: typing.Dict, file_path: str, thing_id: str, primary_keys_by_id: typing.Dict):
    try:
        year = resolved_content.get("year")
        month = resolved_content.get("month")
        day = resolved_content.get("day")
        if year is not None and month is not None and day is not None:
            t_created = datetime.datetime(
                year=int(year),
                month=int(month),
                day=int(day),
            )
    except Exception:
        # In many cases, these don't seem to be populated.  There's nothing we can do if they aren't there, so just
        # leave it as None.
        t_created = None
    tstamp = datetime.datetime.now()
    thing_dict = {
        "resolved_content": resolved_content,
        "id": thing_id,
        "tstamp": tstamp,
        "tcreated": t_created,
        "item_type": "sample",
        "authority_id": SmithsonianItem.AUTHORITY_ID,
        "resolved_url": f"file://{file_path}",
        "resolved_status": 200,
        "tresolved": tstamp,
        "resolve_elapsed": 0,
        "resolved_media_type": SmithsonianItem.TEXT_CSV,
        "identifiers": json.dumps([thing_id]),
        "h3": SmithsonianTransformer.geo_to_h3(resolved_content)
    }
    if thing_id in primary_keys_by_id:
        thing_dict["primary_key"] = primary_keys_by_id[thing_id]

    return thing_dict


def save_to_db(db_session, i, num_newer):
    global num_inserts, num_updates, current_new_things_batch, current_existing_things_batch
    num_inserts += len(current_new_things_batch)
    num_updates += len(current_existing_things_batch)
    print(f"\n\nNum records={i}")
    print(f"Num newer={num_newer}\n\n")
    db_session.bulk_insert_mappings(
        mapper=Thing,
        mappings=current_new_things_batch,
        return_defaults=False,
    )
    db_session.bulk_update_mappings(
        mapper=Thing, mappings=current_existing_things_batch
    )
    db_session.commit()
    current_new_things_batch = []
    current_existing_things_batch = []


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option("-s", "--solr_url", default=None, help="Solr index URL")
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
@click_config_file.configuration_option(config_file_name="smithsonian.cfg")
@click.pass_context
def main(ctx, db_url, solr_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity, heart_rate)


@main.command("load")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.option(
    "-f",
    "--file",
    help="""
    The path to the Darwin Core dump file containing the records to import.  This should be manually downloaded,
    then preprocessed with preprocess_smithsonian.py before importing here.""",
)
@click.option(
    "-d",
    "--modification_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="""The modified date to use when considering delta updates.  Records with a last modified before this date
    will be ignored"""
)
@click.pass_context
def load_records(ctx, max_records, file, modification_date):
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    logging.info("loadRecords: %s", str(session))
    load_smithsonian_entries(session, file, modification_date)


@main.command("populate_isb_core_solr")
@click.pass_context
def populate_isb_core_solr(ctx):
    logger = isb_lib.core.getLogger()
    db_url = ctx.obj["db_url"]
    solr_url = ctx.obj["solr_url"]
    solr_importer = isb_lib.core.CoreSolrImporter(
        db_url=db_url,
        authority_id=isb_lib.smithsonian_adapter.SmithsonianItem.AUTHORITY_ID,
        db_batch_size=1000,
        solr_batch_size=1000,
        solr_url=solr_url,
    )
    allkeys = solr_importer.run_solr_import(
        isb_lib.smithsonian_adapter.reparse_as_core_record
    )
    logger.info(f"Total keys= {len(allkeys)}")


if __name__ == "__main__":
    main()
