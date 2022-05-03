import click
import click_config_file
import isb_lib.core
import csv
import isb_lib.smithsonian_adapter
import logging
import sqlalchemy
import datetime

from isb_lib import smithsonian_adapter
from isb_web.sqlmodel_database import get_thing_with_id, SQLModelDAO, save_thing, all_thing_primary_keys


def _save_record_to_db(session, file_path, record):
    id = record["id"]
    logging.info("got next id from smithsonian %s", id)
    existing_thing = get_thing_with_id(session, id)
    if existing_thing is not None:
        logging.info("Already have %s", id)
    else:
        logging.debug("Don't have %s", id)
        thing = isb_lib.smithsonian_adapter.load_thing(
            record, datetime.datetime.now(), file_path
        )
        try:
            save_thing(session, thing)
        except sqlalchemy.exc.IntegrityError:
            session.rollback()
            logging.error("Item already exists: %s", record)


def load_smithsonian_entries(session, max_count, file_path, start_from=None):
    primary_keys_by_id = all_thing_primary_keys(session, smithsonian_adapter.SmithsonianItem.AUTHORITY_ID)
    # current_existing_things_batch = []
    # current_new_things_batch = []
    #
    # for json_thing in things_fetcher.json_things:
    #     json_thing["tstamp"] = datetime.datetime.now()
    #     identifiers = thing_identifiers_from_resolved_content(
    #         authority, json_thing["resolved_content"]
    #     )
    #     identifiers.append(json_thing["id"])
    #     json_thing["identifiers"] = json.dumps(identifiers)
    #     # remove the pk as that isn't guaranteed to be the same
    #     del json_thing["primary_key"]
    #     if thing_ids.__contains__(json_thing["id"]):
    #         # existing row in the db, for the update to work we need to insert the pk into the dict
    #         json_thing["primary_key"] = thing_ids[json_thing["id"]]
    #         current_existing_things_batch.append(json_thing)
    #     else:
    #         current_new_things_batch.append(json_thing)
    # db_session.bulk_insert_mappings(
    #     mapper=Thing,
    #     mappings=current_new_things_batch,
    #     return_defaults=False,
    # )
    # db_session.bulk_update_mappings(
    #     mapper=Thing, mappings=current_existing_things_batch
    # )

    with open(file_path, newline="") as csvfile:
        csvreader = csv.reader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
        column_headers = []
        i = 0
        num_newer = 0
        for i, current_values in enumerate(csvreader):
            if i == 0:
                column_headers = current_values
                continue
            # Otherwise iterate over the keys and make source JSON
            current_record = {}
            newer_than_start_from = False
            for index, key in enumerate(column_headers):
                if key == "id":
                    # check to see if we have the id for it -- if we do, add in the pk
                    normalized_thing_id = isb_lib.normalized_id(current_values[index])
                    current_record["id"] = normalized_thing_id
                    if normalized_thing_id in primary_keys_by_id:
                        current_record["primary_key"] = primary_keys_by_id[normalized_thing_id]
                elif key == "modified":
                    modified_date = datetime.datetime.strptime(current_values[index], "%Y-%m-%d %H:%M:%S")
                    if modified_date >= start_from:
                        newer_than_start_from = True
                    else:
                        break
                try:
                    current_record[key] = current_values[index]
                except IndexError as e:
                    # This is expected, as the file we're processing is a join of multiple data sources.  Log it and
                    # move on
                    isb_lib.core.getLogger().info(
                        "Ran into an index error processing input: %s", e
                    )
            if newer_than_start_from:
                num_newer += 1
                # print(f"newer: {current_record['id']}")
                #_save_record_to_db(session, file_path, current_record)
            if i % 1000 == 0:
                print(f"\n\nNum records={i}")
                print(f"Num newer={num_newer}\n\n")


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
    load_smithsonian_entries(session, max_records, file, modification_date)


@main.command("populate_isb_core_solr")
@click.pass_context
def populate_isb_core_solr(ctx):
    L = isb_lib.core.getLogger()
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
    L.info(f"Total keys= {len(allkeys)}")


if __name__ == "__main__":
    main()
