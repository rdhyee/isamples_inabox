import click
import click_config_file
import isb_lib.core
import csv
import json
import isb_lib.smithsonian_adapter
import logging
import sqlalchemy
import datetime
from isb_web.sqlmodel_database import get_thing_with_id, SQLModelDAO


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
            logging.debug("Going to add thing to session")
            session.add(thing)
            logging.debug("Added thing to session")
            session.commit()
            logging.debug("committed session")
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
            logging.error("Item already exists: %s", record)


def load_smithsonian_entries(session, max_count, file_path, start_from=None):
    with open(file_path, newline="") as csvfile:
        csvreader = csv.reader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
        column_headers = []
        i = 0
        for i, current_values in enumerate(csvreader):
            if i == 0:
                column_headers = current_values
                continue
            # Otherwise iterate over the keys and make source JSON
            current_record = {}
            for index, key in enumerate(column_headers):
                try:
                    current_record[key] = current_values[index]
                except IndexError as e:
                    # This is expected, as the file we're processing is a join of multiple data sources.  Log it and
                    # move on
                    isb_lib.core.getLogger().info(
                        "Ran into an index error processing input: %s", e
                    )
            _save_record_to_db(session, file_path, current_record)
            if i % 1000 == 0:
                isb_lib.core.getLogger().info("\n\nNum records=%d\n\n", i)


@click.group()
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
@click.pass_context
def load_records(ctx, max_records, file):
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    logging.info("loadRecords: %s", str(session))
    load_smithsonian_entries(session, max_records, file, None)


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
