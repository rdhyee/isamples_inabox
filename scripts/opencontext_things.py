import datetime
import logging
import click
import click_config_file
import asyncio
import isb_lib.core
import isb_lib.opencontext_adapter
import igsn_lib.models
import concurrent.futures
import heartrate
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc

BACKLOG_SIZE = 40


def get_logger():
    return logging.getLogger("main")


def wrap_load_thing(thing_dict, tc):
    """Return request information to assist future management"""
    try:
        return tc, isb_lib.opencontext_adapter.load_thing(thing_dict)
    except:
        pass
    return tc, None


async def _load_open_context_entries(session, max_count, start_from):
    L = get_logger()
    records = isb_lib.opencontext_adapter.OpenContextRecordIterator(
        max_entries=max_count, date_start=start_from, page_size=200
    )

    num_ids = 0
    for record in records:
        L.info("got next id from open context %s", record)
        num_ids += 1
        id = record["uri"]
        try:
            res = session.query(igsn_lib.models.thing.Thing.id).filter_by(id=id).one()
            logging.info("Already have %s", id)
        except sqlalchemy.orm.exc.NoResultFound:
            logging.debug("Don't have %s", id)
            thing = isb_lib.opencontext_adapter.load_thing(
                record, datetime.datetime.now(), records.last_url_str()
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

    L.info("total num records %d", num_ids)

    # total_requested = 0
    # total_completed = 0
    # more_work = True
    # num_prepared = BACKLOG_SIZE  # Number of jobs to prepare for execution
    # with concurrent.futures.ThreadPoolExecutor(
    #     max_workers=1
    # ) as executor:
    #     while more_work:
    #         while (
    #             len(futures) < BACKLOG_SIZE
    #             and total_requested < max_count
    #             and num_prepared > 0
    #         ):
    #             record = next(records)
    #             print("got next id from open context %s", record)


def load_open_context_entries(session, max_count, start_from=None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _load_open_context_entries(session, max_count, start_from=start_from)
    )
    loop.run_until_complete(future)


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
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
@click_config_file.configuration_option(config_file_name="opencontext.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, verbosity, heart_rate)


@main.command("load")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.pass_context
def load_records(ctx, max_records):
    L = get_logger()
    session = isb_lib.core.get_db_session(ctx.obj["db_url"])
    L.info("loadRecords: %s", str(session))
    # ctx.obj["db_url"] = db_url
    load_open_context_entries(session, max_records, None)


@main.command("populate_isb_core_solr")
@click.pass_context
def populate_isb_core_solr(ctx):
    L = get_logger()
    db_url = ctx.obj["db_url"]
    solr_importer = isb_lib.core.CoreSolrImporter(
        db_url=db_url,
        authority_id=isb_lib.opencontext_adapter.OpenContextItem.AUTHORITY_ID,
        db_batch_size=1000,
        solr_batch_size=1000,
        solr_url="http://localhost:8983/api/collections/isb_core_records/",
    )
    allkeys = solr_importer.run_solr_import(isb_lib.opencontext_adapter.reparse_as_core_record)
    L.info(f"Total keys= {len(allkeys)}")

if __name__ == "__main__":
    main()
