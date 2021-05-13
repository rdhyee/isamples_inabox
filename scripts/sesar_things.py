import logging
import os
import time
import requests
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc
import igsn_lib
import isb_lib.sesar_adaptor
import igsn_lib.time
import igsn_lib.models
import igsn_lib.models.thing
import asyncio
import concurrent.futures
import click
import click_config_file

CONCURRENT_DOWNLOADS = 10
BACKLOG_SIZE = 40

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
}
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
LOG_FORMAT = "%(asctime)s %(name)s:%(levelname)s: %(message)s"


def getLogger():
    return logging.getLogger("main")


def wrapLoadThing(igsn, tc):
    """Return request information to assist future management"""
    try:
        return igsn, tc, isb_lib.sesar_adaptor.loadThing(igsn, tc)
    except:
        pass
    return igsn, tc, None


def countThings(session):
    """Return number of things already collected in database"""
    cnt = session.query(igsn_lib.models.thing.Thing).count()
    return cnt


async def _loadSesarEntries(session, max_count, start_from=None):
    L = getLogger()
    futures = []
    working = {}
    ids = isb_lib.sesar_adaptor.SESARIdentifiersSitemap(
        max_entries=countThings(session) + max_count, date_start=start_from
    )
    total_requested = 0
    total_completed = 0
    more_work = True
    num_prepared = BACKLOG_SIZE  # Number of jobs to prepare for execution
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=CONCURRENT_DOWNLOADS
    ) as executor:
        while more_work:
            # populate the futures list with work until the list is full
            # or there is no more work to get.
            while (
                len(futures) < BACKLOG_SIZE
                and total_requested < max_count
                and num_prepared > 0
            ):
                try:
                    _id = next(ids)
                    igsn = igsn_lib.normalize(_id[0])
                    try:
                        res = (
                            session.query(igsn_lib.models.thing.Thing.id)
                            .filter_by(id=isb_lib.sesar_adaptor.fullIgsn(igsn))
                            .one()
                        )
                        logging.debug("Already have %s at %s", igsn, _id[1])
                    except sqlalchemy.orm.exc.NoResultFound:
                        future = executor.submit(wrapLoadThing, igsn, _id[1])
                        futures.append(future)
                        working[igsn] = 0
                        total_requested += 1
                except StopIteration as e:
                    L.info("Reached end of identifier iteration.")
                    num_prepared = 0
                if total_requested >= max_count:
                    num_prepared = 0
            L.debug("%s", working)
            try:
                for fut in concurrent.futures.as_completed(futures, timeout=1):
                    igsn, tc, _thing = fut.result()
                    futures.remove(fut)
                    if not _thing is None:
                        try:
                            session.add(_thing)
                            session.commit()
                        except sqlalchemy.exc.IntegrityError as e:
                            session.rollback()
                            logging.error("Item already exists: %s", _id[0])
                        working.pop(igsn)
                        total_completed += 1
                    else:
                        if working.get(igsn, 0) < 3:
                            if not igsn in working:
                                working[igsn] = 1
                            else:
                                working[igsn] += 1
                            L.info(
                                "Failed to retrieve %s. Retry = %s", igsn, working[igsn]
                            )
                            future = executor.submit(wrapLoadThing, igsn, tc)
                            futures.append(future)
                        else:
                            L.error("Too many retries on %s", igsn)
                            working.pop(igsn)
            except concurrent.futures.TimeoutError:
                # L.info("No futures to process")
                pass
            if len(futures) == 0 and num_prepared == 0:
                more_work = False
            if total_completed >= max_count:
                more_work = False
            L.info(
                "requested, completed, current = %s, %s, %s",
                total_requested,
                total_completed,
                len(futures),
            )


def loadSesarEntries(session, max_count, start_from=None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _loadSesarEntries(session, max_count, start_from=start_from)
    )
    loop.run_until_complete(future)


def getDBSession(db_url):
    engine = igsn_lib.models.getEngine(db_url)
    igsn_lib.models.createAll(engine)
    session = igsn_lib.models.getSession(engine)
    return session


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-v", "--verbosity", default="INFO", help="Specify logging level", show_default=True
)
@click_config_file.configuration_option(config_file_name="sesar.cfg")
@click.pass_context
def main(ctx, db_url, verbosity):
    ctx.ensure_object(dict)
    verbosity = verbosity.upper()
    logging.basicConfig(
        level=LOG_LEVELS.get(verbosity, logging.INFO),
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
    )
    L = getLogger()
    if verbosity not in LOG_LEVELS.keys():
        L.warning("%s is not a log level, set to INFO", verbosity)

    L.info("Using database at: %s", db_url)
    ctx.obj["db_url"] = db_url


@main.command("load")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.pass_context
def loadRecords(ctx, max_records):
    L = getLogger()
    L.info("loadRecords, max = %s", max_records)
    if max_records == -1:
        max_records = 999999999
    session = getDBSession(ctx.obj["db_url"])
    try:
        oldest_record = None
        res = (
            session.query(igsn_lib.models.thing.Thing)
            .order_by(igsn_lib.models.thing.Thing.tcreated.desc())
            .first()
        )
        if not res is None:
            oldest_record = res.tcreated
        logging.info("Oldest = %s", oldest_record)
        time.sleep(1)
        loadSesarEntries(session, max_records, start_from=oldest_record)
    finally:
        session.close()


@main.command("reparse")
@click.pass_context
def reparseRecords(ctx):
    L = getLogger()
    batch_size = 50
    L.info("reparseRecords with batch size: %s", batch_size)
    session = getDBSession(ctx.obj["db_url"])
    try:
        i = 0
        for athing in session.query(igsn_lib.models.thing.Thing).yield_per(1000):
            itype = athing.item_type
            isb_lib.sesar_adaptor.reparseThing(athing)
            L.info("%s: reparse %s, %s -> %s", i, athing.id, itype, athing.item_type)
            if (i + 1) % batch_size == 0:
                session.commit()
        # don't forget to commit the remainder!
        session.commit()
    finally:
        session.close()


@main.command("reload")
@click.option(
    "-s",
    "--status",
    "status_code",
    type=int,
    default=500,
    help="HTTP status of records to reload",
)
@click.pass_context
def reloadRecords(ctx, status_code):
    L = getLogger()
    L.info("reloadRecords, status_code = %s", status_code)
    session = getDBSession(ctx.obj["db_url"])
    try:
        pass

    finally:
        session.close()


if __name__ == "__main__":
    main()
