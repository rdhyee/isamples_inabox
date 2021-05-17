import logging
import os
import time
import requests
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc
import igsn_lib
import isb_lib.geome_adapter
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


def wrapLoadThing(ark, tc):
    """Return request information to assist future management"""
    try:
        return ark, tc, isb_lib.geome_adapter.loadThing(ark, tc)
    except:
        pass
    return ark, tc, None


def countThings(session):
    """Return number of things already collected in database"""
    cnt = session.query(igsn_lib.models.thing.Thing).count()
    return cnt


async def _loadGEOMEEntries(session, max_count, start_from=None):
    L = getLogger()
    futures = []
    working = {}
    ids = isb_lib.geome_adapter.GEOMEIdentifierIterator(
        max_entries=countThings(session) + max_count, date_start=start_from
    )
    #i = 0
    #for id in ids:
    #    print(f"{i:05} {id}")
    #    i += 1
    #    if i > max_count:
    #        break
    #print(f"Counted total of {i}", i)
    #return

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
                    identifier = _id[0]
                    try:
                        res = (
                            session.query(igsn_lib.models.thing.Thing.id)
                            .filter_by(id=identifier)
                            .one()
                        )
                        logging.debug("Already have %s at %s", identifier, _id[1])
                    except sqlalchemy.orm.exc.NoResultFound:
                        future = executor.submit(wrapLoadThing, identifier, _id[1])
                        futures.append(future)
                        working[identifier] = 0
                        total_requested += 1
                except StopIteration as e:
                    L.info("Reached end of identifier iteration.")
                    num_prepared = 0
                if total_requested >= max_count:
                    num_prepared = 0
            L.debug("%s", working)
            try:
                for fut in concurrent.futures.as_completed(futures, timeout=1):
                    identifier, tc, _thing = fut.result()
                    futures.remove(fut)
                    if not _thing is None:
                        try:
                            session.add(_thing)
                            session.commit()
                        except sqlalchemy.exc.IntegrityError as e:
                            session.rollback()
                            logging.error("Item already exists: %s", _id[0])
                        working.pop(identifier)
                        total_completed += 1
                    else:
                        if working.get(identifier, 0) < 3:
                            if not identifier in working:
                                working[identifier] = 1
                            else:
                                working[identifier] += 1
                            L.info(
                                "Failed to retrieve %s. Retry = %s", identifier, working[identifier]
                            )
                            future = executor.submit(wrapLoadThing, identifier, tc)
                            futures.append(future)
                        else:
                            L.error("Too many retries on %s", identifier)
                            working.pop(identifier)
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


def loadGEOMEEntries(session, max_count, start_from=None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _loadGEOMEEntries(session, max_count, start_from=start_from)
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
        loadGEOMEEntries(session, max_records, start_from=oldest_record)
    finally:
        session.close()


@main.command("reparse")
@click.pass_context
def reparseRecords(ctx):
    raise NotImplementedError("reparseRecords")

    def _yieldRecordsByPage(qry, pk):
        nonlocal session
        offset = 0
        page_size = 5000
        while True:
            q = qry
            rec = None
            n = 0
            for rec in q.order_by(pk).offset(offset).limit(page_size):
                n += 1
                yield rec
            if n == 0:
                break
            offset += page_size

    L = getLogger()
    batch_size = 50
    L.info("reparseRecords with batch size: %s", batch_size)
    session = getDBSession(ctx.obj["db_url"])
    try:
        i = 0
        qry = session.query(igsn_lib.models.thing.Thing)
        pk = igsn_lib.models.thing.Thing.id
        for thing in _yieldRecordsByPage(qry, pk):
            itype = thing.item_type
            isb_lib.sesar_adaptor.reparseThing(thing)
            L.info("%s: reparse %s, %s -> %s", i, thing.id, itype, thing.item_type)
            i += 1
            if i % batch_size == 0:
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
    raise NotImplementedError("reloadRecords")
    L = getLogger()
    L.info("reloadRecords, status_code = %s", status_code)
    session = getDBSession(ctx.obj["db_url"])
    try:
        pass

    finally:
        session.close()


if __name__ == "__main__":
    main()
