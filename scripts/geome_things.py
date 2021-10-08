import logging
import os
import time
import requests
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc
import igsn_lib
import isb_lib.core
import isb_lib.geome_adapter
import igsn_lib.time
import asyncio
import concurrent.futures
import click
import click_config_file
from isb_lib.models.thing import Thing
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO, get_thing_with_id, save_thing

CONCURRENT_DOWNLOADS = 10
BACKLOG_SIZE = 40

def getLogger():
    return logging.getLogger("main")

def wrapLoadThing(ark, tc, existing_thing: Thing = None):
    """Return request information to assist future management"""
    try:
        return ark, tc, isb_lib.geome_adapter.loadThing(ark, tc, existing_thing)
    except:
        pass
    return ark, tc, None


def countThings(session):
    """Return number of things already collected in database"""
    cnt = session.query(Thing).count()
    return cnt


async def _loadGEOMEEntries(session, max_count, start_from=None):
    L = getLogger()
    futures = []
    working = {}
    ids = isb_lib.geome_adapter.GEOMEIdentifierIterator(
        max_entries=countThings(session) + max_count, date_start=start_from
    )
    # i = 0
    # for id in ids:
    #    print(f"{i:05} {id}")
    #    i += 1
    #    if i > max_count:
    #        break
    # print(f"Counted total of {i}", i)
    # return

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
                    existing_thing = get_thing_with_id(session, identifier)
                    if existing_thing is not None:
                        logging.debug("Already have %s at %s", identifier, _id[1])
                        future = executor.submit(wrapLoadThing, identifier, _id[1], existing_thing)
                    else:
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
                            save_thing(session, _thing)
                        except sqlalchemy.exc.IntegrityError as e:
                            session.rollback()
                            logging.error("Item already exists: %s", _id[0])
                        # for _rel in _related:
                        #    try:
                        #        session.add(_rel)
                        #        session.commit()
                        #    except sqlalchemy.exc.IntegrityError as e:
                        #        L.debug(e)
                        working.pop(identifier)
                        total_completed += 1
                    else:
                        if working.get(identifier, 0) < 3:
                            if not identifier in working:
                                working[identifier] = 1
                            else:
                                working[identifier] += 1
                            L.info(
                                "Failed to retrieve %s. Retry = %s",
                                identifier,
                                working[identifier],
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


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-s", "--solr_url", default=None, help="Solr index URL"
)
@click.option(
    "-v", "--verbosity", default="INFO", help="Specify logging level", show_default=True
)
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnositcs on 9999"
)
@click_config_file.configuration_option(config_file_name="sesar.cfg")
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
@click.pass_context
def loadRecords(ctx, max_records):
    L = getLogger()
    L.info("loadRecords, max = %s", max_records)
    if max_records == -1:
        max_records = 999999999

    session = SQLModelDAO((ctx.obj["db_url"])).get_session()
    try:
        max_created = sqlmodel_database.last_time_thing_created(
            session, isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID
        )
        logging.info("Oldest = %s", max_created)
        time.sleep(1)
        loadGEOMEEntries(session, max_records, start_from=max_created)
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
    session = SQLModelDAO((ctx.obj["db_url"])).get_session()
    try:
        i = 0
        qry = session.query(Thing)
        pk = Thing.id
        for thing in _yieldRecordsByPage(qry, pk):
            itype = thing.item_type
            isb_lib.geome_adapter.reparseThing(thing, and_relations=False)
            L.info("%s: reparse %s, %s -> %s", i, thing.id, itype, thing.item_type)
            i += 1
            if i % batch_size == 0:
                session.commit()
        # don't forget to commit the remainder!
        session.commit()
    finally:
        session.close()


@main.command("relations")
@click.pass_context
def reparseRelations(ctx):
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
    rsession = requests.session()
    batch_size = 1000
    L.info("reparseRecords with batch size: %s", batch_size)
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    allkeys = set()
    try:
        i = 0
        n = 0
        qry = session.query(Thing).filter(
            Thing.authority_id
            == isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID
        )
        pk = Thing.id
        relations = []
        for thing in _yieldRecordsByPage(qry, pk):
            batch = isb_lib.geome_adapter.reparseRelations(thing)
            relations = relations + batch
            for relation in relations:
                allkeys.add(relation["id"])
            _rel_len = len(relations)
            n += len(batch)
            if i % 25 == 0:
                L.info(
                    "%s: relations id:%s num_rel:%s, total:%s", i, thing.id, _rel_len, n
                )
            if _rel_len > batch_size:
                isb_lib.core.solrAddRecords(rsession, relations, "http://localhost:8983/solr/isb_rel/")
                relations = []
            i += 1
        # don't forget to add the remainder!
        isb_lib.core.solrAddRecords(rsession, relations, "http://localhost:8983/solr/isb_rel/")
        L.info("%s: relations num_rel:%s, total:%s", i, len(relations), n)
        print(f"Total keys= {len(allkeys)}")
        isb_lib.core.solrCommit(rsession, "http://localhost:8983/solr/isb_rel/")
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
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    try:
        pass

    finally:
        session.close()

@main.command("populate_isb_core_solr")
@click.pass_context
def populateIsbCoreSolr(ctx):
    L = getLogger()
    rsession = requests.session()
    db_batch_size = 1000
    solr_batch_size = 20
    L.info("reparseRecords with batch size: %s", db_batch_size)
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    solr_url = ctx.obj["solr_url"]
    allkeys = set()
    try:
        offset = 0
        all_core_records = []
        thing_iterator = isb_lib.core.ThingRecordIterator(
            session,
            authority_id=isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID,
            page_size=db_batch_size,
            offset=offset,
        )
        for thing in thing_iterator.yieldRecordsByPage():
            core_records = isb_lib.geome_adapter.reparseAsCoreRecord(thing)
            print("Just added core_records: %s", str(core_records))
            all_core_records.extend(core_records)
            for r in all_core_records:
                allkeys.add(r["id"])
            batch_size = len(all_core_records)
            if batch_size > solr_batch_size:
                isb_lib.core.solrAddRecords(rsession, all_core_records, url=solr_url)
                all_core_records = []
        if len(all_core_records) > 0:
            isb_lib.core.solrAddRecords(rsession, all_core_records, url=solr_url)
        isb_lib.core.solrCommit(rsession, url=solr_url)
        print(f"Total keys= {len(allkeys)}")
        # verify records
        # for verifying that all records were added to solr
        # found = 0
        # for _id in allkeys:
        #    res = rsession.get(f"http://localhost:8983/solr/isb_rel/get?id={_id}").json()
        #    if res.get("doc",{}).get("id") == _id:
        #        found = found +1
        #    else:
        #        print(f"Missed: {_id}")
        # print(f"Found = {found}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
