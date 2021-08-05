import logging
import os
import time
import json
import requests
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc
import igsn_lib
import isb_lib.core
import isb_lib.sesar_adapter
import igsn_lib.time
import igsn_lib.models
import igsn_lib.models.thing
import asyncio
import concurrent.futures
import click
import click_config_file
import heartrate

CONCURRENT_DOWNLOADS = 10
BACKLOG_SIZE = 40


def getLogger():
    return logging.getLogger("main")


def wrapLoadThing(igsn, tc):
    """Return request information to assist future management"""
    try:
        return igsn, tc, isb_lib.sesar_adapter.loadThing(igsn, tc)
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
    ids = isb_lib.sesar_adapter.SESARIdentifiersSitemap(
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
                            .filter_by(id=isb_lib.sesar_adapter.fullIgsn(igsn))
                            .one()
                        )
                        logging.info("Already have %s at %s", igsn, _id[1])
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
                        # for _rel in _related:
                        #    try:
                        #        session.add(_rel)
                        #        session.commit()
                        #    except sqlalchemy.exc.IntegrityError as e:
                        #        L.debug(e)
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


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-v", "--verbosity", default="INFO", help="Specify logging level", show_default=True
)
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnositcs on 9999"
)
@click_config_file.configuration_option(config_file_name="sesar.cfg")
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
def loadRecords(ctx, max_records):
    L = getLogger()
    L.info("loadRecords, max = %s", max_records)
    if max_records == -1:
        max_records = 999999999
    session = isb_lib.core.get_db_session(ctx.obj["db_url"])
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
    session = isb_lib.core.get_db_session(ctx.obj["db_url"])
    try:
        i = 0
        qry = session.query(igsn_lib.models.thing.Thing)
        pk = igsn_lib.models.thing.Thing.id
        for thing in _yieldRecordsByPage(qry, pk):
            itype = thing.item_type
            isb_lib.sesar_adapter.reparseThing(thing)
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
    # This method is retained for reference to adding relations to the relations database table
    def _commit(session, relations):
        L = getLogger()
        try:
            session.bulk_save_objects(relations, preserve_order=False)
            session.commit()
            return
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
        if len(relations) < 2:
            return
        for relation in relations:
            try:
                session.add(relation)
                session.commit()
            except sqlalchemy.exc.IntegrityError as e:
                session.rollback()
                L.debug("relation already committed: %s", relation.source)

    L = getLogger()
    rsession = requests.session()
    batch_size = 5000
    L.info("reparseRecords with batch size: %s", batch_size)
    session = isb_lib.core.get_db_session(ctx.obj["db_url"])
    allkeys = set()
    try:
        i = 0
        n = 0
        relations = []
        thing_iterator = isb_lib.core.ThingRecordIterator(
            session,
            authority_id=isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID,
            page_size=db_batch_size,
            offset=offset,
        )
        for thing in thing_iterator.yieldRecordsByPage():
            batch = isb_lib.sesar_adapter.reparseRelations(thing, as_solr=True)
            relations = relations + batch
            for r in relations:
                allkeys.add(r["id"])
            _rel_len = len(relations)
            n += len(batch)
            if i % 25 == 0:
                L.info(
                    "%s: relations id:%s num_rel:%s, total:%s", i, thing.id, _rel_len, n
                )
            if _rel_len > batch_size:
                isb_lib.core.solrAddRecords(
                    rsession, relations, "http://localhost:8983/solr/isb_rel/"
                )
                relations = []
            i += 1
        isb_lib.core.solrAddRecords(
            rsession, relations, "http://localhost:8983/solr/isb_rel/"
        )
        isb_lib.core.solrCommit(rsession, "http://localhost:8983/solr/isb_rel/")
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
    raise NotImplementedError("reloadRecords")
    session = isb_lib.core.get_db_session(ctx.obj["db_url"])
    try:
        pass

    finally:
        session.close()


@main.command("populate_isb_core_solr")
@click.pass_context
def populateIsbCoreSolr(ctx):
    L = getLogger()
    db_url = ctx.obj["db_url"]
    solr_importer = isb_lib.core.CoreSolrImporter(
        db_url=db_url,
        authority_id=isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID,
        db_batch_size=1000,
        solr_batch_size=1000,
        solr_url="http://localhost:8983/api/collections/isb_core_records/",
    )
    allkeys = solr_importer.run_solr_import(isb_lib.sesar_adapter.reparseAsCoreRecord)
    L.info(f"Total keys= {len(allkeys)}")


if __name__ == "__main__":
    main()
