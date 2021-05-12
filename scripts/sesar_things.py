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

CONCURRENT_DOWNLOADS = 3
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


def getSesarItem(igsn, t_created):
    def getRelated(relations, prefix):
        related = []
        for k in relations:
            relations_item = relations[k]
            for pk in relations_item.keys():
                if isinstance(relations_item[pk], dict):
                    rel_item = (
                        tstamp,
                        f"{prefix}.{pk}",
                        relations_item[pk]["igsn"],
                    )
                    related.append(rel_item)
                elif isinstance(relations_item[pk], list):
                    for pkitem in relations_item[pk]:
                        rel_item = (
                            tstamp,
                            f"{prefix}.{pk}",
                            pkitem["igsn"],
                        )
                        related.append(rel_item)
        return related

    L = getLogger()
    tstamp = igsn_lib.time.datetimeToJsonStr(t_created)
    L.debug("Retrieve: %s", igsn)
    try:
        # res = igsn_lib.resolve(igsn, include_body=True)
        res = isb_lib.sesar_adaptor.getSesarItem(igsn, verify=True)
        item = {}
        try:
            item = res.json()
        except Exception as e:
            L.warning(e)
        if not isinstance(item, dict):
            L.error("Returned item is not an object %s", igsn)
            return igsn, t_created, None
        if item.get("sample", None) is None:
            return igsn, t_created, None
        r_url = res.url
        r_status = res.status_code
        related = []
        related += getRelated(item["sample"].get("parents", {}), "parent")
        related += getRelated(item["sample"].get("siblings", {}), "sibling")
        related += getRelated(item["sample"].get("children", {}), "child")
        _thing = igsn_lib.models.thing.Thing(
            id=f"IGSN:{igsn}",
            tcreated=igsn_lib.time.datetimeToJD(t_created),
            item_type=item.get("sample", {}).get("sample_type", "sample"),
            authority_id="SESAR",
            related=related,
            resolved_url=r_url,
            resolved_status=r_status,
            tresolved=igsn_lib.time.jdnow(),
            resolved_content=item,
        )
        L.debug(_thing)
        return igsn, t_created, _thing
    except Exception as e:
        L.error(e)
    return igsn, t_created, None


def getSesarItemJsonLD(igsn, t_created):
    def getRelatedItems(relations, predicate):
        """For each relation, return (tstamp, predicate, target).

        The intent is to capture the statement "on tstamp, this is related to target by predicate".
        """
        nonlocal tstamp
        related = []
        for k in relations:
            related.append((tstamp, predicate, k))
        return related

    # t_created is the value of lastmod from the sitemap
    L = getLogger()
    tstamp = igsn_lib.time.datetimeToJsonStr(t_created)
    L.info("Retrieve: %s", igsn)
    try:
        # res = igsn_lib.resolve(igsn, include_body=True)
        res = isb_lib.sesar_adaptor.getSesarItem_jsonld(igsn, verify=True)
        elapsed = igsn_lib.time.datetimeDeltaToSeconds(res.elapsed)
        for h in res.history:
            elapsed = igsn_lib.time.datetimeDeltaToSeconds(h.elapsed)
        item = {}
        try:
            item = res.json()
        except Exception as e:
            L.warning(e)
        if not isinstance(item, dict):
            L.error("Returned item is not an object %s", igsn)
            return igsn, t_created, None
        r_url = res.url
        r_status = res.status_code
        related = []
        related += list(
            map(
                lambda V: (tstamp, "child", V),
                item.get("description", {})
                .get("supplementMetadata", {})
                .get("childIGSN", []),
            )
        )
        related += list(
            map(
                lambda V: (tstamp, "parent", V),
                item.get("description", {})
                .get("supplementMetadata", {})
                .get("parentIGSN", []),
            )
        )
        related += list(
            map(
                lambda V: (tstamp, "sibling", V),
                item.get("description", {})
                .get("supplementMetadata", {})
                .get("siblingIGSN", []),
            )
        )
        _thing = igsn_lib.models.thing.Thing(
            id=f"IGSN:{igsn}",
            tcreated=igsn_lib.time.datetimeToJD(t_created),
            item_type=item.get("sample", {}).get("sample_type", "sample"),
            authority_id="SESAR",
            related=related,
            resolved_url=r_url,
            resolved_status=r_status,
            tresolved=igsn_lib.time.jdnow(),
            resolved_content=item,
            resolve_elapsed = elapsed,
        )
        L.debug(_thing)
        return igsn, t_created, _thing
    except Exception as e:
        L.error(e)
    return igsn, t_created, None

def countThings(session):
    """Return number of things already collected in database"""
    return 1000


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
            while len(futures) < BACKLOG_SIZE and total_requested < max_count and num_prepared > 0:
                try:
                    _id = next(ids)
                    igsn = igsn_lib.normalize(_id[0])
                    try:
                        res = (
                            session.query(igsn_lib.models.thing.Thing)
                            .filter_by(id=f"IGSN:{igsn}")
                            .one()
                        )
                        logging.info("Already have %s at %s", igsn, _id[1])
                    except sqlalchemy.orm.exc.NoResultFound:
                        future = executor.submit(getSesarItemJsonLD, igsn, _id[1])
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
                    if not _thing is None:
                        try:
                            session.add(_thing)
                            session.commit()
                        except sqlalchemy.exc.IntegrityError as e:
                            session.rollback()
                            logging.error("Item already exists: %s", _id[0])
                        futures.remove(fut)
                        working.pop(igsn)
                        total_completed += 1
                    else:
                        L.info("Failed to retrieve %s", igsn)
                        if working.get(igsn,0) < 3:
                            future = executor.submit(getSesarItemJsonLD, igsn, tc)
                            futures.append(future)
                            if not igsn in working:
                                working[igsn] = 0
                            else:
                                working[igsn] += 1
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


@click.command()
@click.option("-d","--db_url", default=None, help="SQLAlchemy database URL for storage")
@click.option("-m","--max_records", default=-1, help="Maximum number of records to retrieve (-1 = all)")
@click.option(
    "-v", "--verbosity", default="INFO", help="Specify logging level", show_default=True
)
@click_config_file.configuration_option(config_file_name="sesar.cfg")
def main(db_url, max_records, verbosity):
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
    L.info("Maximum to load = %s", max_records)

    engine = igsn_lib.models.getEngine(db_url)
    igsn_lib.models.createAll(engine)
    session = igsn_lib.models.getSession(engine)
    oldest_record = None
    res = (
        session.query(igsn_lib.models.thing.Thing)
        .order_by(igsn_lib.models.thing.Thing.tcreated.desc())
        .first()
    )
    if not res is None:
        oldest_record = igsn_lib.time.jdToDateTime(res.tcreated)
    logging.info("Oldest = %s", oldest_record)
    time.sleep(5)
    try:
        loadSesarEntries(session, max_records, start_from=oldest_record)
    finally:
        session.close()


if __name__ == "__main__":
    main()
