import concurrent.futures
import datetime
import json
import typing
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

import click
import requests

import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import logging
import re

from isb_lib.models.thing import Thing
from isb_lib.sitemaps.sitemap_fetcher import (
    SitemapIndexFetcher,
    SitemapFileFetcher,
    ThingFetcher, ThingsFetcher,
)
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO, all_thing_identifiers, thing_identifiers_from_resolved_content

CONCURRENT_DOWNLOADS = 50000
# when we hit this length, add some more to the queue
REFETCH_LENGTH = CONCURRENT_DOWNLOADS / 2

__NUM_THINGS_FETCHED = 0


@click.command()
@click.pass_context
@click.option(
    "-u",
    "--url",
    type=str,
    default=None,
    help="The URL to the sitemap index file to consume",
)
@click.option(
    "-a",
    "--authority",
    type=str,
    default=None,
    help="The authority used for storing the retrieved records",
)
@click.option(
    "-i",
    "--ignore_last_modified",
    is_flag=True,
    help="Whether to ignore the last modified date and do a full rebuild",
)
def main(ctx, url: str, authority: str, ignore_last_modified: bool):
    solr_url = isb_web.config.Settings().solr_url
    rsession = requests.session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=CONCURRENT_DOWNLOADS * 2, pool_maxsize=CONCURRENT_DOWNLOADS * 2
    )
    rsession.mount("http://", adapter)
    rsession.mount("https://", adapter)
    db_url = isb_web.config.Settings().database_url
    db_session = SQLModelDAO(db_url).get_session()
    if authority is not None:
        authority = authority.upper()
    isb_lib.core.things_main(ctx, db_url, solr_url, "INFO", False)
    if ignore_last_modified:
        last_updated_date = None
    else:
        # Smithsonian's dump has dates marked in the future.  So, Smithsonian will never update.  For the purposes
        # of iSamples Central, this is actually ok as we don't have an automated import pipeline for Smithsonian.
        # Once the Smithsonian gets an automated import time in place, we'll need to address this somehow.
        # https://github.com/isamplesorg/isamples_inabox/issues/110
        last_updated_date = sqlmodel_database.last_time_thing_created(
            db_session, authority
        )
    thing_ids_to_pks = all_thing_identifiers(db_session, authority)
    logging.info(
        f"Going to fetch records for authority {authority} with updated date > {last_updated_date}"
    )
    fetch_sitemap_files(authority, last_updated_date, thing_ids_to_pks, rsession, url, db_session)
    logging.info(f"Completed.  Fetched {__NUM_THINGS_FETCHED} things total.")


def thing_fetcher_for_url(thing_url: str, rsession) -> ThingFetcher:
    # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to change full to true to get all the metadata, as well as the original format
    parsed_url = urllib.parse.urlparse(thing_url)
    parsed_url = parsed_url._replace(query="full=true&format=original")
    thing_fetcher = ThingFetcher(parsed_url.geturl(), rsession)
    logging.info(f"Constructed ThingFetcher for {parsed_url.geturl()}")
    return thing_fetcher


THING_URL_REGEX = re.compile(r"(.*)/thing/(.*)")


def _group_from_thing_url_regex(thing_url: str, group: int) -> typing.Optional[str]:
    url_path = urllib.parse.urlparse(thing_url).path
    match = THING_URL_REGEX.search(url_path)
    if match is None:
        logging.critical(f"Didn't find match in thing URL {thing_url}")
        return None
    else:
        group_str = match.group(group)
        return group_str


def thing_identifier_from_thing_url(thing_url: str) -> typing.Optional[str]:
    # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to change full to true to get all the metadata, as well as the original format
    return _group_from_thing_url_regex(thing_url, 2)


def pre_thing_host_url(thing_url: str) -> typing.Optional[str]:
    # At this point, we need to parse out the the URL a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to grab the part of the URL before thing (https://mars.cyverse.org/) and change it to
    # https://mars.cyverse.org/things to do the bulk fetch
    return _group_from_thing_url_regex(thing_url, 1)


def construct_thing_futures(
    thing_futures: list,
    sitemap_file_iterator: Iterator,
    rsession: requests.sessions,
    thing_executor: ThreadPoolExecutor,
) -> bool:
    constructed_all_futures_for_sitemap_file = False
    thing_ids = []
    things_url = None
    while len(thing_ids) < CONCURRENT_DOWNLOADS:
        try:
            url = next(sitemap_file_iterator)
            if things_url is None:
                # parse out the base things url from the first one we grab (they should all be the same)
                things_url = pre_thing_host_url(url)
                if things_url is not None:
                    things_url += "/things"
                else:
                    logging.critical(f"Couldn't parse out things url from url {url} -- unable to construct things.")
            identifier = thing_identifier_from_thing_url(url)
            if identifier is not None:
                thing_ids.append(identifier)
            else:
                logging.critical(f"Cannot parse out identifier from url {url} -- will not fetch thing.")
        except StopIteration:
            constructed_all_futures_for_sitemap_file = True
            break
    things_fetcher = ThingsFetcher(things_url, thing_ids, rsession)
    things_future = thing_executor.submit(things_fetcher.fetch_things)
    thing_futures.append(things_future)
    return constructed_all_futures_for_sitemap_file


def fetch_sitemap_files(authority, last_updated_date, thing_ids: typing.Dict[str, int], rsession, url, db_session):
    sitemap_index_fetcher = SitemapIndexFetcher(
        url, authority, last_updated_date, rsession
    )
    # fetch the index file, and iterate over the individual sitemap files serially so we preserve order
    sitemap_index_fetcher.fetch_index_file()
    for url in sitemap_index_fetcher.urls_to_fetch:
        sitemap_file_fetcher = SitemapFileFetcher(
            url, authority, last_updated_date, rsession
        )
        sitemap_file_fetcher.fetch_sitemap_file()
        sitemap_file_iterator = sitemap_file_fetcher.url_iterator()
        thing_futures = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_DOWNLOADS
        ) as thing_executor:
            # Initialize to false because we want to process everything at least once even if we hit the end
            fetched_all_things_for_current_sitemap_file = False
            construct_thing_futures(
                thing_futures,
                sitemap_file_iterator,
                rsession,
                thing_executor,
            )
            current_existing_things_batch = []
            current_new_things_batch = []
            while not fetched_all_things_for_current_sitemap_file:
                # Then read out results and save to the database after the queue is filled to capacity.
                # Provided there are more urls in the iterator, return to the top of the loop to fill the queue again
                for thing_fut in concurrent.futures.as_completed(thing_futures):
                    things_fetcher = thing_fut.result()
                    if things_fetcher is not None and things_fetcher.json_things is not None:
                        global __NUM_THINGS_FETCHED
                        __NUM_THINGS_FETCHED += len(things_fetcher.json_things)
                        logging.info(f"About to process {len(things_fetcher.json_things)} things")
                        for json_thing in things_fetcher.json_things:
                            json_thing["tstamp"] = datetime.datetime.now()
                            identifiers = thing_identifiers_from_resolved_content(authority, json_thing["resolved_content"])
                            identifiers.append(json_thing["id"])
                            json_thing["identifiers"] = json.dumps(identifiers)
                            # remove the pk as that isn't guaranteed to be the same
                            del json_thing["primary_key"]
                            if thing_ids.__contains__(json_thing["id"]):
                                # existing row in the db, for the update to work we need to insert the pk into the dict
                                json_thing["primary_key"] = thing_ids[json_thing["id"]]
                                current_existing_things_batch.append(json_thing)
                            else:
                                current_new_things_batch.append(json_thing)
                        db_session.bulk_insert_mappings(
                            mapper=Thing, mappings=current_new_things_batch, return_defaults=False
                        )
                        db_session.bulk_update_mappings(
                            mapper=Thing, mappings=current_existing_things_batch
                        )
                        db_session.commit()
                        logging.info(f"Just processed {len(things_fetcher.json_things)} things")
                    else:
                        logging.error(f"Error fetching thing for {things_fetcher.url}")
                    thing_futures.remove(thing_fut)
                    if len(thing_futures) < REFETCH_LENGTH:
                        # if we are running low on things to process, kick off the next batch to download
                        fetched_all_things_for_current_sitemap_file = (
                            construct_thing_futures(
                                thing_futures,
                                sitemap_file_iterator,
                                rsession,
                                thing_executor,
                            )
                        )


if __name__ == "__main__":
    main()
