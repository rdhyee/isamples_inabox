import concurrent.futures
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
from isb_lib.sitemaps.sitemap_fetcher import (
    SitemapIndexFetcher,
    SitemapFileFetcher,
    ThingFetcher,
)
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO

CONCURRENT_DOWNLOADS = 50
# when we hit this length, add some more to the queue
REFETCH_LENGTH = CONCURRENT_DOWNLOADS / 2


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
    isb_lib.core.things_main(ctx, db_url, solr_url, "CRITICAL", False)
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
    logging.info(
        f"Going to fetch records for authority {authority} with updated date > {last_updated_date}"
    )
    fetch_sitemap_files(authority, last_updated_date, rsession, url, db_session)


def thing_fetcher_for_url(thing_url: str, rsession) -> ThingFetcher:
    # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to change full to true to get all the metadata, as well as the original format
    parsed_url = urllib.parse.urlparse(thing_url)
    parsed_url = parsed_url._replace(query="full=true&format=original")
    thing_fetcher = ThingFetcher(parsed_url.geturl(), rsession)
    logging.info(f"Constructed ThingFetcher for {parsed_url.geturl()}")
    return thing_fetcher


def construct_thing_futures(
    thing_futures: list,
    sitemap_file_iterator: Iterator,
    rsession: requests.sessions,
    thing_executor: ThreadPoolExecutor,
    sitemap_file_fetcher: SitemapFileFetcher,
) -> bool:
    fetched_all_things_for_current_sitemap_file = False
    while len(thing_futures) < CONCURRENT_DOWNLOADS:
        try:
            thing_fetcher = thing_fetcher_for_url(next(sitemap_file_iterator), rsession)
            # Add the download job to the queue and remember it
            thing_future = thing_executor.submit(thing_fetcher.fetch_thing)
            thing_futures.append(thing_future)
        except StopIteration:
            logging.info(f"Finished all the requests for f{sitemap_file_fetcher.url}")
            fetched_all_things_for_current_sitemap_file = True
    return fetched_all_things_for_current_sitemap_file


def fetch_sitemap_files(authority, last_updated_date, rsession, url, db_session):
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
        num_things_fetched = 0
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_DOWNLOADS
        ) as thing_executor:
            fetched_all_things_for_current_sitemap_file = construct_thing_futures(
                thing_futures,
                sitemap_file_iterator,
                rsession,
                thing_executor,
                sitemap_file_fetcher,
            )

            while not fetched_all_things_for_current_sitemap_file:
                # Then read out results and save to the database after the queue is filled to capacity.
                # Provided there are more urls in the iterator, return to the top of the loop to fill the queue again
                for thing_fut in concurrent.futures.as_completed(thing_futures):

                    if num_things_fetched % 1000 == 0:
                        logging.critical(f"Have fetched {num_things_fetched} things")

                    thing_fetcher = thing_fut.result()
                    if thing_fetcher.thing is not None:
                        num_things_fetched += 1
                        logging.info(
                            f"Finished fetching thing {thing_fetcher.thing.id}"
                        )
                        if (
                            thing_fetcher.primary_key_fetched
                            not in sitemap_index_fetcher.primary_keys_fetched
                        ):
                            sqlmodel_database.save_or_update_thing(
                                db_session, thing_fetcher.thing
                            )
                            sitemap_index_fetcher.primary_keys_fetched.add(
                                thing_fetcher.primary_key_fetched
                            )
                    else:
                        logging.error(f"Error fetching thing for {thing_fetcher.url}")
                        thing_id = thing_fetcher.thing_identifier()
                        sqlmodel_database.mark_thing_not_found(
                            db_session, thing_id, thing_fetcher.url
                        )
                    thing_futures.remove(thing_fut)
                    if len(thing_futures) < REFETCH_LENGTH:
                        # if we are running low on things to process, kick off the next batch to download
                        fetched_all_things_for_current_sitemap_file = (
                            construct_thing_futures(
                                thing_futures,
                                sitemap_file_iterator,
                                rsession,
                                thing_executor,
                                sitemap_file_fetcher,
                            )
                        )


if __name__ == "__main__":
    main()
