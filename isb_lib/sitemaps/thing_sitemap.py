import typing
from typing import Optional

import requests

from isb_web import isb_solr_query

MAX_URLS_IN_SITEMAP = 50000


class SitemapIndexEntry:
    """Individual sitemap file entry in the sitemap index"""

    def __init__(self, sitemap_filename: str, last_mod: str):
        self.sitemap_filename = sitemap_filename
        self.last_mod_str = last_mod


class UrlSetEntry:
    """Individual url entry in an urlset"""

    def __init__(self, identifier: str, last_mod: str):
        self.identifier = identifier
        self.last_mod_str = last_mod


class UrlSetIterator:
    """Iterator class responsible for listing individual urls in an urlset"""

    def __init__(
        self,
        sitemap_index: int,
        max_length: int,
        things: typing.List[typing.Dict[typing.AnyStr, typing.AnyStr]],
    ):
        self._things: list = things
        self._thing_index = 0
        self._max_length = max_length
        self.num_urls = 0
        self.sitemap_index = sitemap_index
        self.last_tstamp_str: Optional[str] = None
        self.last_identifier: Optional[str] = None

    def __iter__(self):
        return self

    def __next__(self) -> UrlSetEntry:
        # Dont read past the bounds
        if self._thing_index == len(self._things):
            raise StopIteration
        next_thing = self._things[self._thing_index]
        timestamp_str = next_thing.get("sourceUpdatedTime")
        next_url_set_entry = UrlSetEntry(next_thing["id"], timestamp_str)
        # Update the necessary state
        self.num_urls += 1
        self._thing_index += 1
        self.last_tstamp_str = next_url_set_entry.last_mod_str
        self.last_identifier = next_url_set_entry.identifier
        return next_url_set_entry

    def sitemap_index_entry(self) -> SitemapIndexEntry:
        return SitemapIndexEntry(
            f"sitemap-{self.sitemap_index}.xml", self.last_tstamp_str or ""
        )


class SitemapIndexIterator:
    """Iterator class responsible for listing the individual sitemap files in a sitemap index"""

    def __init__(
        self,
        authority: str = None,
        num_things_per_file: int = MAX_URLS_IN_SITEMAP,
        status: int = 200,
        offset: int = 0,
    ):
        self._last_timestamp_str: Optional[str] = None
        self._last_primary_key: Optional[str] = "0"
        self._authority = authority
        self._num_things_per_file = num_things_per_file
        self._status = status
        self._offset = offset
        self._last_url_set_iterator: Optional[UrlSetIterator] = None
        self._rsession = requests.session()
        self.num_url_sets = 0

    def __iter__(self):
        return self

    def __next__(self) -> UrlSetIterator:
        if self._last_url_set_iterator is not None:
            # Update our last values with the last ones from the previous iterator
            self._last_timestamp_str = self._last_url_set_iterator.last_tstamp_str
            self._last_primary_key = self._last_url_set_iterator.last_identifier
        things = isb_solr_query.solr_records_for_sitemap(
            self._rsession, self._authority, self._offset, self._num_things_per_file
        )
        if len(things) == 0:
            raise StopIteration
        next_url_set_iterator = UrlSetIterator(
            self.num_url_sets, self._num_things_per_file, things
        )
        self._last_url_set_iterator = next_url_set_iterator
        self.num_url_sets = self.num_url_sets + 1
        self._offset = self._offset + self._num_things_per_file
        return next_url_set_iterator
