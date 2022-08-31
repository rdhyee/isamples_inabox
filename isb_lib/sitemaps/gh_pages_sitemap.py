import datetime
import typing
from typing import Optional
import glob
import os.path

from isb_lib.sitemaps import SitemapIndexEntry, UrlSetEntry

MAX_URLS_IN_SITEMAP = 50000


class GHPagesUrlSetIterator:
    """Iterator class responsible for listing individual urls in an urlset"""

    def __init__(
        self,
        sitemap_index: int,
        max_length: int,
        filenames: typing.List[str],
    ):
        self._filenames: list = filenames
        self._filename_index = 0
        self._max_length = max_length
        self.num_urls = 0
        self.sitemap_index = sitemap_index
        self.last_tstamp_str: Optional[str] = None
        self.last_identifier: Optional[str] = None

    def __iter__(self):
        return self

    def __next__(self) -> UrlSetEntry:
        # Dont read past the bounds
        if self._filename_index == len(self._filenames):
            raise StopIteration
        next_filename = os.path.basename(self._filenames[self._filename_index])
        # TODO: this is a placeholder, need a way to maintain this state somehow
        timestamp_str = str(datetime.datetime.now())
        next_url_set_entry = UrlSetEntry(next_filename, timestamp_str)
        # Update the necessary state
        self.num_urls += 1
        self._filename_index += 1
        self.last_tstamp_str = next_url_set_entry.last_mod_str
        self.last_identifier = next_url_set_entry.identifier
        return next_url_set_entry

    def sitemap_index_entry(self) -> SitemapIndexEntry:
        return SitemapIndexEntry(
            f"sitemap-{self.sitemap_index}.xml", self.last_tstamp_str or ""
        )


class GHPagesSitemapIndexIterator:
    """Iterator class responsible for listing the individual sitemap files in a sitemap index"""

    def __init__(
        self,
        json_directory_path: str,
        num_things_per_file: int = MAX_URLS_IN_SITEMAP
    ):
        self._last_timestamp_str: Optional[str] = None
        self._last_primary_key: Optional[str] = "0"
        self._json_directory_path = json_directory_path
        self._num_things_per_file = num_things_per_file
        self._last_url_set_iterator: Optional[GHPagesUrlSetIterator] = None
        self.num_url_sets = 0
        self._json_files = glob.glob(os.path.join(json_directory_path, "*.json"))
        self._json_file_index = 0

    def __iter__(self):
        return self

    def __next__(self) -> GHPagesUrlSetIterator:
        if self._last_url_set_iterator is not None:
            # Update our last values with the last ones from the previous iterator
            self._last_timestamp_str = self._last_url_set_iterator.last_tstamp_str
            self._last_primary_key = self._last_url_set_iterator.last_identifier

        if self._json_file_index > len(self._json_files):
            raise StopIteration

        filenames_sublist = self._json_files[self._json_file_index: self._num_things_per_file]
        next_url_set_iterator = GHPagesUrlSetIterator(
            self.num_url_sets, self._num_things_per_file, filenames_sublist
        )
        self._last_url_set_iterator = next_url_set_iterator
        self.num_url_sets = self.num_url_sets + 1
        self._json_file_index = self._json_file_index + self._num_things_per_file
        return next_url_set_iterator
