"""

"""

import logging
import datetime
import igsn_lib.time

def getLogger():
    return logging.getLogger("isb_lib.core")


class IdentifierIterator:
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
        page_size: int = 100,
    ):
        self._start_offset = offset
        self._max_entries = max_entries
        self._date_start = date_start
        self._date_end = date_end
        self._page_size = page_size
        self._cpage = None
        self._coffset = self._start_offset
        self._page_offset = 0
        self._total_records = 0
        self._started = False

    def __len__(self):
        '''
        Override if necessary to provide the length of the identifier list.

        Returns:
            integer
        '''
        return self._total_records

    def _getPage(self):
        """Override this method to retrieve a page of entries from the service.

        After completion, self._cpage contains the next page of entries or None if there
        are no more pages available, and self._page_offset is set to the first entry (usually 0)

        This implementation generates a page of entries for testing purposes.
        """
        # Create at most 1000 records
        self._total_records = 1000
        if self._coffset >= self._total_records:
            self._cpage = None
            self._page_offset = 0
            return
        self._cpage = []
        for i in range(0, self._page_size):
            # create an entry tuple, (id, timestamp)
            entry = (
                self._coffset + i,
                igsn_lib.time.dtnow(),
            )
            self._cpage.append(entry)
        self._page_offset = 0

    def __iter__(self):
        return self

    def __next__(self):
        L = getLogger()
        # No more pages?
        if not self._started:
            L.debug("Not started, get first page")
            self._getPage()
            self._started = True
        if self._cpage is None:
            L.debug("_cpage is None, stopping.")
            raise StopIteration
        # Reached maximum requested entries?
        if self._max_entries> 0 and self._coffset >= self._max_entries:
            L.debug("Over max entries, stopping.")
            raise StopIteration
        # fetch a new page
        if self._page_offset >= len(self._cpage):
            self._getPage()
        try:
            entry = self._cpage[self._page_offset]
            self._page_offset += 1
            self._coffset += 1
            return entry
        except KeyError as e:
            raise StopIteration
        except TypeError as e:
            raise StopIteration
        except ValueError as e:
            raise StopIteration


class CollectionAdaptor:
    def __init__(self):
        pass

    def listIdentifiers(self):
        pass

    def getRecord(self, identifier, format=None, profile=None):
        pass

    def listFormats(self, profile=None):
        pass

    def listProfiles(self, format=None):
        pass
