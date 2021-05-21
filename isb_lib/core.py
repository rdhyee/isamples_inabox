"""

"""

import logging
import datetime
import hashlib
import json
import igsn_lib.time

SOLR_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def getLogger():
    return logging.getLogger("isb_lib.core")


def datetimeToSolrStr(dt):
    if dt is None:
        return None
    return dt.strftime(SOLR_TIME_FORMAT)


def relationAsSolrDoc(
    ts,
    source,
    s,
    p,
    o,
    name,
):
    doc = {
        "source": source,
        "s": s,
        "p": p,
        "o": o,
        "name": name,
    }
    doc["id"] = hashlib.md5(json.dumps(doc).encode("utf-8")).hexdigest()
    doc["tstamp"] = datetimeToSolrStr(ts)
    return doc


def solrAddRecords(rsession, records, url="http://localhost:8983/solr/isb_rel/"):
    """
    Push records to Solr.

    Existing records with the same id are overwritten with no consideration of version.

    Note that it Solr recommends no manual commits, instead rely on
    proper configuration of the core.

    Args:
        rsession: requests.Session
        relations: list of relations

    Returns: nothing

    """
    L = getLogger()
    headers = {"Content-Type":"application/json"}
    data = json.dumps(records).encode("utf-8")
    params = {"overwrite": "true"}
    _url = f"{url}update"
    res = rsession.post(_url, headers=headers, data=data, params=params)
    L.debug("post status: %s", res.status_code)
    if res.status_code != 200:
        L.error(res.text)
        #TODO: something more elegant for error handling
        raise ValueError()

def solrCommit(rsession, url="http://localhost:8983/solr/isb_rel/"):
    L = getLogger()
    headers = {"Content-Type":"application/json"}
    params = {"commit":"true"}
    _url = f"{url}update"
    res = rsession.get(_url, headers=headers, params=params)
    L.debug("Solr commit: %s", res.text)


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
        """
        Override if necessary to provide the length of the identifier list.

        Returns:
            integer
        """
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
        # L.debug("max_entries: %s; len(cpage): %s; page_offset: %s; coffset: %s", self._max_entries, len(self._cpage), self._page_offset, self._coffset)
        # Reached maximum requested entries?
        if self._max_entries > 0 and self._coffset >= self._max_entries:
            L.debug(
                "Over (%s) max entries (%s), stopping.",
                self._coffset,
                self._max_entries,
            )
            raise StopIteration
        # fetch a new page
        if self._page_offset >= len(self._cpage):
            # L.debug("Get page")
            self._getPage()
        try:
            entry = self._cpage[self._page_offset]
            self._page_offset += 1
            self._coffset += 1
            return entry
        except IndexError as e:
            raise StopIteration
        except KeyError as e:
            raise StopIteration
        except TypeError as e:
            raise StopIteration
        except ValueError as e:
            raise StopIteration


class CollectionAdaptor:
    def __init__(self):
        self._identifier_iterator = IdentifierIterator
        pass

    def listIdentifiers(self, **kwargs):
        return self._identifier_iterator(**kwargs)

    def getRecord(self, identifier, format=None, profile=None):
        return {"identifier": identifier}

    def listFormats(self, profile=None):
        return []

    def listProfiles(self, format=None):
        return []
