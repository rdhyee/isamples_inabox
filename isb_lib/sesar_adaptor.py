"""

"""
import logging
import datetime
import requests
import sickle.oaiexceptions
import sickle.utils
import igsn_lib.oai
import igsn_lib.time
import isb_lib.core
import isb_lib.sitemaps

HTTP_TIMEOUT = 10.0  # seconds
DEFAULT_IGSN_OAI = "https://doidb.wdc-terra.org/igsnoaip/oai"
DEFAULT_SESAR_SITEMAP = "https://app.geosamples.org/sitemaps/sitemap-index.xml"


def getLogger():
    return logging.getLogger("isb_lib.sesar_adapter")


def getSesarItem_app(igsn_value, verify=False):
    # return JSON entry from sesar
    headers = {"Accept": "application/json"}
    url = "https://app.geosamples.org/webservices/display.php"
    params = {"igsn": igsn_value}
    res = requests.get(
        url, params=params, headers=headers, verify=verify, timeout=HTTP_TIMEOUT
    )
    return res


def getSesarItem_jsonld(igsn_value, verify=False):
    # Return JSON-LD representation of SESAR record. This is the preferred.
    headers = {"Accept": "application/ld+json, application/json"}
    url = f"https://api.geosamples.org/v1/sample/igsn-ev-json-ld/igsn/{igsn_value}"
    res = requests.get(url, headers=headers, verify=verify, timeout=HTTP_TIMEOUT)
    return res


class SESARIdentifiersSitemap(isb_lib.core.IdentifierIterator):
    """
    Implements an iterator for SESAR IGSN identifiers using sitemap source.

    This is much faster than OAI-PMH iteration, though requires loading the
    entire sitemap set since there's no ordering by dates.
    """

    def __init__(
        self,
        sitemap_url: str = DEFAULT_SESAR_SITEMAP,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
        )
        self._sitemap_url = sitemap_url

    def loadEntries(self):
        self._cpage = []
        smi = isb_lib.sitemaps.SiteMap(self._sitemap_url)
        counter = 0
        for item in smi.scanItems():
            self._cpage.append(item)
            counter += 1
            if counter > self._max_entries:
                break
        self._total_records = len(self._cpage)

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

        """
        if self._cpage is None:
            self.loadEntries()
        if self._coffset >= self._total_records:
            return
        self._page_offset = 0


class SESARIdentifiersOAIPMH(isb_lib.core.IdentifierIterator):
    """
    Implements an iterator for SESAR IGSN identifiers using OAI-PMH source.
    """

    def __init__(
        self,
        service_url: str = DEFAULT_IGSN_OAI,
        metadata_prefix=igsn_lib.oai.DEFAULT_METADATA_PREFIX,
        set_spec="IEDA.SESAR",
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
        )
        self.pager_params = {
            "metadataPrefix": metadata_prefix,
            "set": set_spec,
            "from": None
            if self._date_start is None
            else self._date_start.strftime(igsn_lib.time.OAI_TIME_FORMAT),
            "until": None
            if self._date_end is None
            else self._date_end.strftime(igsn_lib.time.OAI_TIME_FORMAT),
        }
        self.svc = igsn_lib.oai.getSickle(service_url)
        self.pager = None

    def _getPage(self):
        L = getLogger()
        self._page_offset = 0
        try:
            if self.pager is None:
                L.debug("pager params = %s", self.pager_params)
                try:
                    self.pager = self.svc.ListRecords(
                        ignore_deleted=True, **self.pager_params
                    )
                except sickle.oaiexceptions.NoRecordsMatch:
                    self._cpage = None
                    return
            _page = self.pager.next()
            # Get counts
            self._total_records = int(self.pager.resumption_token.complete_list_size)
            L.debug("Total records = %s", self._total_records)
            self._cpage = []
            for item in self.pager._items:
                ditem = sickle.utils.xml_to_dict(
                    item, nsmap=igsn_lib.oai.IGSN_OAI_NAMESPACES
                )
                igsn = ditem.get(
                    "{http://igsn.org/schema/kernel-v.1.0}sampleNumber",
                    [
                        None,
                    ],
                )[0]
                if not igsn is None:
                    tstamp = igsn_lib.time.datetimeFromSomething(
                        ditem.get(
                            "{http://www.openarchives.org/OAI/2.0/}datestamp",
                            [
                                None,
                            ],
                        )[0]
                    )
                    oai_id = ditem.get(
                        "{http://www.openarchives.org/OAI/2.0/}identifier",
                        [
                            None,
                        ],
                    )[0]
                    self._cpage.append(
                        (
                            igsn,
                            tstamp,
                            oai_id,
                        )
                    )
            L.debug("Items in page: %s", len(self._cpage))
        except StopIteration:
            self._cpage = None
