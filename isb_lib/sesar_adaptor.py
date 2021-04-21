"""

"""
import logging
import datetime
import sickle.oaiexceptions
import sickle.utils
import igsn_lib.oai
import igsn_lib.time
import isb_lib.core

DEFAULT_IGSN_OAI = "https://doidb.wdc-terra.org/igsnoaip/oai"

def getLogger():
    return logging.getLogger("isb_lib.sesar_adapter")


class SESARIdentifiers(isb_lib.core.IdentifierIterator):
    '''
    Implements an iterator for SESAR IGSN identifiers.

    '''
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
