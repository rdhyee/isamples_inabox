"""

"""
import logging
import typing
import datetime
import requests
import sickle.oaiexceptions
import sickle.utils
import hashlib
import json
import igsn_lib
import igsn_lib.oai
import igsn_lib.time
import igsn_lib.models.thing
import igsn_lib.models.relation
import isb_lib.core
import isb_lib.sitemaps
import uuid
import isamples_metadata.SESARTransformer

HTTP_TIMEOUT = 10.0  # seconds
##############
# Per Lulin, we shouldn't be using this as they are wanting to deprecate it
DEFAULT_IGSN_OAI = "https://doidb.wdc-terra.org/igsnoaip/oai"
##############

DEFAULT_SESAR_SITEMAP = "https://app.geosamples.org/sitemaps/sitemap-index.xml"
MEDIA_JSON_LD = "application/ld+json"


def getLogger():
    return logging.getLogger("isb_lib.sesar_adapter")


def fullIgsn(v):
    return f"IGSN:{igsn_lib.normalize(v)}"


def getSesarItem_app(igsn_value, verify=False):
    # return JSON entry from sesar, using the older app interface
    headers = {"Accept": "application/json"}
    url = "https://app.geosamples.org/webservices/display.php"
    params = {"igsn": igsn_value}
    res = requests.get(
        url, params=params, headers=headers, verify=verify, timeout=HTTP_TIMEOUT
    )
    return res


def getSESARItem_jsonld(igsn_value: str, verify: bool = True) -> requests.Response:
    """
    Return JSON-LD representation of SESAR record.

    This is the preferred mechanism as of May 2021.

    Args:
        igsn_value: value part of IGSN identifier
        verify: TLS verify connection

    Returns:
        response object
    """
    headers = {"Accept": "application/ld+json, application/json"}
    url = f"https://api.geosamples.org/v1/sample/igsn-ev-json-ld/igsn/{igsn_value}"
    res = requests.get(url, headers=headers, verify=verify, timeout=HTTP_TIMEOUT)
    return res


class SESARItem(object):
    AUTHORITY_ID = "SESAR"
    RELATION_TYPE = {
        "parent": "sesar_parent",
        "child": "sesar_child",
    }

    def __init__(self, identifier: str, source):
        self.identifier = fullIgsn(identifier)
        self.item = source

    def asRelations(self) -> typing.List[igsn_lib.models.relation.Relation]:
        related = []
        _id = self.item.get("description", {}).get("parentIdentifier", None)
        if _id is not None:
            _id = fullIgsn(_id)
            related.append(
                igsn_lib.models.relation.Relation(
                    source=self.identifier,
                    name="",
                    s=self.identifier,
                    p=SESARItem.RELATION_TYPE["parent"],
                    o=_id,
                )
            )
        for child in (
            self.item.get("description", {})
            .get("supplementMetadata", {})
            .get("childIGSN", [])
        ):
            _id = fullIgsn(child)
            related.append(
                igsn_lib.models.relation.Relation(
                    source=self.identifier,
                    name="",
                    s=self.identifier,
                    p=SESARItem.RELATION_TYPE["child"],
                    o=_id,
                )
            )
        # Don't include siblings.
        # Adding siblings adds about an order of magnitude more relations, and
        # computing the siblings is simple - all the o with parent s
        return related

    def solrRelations(self):
        """Provides a list of relation dicts suitable for adding to Solr"""

        related = []
        _id = self.item.get("description", {}).get("parentIdentifier", None)
        if _id is not None:
            _id = fullIgsn(_id)
            related.append(
                isb_lib.core.relationAsSolrDoc(
                    igsn_lib.time.dtnow(),
                    self.identifier,
                    self.identifier,
                    SESARItem.RELATION_TYPE["parent"],
                    _id,
                    "",
                )
            )
        for child in (
            self.item.get("description", {})
            .get("supplementMetadata", {})
            .get("childIGSN", [])
        ):
            _id = fullIgsn(child)
            related.append(
                isb_lib.core.relationAsSolrDoc(
                    igsn_lib.time.dtnow(),
                    self.identifier,
                    self.identifier,
                    SESARItem.RELATION_TYPE["child"],
                    _id,
                    "",
                )
            )
        return related

    def getItemType(self):
        return self.item.get("description", {}).get("sampleType", "sample")

    def asThing(
        self,
        t_created: datetime.datetime,
        status: int,
        resolved_url: str,
        t_resolved: datetime.datetime,
        resolve_elapsed: float,
        media_type: str = None,
    ) -> igsn_lib.models.thing.Thing:
        L = getLogger()
        L.debug("SESARItem.asThing")
        # Note: SESAR incorrectly returns "application/json;charset=UTF-8" for json-ld content
        if media_type is None:
            media_type = MEDIA_JSON_LD
        _thing = igsn_lib.models.thing.Thing(
            id=self.identifier,
            tcreated=t_created,
            item_type=None,
            authority_id=SESARItem.AUTHORITY_ID,
            resolved_url=resolved_url,
            resolved_status=status,
            tresolved=t_resolved,
            resolve_elapsed=resolve_elapsed,
        )
        if not isinstance(self.item, dict):
            L.error("Item is not an object")
            return _thing
        _thing.item_type = self.getItemType()
        _thing.related = None
        _thing.resolved_media_type = media_type
        _thing.resolve_elapsed = resolve_elapsed
        _thing.resolved_content = self.item
        return _thing


def _validateResolvedContent(thing: igsn_lib.models.thing.Thing):
    if not isinstance(thing.resolved_content, dict):
        raise ValueError("Thing.resolved_content is not an object")
    if not thing.authority_id == SESARItem.AUTHORITY_ID:
        raise ValueError("Thing is not a SESAR item")

def reparseRelations(thing: igsn_lib.models.thing.Thing, as_solr: bool = False):
    _validateResolvedContent(thing)
    item = SESARItem(thing.id, thing.resolved_content)
    if as_solr:
        return item.solrRelations()
    return item.asRelations()


def reparseThing(thing: igsn_lib.models.thing.Thing) -> igsn_lib.models.thing.Thing:
    """Reparse the resolved_content"""
    _validateResolvedContent(thing)
    item = SESARItem(thing.resolved_content)
    thing.item_type = item.getItemType()
    thing.tstamp = igsn_lib.time.dtnow()
    return thing

def reparseAsCoreRecord(thing: igsn_lib.models.thing.Thing) -> typing.Dict:
    _validateResolvedContent(thing)
    transformer = isamples_metadata.SESARTransformer.SESARTransformer(thing.resolved_content)
    return isb_lib.core.coreRecordAsSolrDoc(transformer.transform())

def loadThing(
    identifier: str, t_created: datetime.datetime
) -> igsn_lib.models.thing.Thing:
    """
    Load a thing from its source.

    Minimal parsing of the thing is performed to populate the database record.

    Args:
        identifier: Identifier for the thing.
        t_created: Hint as to when the thing was created, according to the source.

    Returns:
        Instance of Thing
    """
    L = getLogger()
    L.info("loadThing: %s", identifier)
    response = getSESARItem_jsonld(identifier, verify=True)
    t_resolved = igsn_lib.time.dtnow()
    elapsed = igsn_lib.time.datetimeDeltaToSeconds(response.elapsed)
    for h in response.history:
        elapsed = igsn_lib.time.datetimeDeltaToSeconds(h.elapsed)
    r_url = response.url
    r_status = response.status_code
    obj = None
    try:
        obj = response.json()
    except Exception as e:
        L.warning(e)
    item = SESARItem(identifier, obj)
    thing = item.asThing(t_created, r_status, r_url, t_resolved, elapsed)
    return thing


def reloadThing(thing):
    """Given an instance of thing, reload from the origin source."""
    L = getLogger()
    L.debug("reloadThing id=%s", thing.id)
    identifier = igsn_lib.normalize(thing.id)
    return loadThing(identifier, thing.tcreated)


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
