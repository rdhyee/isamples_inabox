"""

"""
import logging
import datetime
import hashlib
import json
import typing

import igsn_lib.time
from isb_lib.models.thing import Thing
from isamples_metadata.Transformer import Transformer
import dateparser
import re
import requests
import shapely.wkt
import shapely.geometry
import heartrate

from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO

RECOGNIZED_DATE_FORMATS = [
    "%Y",  # e.g. 1985
    "%Y-%m-%d",  # e.g. 1947-08-06
    "%Y-%m",  # e.g. 2020-07
    "%Y-%m-%d %H:M:%S",  # e.g 2019-12-08 15:54:00
]
DATEPARSER_SETTINGS = {
    "DATE_ORDER": "YMD",
    "PREFER_DAY_OF_MONTH": "first",
    "TIMEZONE": "UTC",
    "RETURN_AS_TIMEZONE_AWARE": True,
}

SOLR_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
ELEVATION_PATTERN = re.compile(r"\s*(-?\d+\.?\d*)\s*m?", re.IGNORECASE)


MEDIA_JSON = "application/json"
MEDIA_NQUADS = "application/n-quads"
MEDIA_GEO_JSON = "application/geo+json"


def getLogger():
    return logging.getLogger("isb_lib.core")


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


def initialize_logging(verbosity: typing.AnyStr):
    logging.basicConfig(
        level=LOG_LEVELS.get(verbosity, logging.INFO),
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
    )
    L = getLogger()
    verbosity = verbosity.upper()
    if verbosity not in LOG_LEVELS.keys():
        L.warning("%s is not a log level, set to INFO", verbosity)


def things_main(ctx, db_url, solr_url, verbosity, heart_rate):
    ctx.ensure_object(dict)
    initialize_logging(verbosity)

    getLogger().info("Using database at: %s", db_url)
    ctx.obj["db_url"] = db_url
    getLogger().info("Using solr at: %s", solr_url)
    ctx.obj["solr_url"] = solr_url
    if heart_rate:
        heartrate.trace(browser=True)


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


def validate_resolved_content(authority_id: typing.AnyStr, thing: Thing):
    if not isinstance(thing.resolved_content, dict):
        raise ValueError("Thing.resolved_content is not an object")
    if not thing.authority_id == authority_id:
        raise ValueError(f"Mismatched authority_id on Thing, expecting {authority_id}, received {thing.authority_id}")


def _shouldAddMetadataValueToSolrDoc(metadata: typing.Dict, key: typing.AnyStr) -> bool:
    shouldAdd = False
    value = metadata.get(key)
    if value is not None:
        if key == "latitude":
            # Explicitly disallow bools as they'll pass the logical test otherwise and solr will choke downstream
            shouldAdd = type(value) is not bool and -90.0 <= value <= 90.0
            if not shouldAdd:
                getLogger().error("Invalid latitude %f", value)
        elif key == "longitude":
            shouldAdd = type(value) is not bool and -180.0 <= value <= 180
            if not shouldAdd:
                getLogger().error("Invalid longitude %f", value)
        elif type(value) is list:
            shouldAdd = len(value) > 0
        elif type(value) is str:
            shouldAdd = len(value) > 0 and value != Transformer.NOT_PROVIDED
        else:
            shouldAdd = True
    return shouldAdd


def _coreRecordAsSolrDoc(coreMetadata: typing.Dict) -> typing.Dict:  # noqa: C901 -- need to examine computational complexity
    # Before preparing the document in solr format, strip out any whitespace in string values
    for k, v in coreMetadata.items():
        if type(v) is str:
            coreMetadata[k] = v.strip()

    doc = {
        "id": coreMetadata["sampleidentifier"],
        "isb_core_id": coreMetadata["@id"],
        "indexUpdatedTime": datetimeToSolrStr(igsn_lib.time.dtnow())
    }
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "sourceUpdatedTime"):
        doc["sourceUpdatedTime"] = coreMetadata["sourceUpdatedTime"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "label"):
        doc["label"] = coreMetadata["label"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "description"):
        doc["description"] = coreMetadata["description"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "hasContextCategory"):
        doc["hasContextCategory"] = coreMetadata["hasContextCategory"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "hasMaterialCategory"):
        doc["hasMaterialCategory"] = coreMetadata["hasMaterialCategory"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "hasSpecimenCategory"):
        doc["hasSpecimenCategory"] = coreMetadata["hasSpecimenCategory"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "keywords"):
        doc["keywords"] = coreMetadata["keywords"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "informalClassification"):
        doc["informalClassification"] = coreMetadata["informalClassification"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "registrant"):
        doc["registrant"] = coreMetadata["registrant"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "samplingPurpose"):
        doc["samplingPurpose"] = coreMetadata["samplingPurpose"]
    if "producedBy" in coreMetadata:
        handle_produced_by_fields(coreMetadata, doc)
    if "curation" in coreMetadata:
        handle_curation_fields(coreMetadata, doc)
    if "relatedResource" in coreMetadata:
        handle_related_resources(coreMetadata, doc)

    return doc


def coreRecordAsSolrDoc(transformer: Transformer) -> typing.Dict:
    """
    Args:
        transformer: A Transformer instance containing the document to transform

    Returns: The coreMetadata in solr document format, suitable for posting to the solr JSON api
    (https://solr.apache.org/guide/8_1/json-request-api.html)
    """
    coreMetadata = transformer.transform()

    last_updated = transformer.last_updated_time()
    if last_updated is not None:
        date_time = parsed_date(last_updated)
        if date_time is not None:
            coreMetadata["sourceUpdatedTime"] = datetimeToSolrStr(date_time)
    return _coreRecordAsSolrDoc(coreMetadata)


def handle_curation_fields(coreMetadata: typing.Dict, doc: typing.Dict):
    curation = coreMetadata["curation"]
    if _shouldAddMetadataValueToSolrDoc(curation, "label"):
        doc["curation_label"] = curation["label"]
    if _shouldAddMetadataValueToSolrDoc(curation, "description"):
        doc["curation_description"] = curation["description"]
    if _shouldAddMetadataValueToSolrDoc(curation, "accessConstraints"):
        doc["curation_accessConstraints"] = curation["accessConstraints"]
    if _shouldAddMetadataValueToSolrDoc(curation, "location"):
        doc["curation_location"] = curation["location"]
    if _shouldAddMetadataValueToSolrDoc(curation, "responsibility"):
        doc["curation_responsibility"] = curation["responsibility"]


def shapely_to_solr(shape: shapely.geometry.shape):
    centroid = shape.centroid
    bb = shape.bounds
    res = {
        "producedBy_samplingSite_location_ll": f"{centroid.y},{centroid.x}",
        "producedBy_samplingSite_location_bb": f"ENVELOPE({bb[0]}, {bb[2]}, {bb[3]}, {bb[1]})",
        "producedBy_samplingSite_location_rpt": shape.wkt
    }
    return res


def lat_lon_to_solr(coreMetadata: typing.Dict, latitude: typing.SupportsFloat, longitude: typing.SupportsFloat):
    coreMetadata.update(shapely_to_solr(shapely.geometry.Point(longitude, latitude)))
    coreMetadata["producedBy_samplingSite_location_latitude"] = latitude
    coreMetadata["producedBy_samplingSite_location_longitude"] = longitude


def handle_produced_by_fields(coreMetadata: typing.Dict, doc: typing.Dict):  # noqa: C901 -- need to examine computational complexity
    # The solr index flattens subdictionaries, so check the keys explicitly in the subdictionary to see if they should be added to the index
    producedBy = coreMetadata["producedBy"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "label"):
        doc["producedBy_label"] = producedBy["label"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "description"):
        doc["producedBy_description"] = producedBy["description"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "responsibility"):
        doc["producedBy_responsibility"] = producedBy["responsibility"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "hasFeatureOfInterest"):
        doc["producedBy_hasFeatureOfInterest"] = producedBy["hasFeatureOfInterest"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "resultTime"):
        raw_date_str = producedBy["resultTime"]
        date_time = parsed_date(raw_date_str)
        if date_time is not None:
            doc["producedBy_resultTime"] = datetimeToSolrStr(date_time)
    if _shouldAddMetadataValueToSolrDoc(producedBy, "@id"):
        doc["producedBy_isb_core_id"] = producedBy["@id"]
    if "samplingSite" in producedBy:
        samplingSite = producedBy["samplingSite"]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, "description"):
            doc["producedBy_samplingSite_description"] = samplingSite["description"]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, "label"):
            doc["producedBy_samplingSite_label"] = samplingSite["label"]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, "placeName"):
            doc["producedBy_samplingSite_placeName"] = samplingSite["placeName"]

        if "location" in samplingSite:
            location = samplingSite["location"]
            if _shouldAddMetadataValueToSolrDoc(location, "elevation"):
                location_str = location["elevation"]
                match = ELEVATION_PATTERN.match(location_str)
                if match is not None:
                    doc["producedBy_samplingSite_location_elevationInMeters"] = float(
                        match.group(1)
                    )
            if _shouldAddMetadataValueToSolrDoc(
                location, "latitude"
            ) and _shouldAddMetadataValueToSolrDoc(location, "longitude"):
                lat_lon_to_solr(doc, location['latitude'], location['longitude'])


def handle_related_resources(coreMetadata: typing.Dict, doc: typing.Dict):
    related_resources = coreMetadata["relatedResource"]
    related_resource_ids = []
    for related_resource in related_resources:
        related_resource_ids.append(related_resource["target"])
    doc["relatedResource_isb_core_id"] = related_resource_ids


def parsed_date(raw_date_str):
    # TODO: https://github.com/isamplesorg/isamples_inabox/issues/24
    date_time = dateparser.parse(
        raw_date_str, date_formats=RECOGNIZED_DATE_FORMATS, settings=DATEPARSER_SETTINGS
    )
    return date_time


def parsed_datetime_from_isamples_format(raw_date_str) -> datetime.datetime:
    """dateparser was very slow on dates like this: 2006-03-22T12:00:00Z, so roll our own"""
    components = raw_date_str.split("T")
    date = components[0]
    time = components[1]
    # ts looks like this: 2018-03-27, shockingly dateparser.parse was very slow on these
    date_pieces = date.split("-")
    # chop off the TZ string
    time = time.replace("Z", "")
    time_pieces = time.split(":")
    lastmod_date = datetime.datetime(year=int(date_pieces[0]), month=int(date_pieces[1]), day=int(date_pieces[2]),
                                     hour=int(time_pieces[0]), minute=int(time_pieces[1]), second=int(time_pieces[2]),
                                     tzinfo=None)
    return lastmod_date


def solr_delete_records(rsession, ids_to_delete: typing.List[typing.AnyStr], url):
    L = getLogger()
    headers = {"Content-Type": "application/json"}
    dicts_to_delete = []
    for id in ids_to_delete:
        dicts_to_delete.append({"id": id})
    params = {
        "delete": dicts_to_delete,
    }
    data = json.dumps(params).encode("utf-8")
    _url = f"{url}update?commit=true"
    res = rsession.post(_url, headers=headers, data=data)
    L.debug("post status: %s", res.status_code)
    L.debug("Solr update: %s", res.text)
    if res.status_code != 200:
        L.error(res.text)
        # TODO: something more elegant for error handling
        raise ValueError()
    else:
        L.debug("Successfully posted data %s to url %s", str(data), str(_url))


def solrAddRecords(rsession, records, url):
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

    # Need to strip previously generated fields to avoid solr inconsistency errors
    for record in records:
        record.pop("_version_", None)
        record.pop("producedBy_samplingSite_location_bb__minY", None)
        record.pop("producedBy_samplingSite_location_bb__minX", None)
        record.pop("producedBy_samplingSite_location_bb__maxY", None)
        record.pop("producedBy_samplingSite_location_bb__maxX", None)

        # If we don't nuke all the copy fields, they'll end up copying over multiple times
        record.pop("searchText", None)
        record.pop("description_text", None)
        record.pop("producedBy_description_text", None)
        record.pop("producedBy_samplingSite_description_text", None)
        record.pop("curation_description_text", None)

    L = getLogger()
    headers = {"Content-Type": "application/json"}
    data = json.dumps(records).encode("utf-8")
    params = {"overwrite": "true"}
    _url = f"{url}update"
    L.debug("Going to post data %s to url %s", str(data), str(_url))
    res = rsession.post(_url, headers=headers, data=data, params=params)
    L.debug("post status: %s", res.status_code)
    L.debug("Solr update: %s", res.text)
    if res.status_code != 200:
        L.error(res.text)
        # TODO: something more elegant for error handling
        raise ValueError()
    else:
        L.debug("Successfully posted data %s to url %s", str(data), str(_url))


def solrCommit(rsession, url):
    L = getLogger()
    headers = {"Content-Type": "application/json"}
    params = {"commit": "true"}
    _url = f"{url}update"
    res = rsession.get(_url, headers=headers, params=params)
    L.debug("Solr commit: %s", res.text)


def solr_max_source_updated_time(
    url: typing.AnyStr, authority_id: typing.AnyStr, rsession=requests.session()
) -> typing.Optional[datetime.datetime]:
    headers = {"Content-Type": "application/json"}
    params = {
        "q": f"source:{authority_id}",
        "sort": "sourceUpdatedTime desc",
        "rows": 1,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params)
    try:
        dict = res.json()
        docs = dict["response"]["docs"]
        if docs is not None and len(docs) > 0:
            return dateparser.parse(docs[0]["sourceUpdatedTime"])
    except Exception:
        getLogger().error("Didn't get expected JSON back from %s when fetching max source updated time for %s", _url, authority_id)

    return None


def sesar_fetch_lowercase_igsn_records(
    url: typing.AnyStr, rows: int, rsession=requests.session()
) -> typing.List[typing.Dict]:
    headers = {"Content-Type": "application/json"}
    params = {
        "q": "source:SESAR AND id:*igsn*",
        "rows": rows,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params)
    dict = res.json()
    docs = dict["response"]["docs"]
    return docs


def opencontext_fetch_broken_id_records(
    url: typing.AnyStr, rows: int, rsession=requests.session()
) -> typing.List[typing.Dict]:
    headers = {"Content-Type": "application/json"}
    params = {
        "q": "source:OPENCONTEXT AND id:http*",
        "rows": rows,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params)
    dict = res.json()
    docs = dict["response"]["docs"]
    return docs


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
        except IndexError:
            raise StopIteration
        except KeyError:
            raise StopIteration
        except TypeError:
            raise StopIteration
        except ValueError:
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


class ThingRecordIterator:
    def __init__(
        self,
        session,
        authority_id: typing.AnyStr,
        status: int = 200,
        page_size: int = 5000,
        offset: int = 0,
        min_time_created: datetime.datetime = None,
    ):
        self._session = session
        self._authority_id = authority_id
        self._status = status
        self._page_size = page_size
        self._offset = offset
        self._min_time_created = min_time_created
        self._id = offset

    def yieldRecordsByPage(self):
        while True:
            n = 0
            things = sqlmodel_database.paged_things_with_ids(
                self._session,
                self._authority_id,
                self._status,
                self._page_size,
                self._offset,
                self._min_time_created,
                self._id,
            )
            max_id_in_page = 0
            for rec in things:
                n += 1
                yield rec
                max_id_in_page = rec.primary_key
            if n == 0:
                break
            # Grab the next page, by only selecting records with _id > than the last one we fetched
            self._id = max_id_in_page


class CoreSolrImporter:
    def __init__(
        self,
        db_url: typing.AnyStr,
        authority_id: typing.AnyStr,
        db_batch_size: int,
        solr_batch_size: int,
        solr_url: typing.AnyStr,
        offset: int = 0,
        min_time_created: datetime.datetime = None,
    ):
        self._db_session = SQLModelDAO(db_url).get_session()
        self._authority_id = authority_id
        self._min_time_created = min_time_created
        self._thing_iterator = ThingRecordIterator(
            self._db_session,
            authority_id=self._authority_id,
            page_size=db_batch_size,
            offset=offset,
            min_time_created=min_time_created,
        )
        self._db_batch_size = db_batch_size
        self._solr_batch_size = solr_batch_size
        self._solr_url = solr_url

    def run_solr_import(
        self, core_record_function: typing.Callable
    ) -> typing.Set[typing.AnyStr]:
        getLogger().info(
            "importing solr records with db batch size: %s, solr batch size: %s",
            self._db_batch_size,
            self._solr_batch_size,
        )
        allkeys = set()
        rsession = requests.session()
        try:
            core_records = []
            for thing in self._thing_iterator.yieldRecordsByPage():
                try:
                    core_records_from_thing = core_record_function(thing)
                except Exception as e:
                    getLogger().error("Failed trying to run transformer, skipping record %s exception %s",
                                      thing.resolved_content, e)
                    continue

                for core_record in core_records_from_thing:
                    core_record["source"] = self._authority_id
                    core_records.append(core_record)
                for r in core_records:
                    allkeys.add(r["id"])
                batch_size = len(core_records)
                if batch_size > self._solr_batch_size:
                    solrAddRecords(
                        rsession,
                        core_records,
                        url=self._solr_url,
                    )
                    getLogger().info(
                        "Just added solr records, length of all keys is %d",
                        len(allkeys),
                    )
                    core_records = []
            if len(core_records) > 0:
                solrAddRecords(
                    rsession,
                    core_records,
                    url=self._solr_url,
                )
            solrCommit(rsession, url=self._solr_url)
            # verify records
            # for verifying that all records were added to solr
            # found = 0
            # for _id in allkeys:
            #    res = rsession.get(f"http://localhost:8983/solr/isb_rel/get?id={_id}").json()
            #    if res.get("doc",{}).get("id") == _id:
            #        found = found +1
            #    else:
            #        print(f"Missed: {_id}")
            # print(f"Found = {found}")
        finally:
            self._db_session.close()
        return allkeys
