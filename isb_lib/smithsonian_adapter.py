import typing
import datetime

import isamples_metadata.SmithsonianTransformer

import isb_lib.core
import igsn_lib.models.thing
import logging


class SmithsonianItem(object):
    AUTHORITY_ID = "SMITHSONIAN"
    TEXT_CSV = "text/csv"

    def __init__(self, identifier: str, source_dict: typing.Dict):
        self.identifier = identifier
        self.source_item = source_dict

    def as_thing(
        self,
        t_created: datetime.datetime,
        status: int,
        source_file_path: str,
        t_resolved: datetime.datetime,
    ) -> igsn_lib.models.thing.Thing:
        logging.debug("SmithsonianItem.asThing")
        _thing = igsn_lib.models.thing.Thing(
            id=self.identifier,
            tcreated=t_created,
            item_type=None,
            authority_id=SmithsonianItem.AUTHORITY_ID,
            resolved_url=f"file://{source_file_path}",
            resolved_status=status,
            tresolved=t_resolved,
            resolve_elapsed=0,
        )
        if not isinstance(self.source_item, dict):
            L.error("Item is not an object")
            return _thing
        _thing.item_type = "sample"
        _thing.related = None
        _thing.resolved_media_type = SmithsonianItem.TEXT_CSV
        # Note that this field doesn't make sense for Smithsonian as the information is coming from a local file
        # _thing.resolve_elapsed = resolve_elapsed
        _thing.resolved_content = self.source_item
        return _thing


def load_thing(
    thing_dict: typing.Dict, t_resolved: datetime.datetime, file_path: typing.AnyStr
) -> igsn_lib.models.thing.Thing:
    """
    Load a thing from its source.

    Minimal parsing of the thing is performed to populate the database record.

    Args:
        thing_dict: Dictionary representing the thing
        t_resolved: When the item was resolved from the source

    Returns:
        Instance of Thing
    """
    L = isb_lib.core.getLogger()
    id = thing_dict["id"]
    try:
        t_created = datetime.datetime(
            year=int(thing_dict["year"]),
            month=int(thing_dict["month"]),
            day=int(thing_dict["day"]),
        )
    except ValueError as e:
        # In many cases, these don't seem to be populated.  There's nothing we can do if they aren't there, so just
        # leave it as None.
        t_created = None
    L.info("loadThing: %s", id)
    item = SmithsonianItem(id, thing_dict)
    thing = item.as_thing(t_created, 200, file_path, t_resolved)
    return thing


def _validate_resolved_content(thing: igsn_lib.models.thing.Thing):
    isb_lib.core.validate_resolved_content(SmithsonianItem.AUTHORITY_ID, thing)


def reparse_as_core_record(thing: igsn_lib.models.thing.Thing) -> typing.Dict:
    _validate_resolved_content(thing)
    try:
        transformer = isamples_metadata.SmithsonianTransformer.SmithsonianTransformer(
            thing.resolved_content
        )
        return isb_lib.core.coreRecordAsSolrDoc(transformer.transform())
    except Exception as e:
        logging.fatal(
            "Failed trying to run transformer on %s", str(thing.resolved_content)
        )
        raise
