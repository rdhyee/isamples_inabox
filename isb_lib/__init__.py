import json
import datetime

JSON_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
"""datetime format string for generating JSON content
"""

def datetimeToJsonStr(dt):
    """Convert datetime to a JSON compatible string"""
    if dt is None:
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        # Naive timestamp, convention is this must be UTC
        return f"{dt.strftime(JSON_TIME_FORMAT)}Z"
    return dt.strftime(JSON_TIME_FORMAT)


def _jsonConverter(o):
    if isinstance(o, datetime.datetime):
        return datetimeToJsonStr(o)
    return o.__str__()

def jsonDumps(obj):
    """Dump object as JSON, handling date conversion"""
    return json.dumps(obj, indent=2, default=_jsonConverter, sort_keys=True)