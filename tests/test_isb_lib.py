import datetime
import pytz

from isb_lib import datetimeToJsonStr, jsonDumps


def test_datetime_to_json_str():
    now = datetime.datetime.now()
    datetime_str = datetimeToJsonStr(now)
    assert datetime_str is not None


def test_datetime_to_json_str_with_timezone():
    now = datetime.datetime.now(tz=pytz.timezone("US/Eastern"))
    datetime_str = datetimeToJsonStr(now)
    assert datetime_str is not None


def test_datetime_to_json_str_none():
    datetime_str = datetimeToJsonStr(None)
    assert datetime_str is None


def test_json_dumps():
    test_obj = {
        "key1": "value1",
        "key2": datetime.datetime.now()
    }
    json_str = jsonDumps(test_obj)
    assert json_str is not None
