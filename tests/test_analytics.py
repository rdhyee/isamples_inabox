import pytest
import fastapi
import starlette.datastructures
import json
import isb_web.analytics
from isb_web.analytics import AnalyticsEvent
from isb_lib.core import MEDIA_JSON

TEST_USER_AGENT = "user-agent"
TEST_FORWARDED_FOR = "1.2.3.4"
TEST_REFERRER = "a_referrer"
TEST_URL = "http://foo.bar"


@pytest.fixture()
def request_fixture() -> fastapi.Request:
    scope = {"type": "http"}
    request = fastapi.Request(scope)

    headers = {
        "user-agent": TEST_USER_AGENT,
        "x-forwarded-for": TEST_FORWARDED_FOR,
        "referer": TEST_REFERRER,
    }
    request.__setattr__("_headers", headers)
    request.__setattr__("_url", starlette.datastructures.URL(TEST_URL))
    return request


def test_analytics_request_headers(request_fixture: fastapi.Request):
    headers = isb_web.analytics._analytics_request_headers(request_fixture)
    assert headers is not None
    assert headers["Content-Type"] == MEDIA_JSON
    assert headers["User-Agent"] == TEST_USER_AGENT
    assert headers["X-Forwarded-For"] == TEST_FORWARDED_FOR


def test_analytics_request_data(request_fixture: fastapi.Request):
    data = isb_web.analytics._analytics_request_data(
        AnalyticsEvent.THING_LIST, request_fixture, None
    )
    assert data is not None
    assert data["name"] == AnalyticsEvent.THING_LIST.value
    assert data["domain"] == "UNSET"
    assert data["url"] == TEST_URL
    assert data["referrer"] == TEST_REFERRER
    assert data.get("props") is None


def test_analytics_request_data_custom_properties(request_fixture: fastapi.Request):
    props = {"foo": "bar"}
    data = isb_web.analytics._analytics_request_data(
        AnalyticsEvent.THING_LIST, request_fixture, props
    )
    props_string = data.get("props")
    assert props_string is not None
    props_dict = json.loads(props_string)
    assert props_dict["foo"] == "bar"
