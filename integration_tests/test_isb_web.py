import typing

import requests
import pytest
import os


@pytest.fixture
def rsession():
    return requests.session()


@pytest.fixture
def hostname():
    hostname = os.getenv("INPUT_HOSTNAME")
    if hostname is None:
        hostname = "https://hyde.cyverse.org/isamples_central/"
    # ensure the hostname is properly ended, since we construct the URLs by hand
    if not hostname.endswith("/"):
        hostname = hostname + "/"
    return hostname


@pytest.fixture
def headers():
    return {"accept": "application/json", "User-Agent": "iSamples Integration Bot 2000"}


authority_ids = [
    "SESAR",
    "OPENCONTEXT",
    "GEOME",
    "SMITHSONIAN",
]


def _get_things_with_authority(
    rsession: requests.Session,
    hostname: str,
    headers: typing.Dict,
    authority_id: str,
    limit: int,
) -> typing.List:
    url = f"{hostname}thing/"
    params: dict = {
        "limit": limit,
        "authority": authority_id,
        "status": 200,
    }
    res = rsession.get(url, headers=headers, params=params)
    response_dict = res.json()
    things = response_dict.get("data")
    return things


@pytest.mark.parametrize("authority_id", authority_ids)
def test_thing_list(
    rsession: requests.Session,
    hostname: str,
    headers: typing.Dict,
    authority_id: str,
):
    things = _get_things_with_authority(rsession, hostname, headers, authority_id, 10)
    for thing in things:
        id = thing["id"]
        assert id is not None
        assert thing["resolved_url"] is not None
        assert thing["resolved_status"] == 200


formats = ["original", "core"]


@pytest.mark.parametrize("authority_id", authority_ids)
@pytest.mark.parametrize("format", formats)
def test_get_thing(
    rsession: requests.Session,
    hostname: str,
    headers: typing.Dict,
    authority_id: str,
    format: str,
):
    first_thing = _get_things_with_authority(
        rsession, hostname, headers, authority_id, 1
    )[0]
    first_thing_id = first_thing["id"]
    url = f"{hostname}thing/{first_thing_id}"
    params = {"format": format}
    res = rsession.get(url, headers=headers, params=params)
    response_dict = res.json()
    assert len(response_dict) > 0
