import pytest
import requests
import typing
import os


@pytest.fixture
def rsession():
    return requests.session()


@pytest.fixture
def solr_url():
    solr_url = os.getenv("INPUT_SOLR_URL")
    if solr_url is None:
        solr_url = "https://mars.cyverse.org/thing/select"
    return solr_url


def headers():
    return {
        "accept": "application/json",
        "User-Agent": "iSamples SOLR Integration Bot 2000",
    }


authority_ids = [
    "SESAR",
    "OPENCONTEXT",
    "GEOME",
    "SMITHSONIAN",
]


def _send_solr_query(
    rsession: requests.Session, solr_url: str, query: str
) -> typing.List[typing.Dict]:
    params: dict = {
        "start": 0,
        "limit": 10,
        "fl": "*",
        "q": f"searchText:({query})",
    }
    res = rsession.get(solr_url, headers=headers(), params=params)
    response_dict = res.json()
    docs = response_dict.get("response").get("docs")
    return docs


def _value_in_search_text(value: str, doc: typing.Dict) -> bool:
    appears_in_search_text = False
    for searchText in doc["searchText"]:
        if value.lower() in searchText.lower():
            appears_in_search_text = True
            break
    return appears_in_search_text


@pytest.mark.parametrize("authority_id", authority_ids)
def test_solr_query_by_source(
    rsession: requests.Session, solr_url: str, authority_id: str
):
    docs = _send_solr_query(rsession, solr_url, authority_id)
    for doc in docs:
        appears_in_search_text = _value_in_search_text(authority_id, doc)
        assert appears_in_search_text


opencontext_projects = [
    "Çatalhöyük Zooarchaeology",
    "Petra Great Temple Excavations",
    # Commented out due to https://github.com/isamplesorg/isamples_inabox/issues/102
    # "Excavations at Polis",
    "Avkat Archaeological Project",
    "Kenan Tepe",
    "Giza Botanical Database",
]


@pytest.mark.parametrize("project_label", opencontext_projects)
def test_opencontext_projects(
    rsession: requests.Session, solr_url: str, project_label: str
):
    docs = _send_solr_query(rsession, solr_url, project_label)
    for doc in docs:
        for word in project_label.split():
            appears_in_search_text = _value_in_search_text(word, doc)
            assert appears_in_search_text
            assert doc["source"] == "OPENCONTEXT"


geome_search_terms = [
    "SYMBIOCODE",
    "INDO_PIRE",
    "PEER_2016",
    "gastropoda",
]


@pytest.mark.parametrize("geome_search_term", geome_search_terms)
def test_geome_search_terms(
    rsession: requests.Session, solr_url: str, geome_search_term: str
):
    docs = _send_solr_query(rsession, solr_url, geome_search_term)
    for doc in docs:
        for word in geome_search_term.split():
            appears_in_search_text = _value_in_search_text(word, doc)
            assert appears_in_search_text
            assert doc["source"] == "GEOME"


geographic_search_terms = [
    "New Zealand",
    "United States",
    "Moorea",
    "Bali",
    "Italy",
]


@pytest.mark.parametrize("geographic_search_term", geographic_search_terms)
def test_geographic_search_terms(
    rsession: requests.Session, solr_url: str, geographic_search_term: str
):
    docs = _send_solr_query(rsession, solr_url, geographic_search_term)
    for doc in docs:
        appears_in_search_text = _value_in_search_text(geographic_search_term, doc)
        assert appears_in_search_text
