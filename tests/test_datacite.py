import json
import pytest
from requests import Response

import isb_lib.identifiers.datacite as datacite
from isb_lib.identifiers.datacite import _dois_headers, _dois_auth, _validate_response, _doi_or_none, dois_url, \
    doi_from_id, _attribute_dict_with_doi_or_prefix, _datacite_post_bytes
from isb_lib.identifiers.identifier import DataciteIdentifier, IGSNIdentifier


def test_datacite_metadata():
    test_file_path = "./test_data/DOIS/thing1.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        prefix = "123456"
        authority = "AUTHORITY"
        doi_metadata = datacite.datacite_metadata_from_core_record(prefix, None, False, authority, source_record)
        assert doi_metadata is not None
        data_dict = doi_metadata.get("data")
        assert data_dict is not None
        attributes = data_dict.get("attributes")
        assert prefix == attributes.get("prefix")
        assert authority == attributes.get("publisher")
        assert 2006 == attributes.get("publicationYear")
        assert "Curator Integrated Ocean Drilling Program (TAMU)" == attributes.get("creators")[0]
        assert "Sample 178-1098B-1H-1 (143-144 cm.)" == attributes.get("titles")[0]["title"]


def test_datacite_metadata_missing_label():
    test_file_path = "./test_data/DOIS/missing_label.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        with pytest.raises(ValueError):
            datacite.datacite_metadata_from_core_record("prefix", None, False, "authority", source_record)


def test_dois_headers():
    headers = _dois_headers()
    assert len(headers) > 0


def test_dois_auth():
    auth = _dois_auth("password", "username")
    assert auth is not None


def test_validate_good_response():
    response = _good_response()
    assert _validate_response(response) is True


def _good_response():
    response = Response()
    response.status_code = 200
    return response


def test_validate_bad_response():
    response = _bad_response()
    assert _validate_response(response) is False


def _bad_response():
    response = Response()
    response.status_code = 404
    return response


def test_doi_or_none_bad():
    bad_response = _bad_response()
    assert _doi_or_none(bad_response) is None


def test_dois_url():
    assert dois_url() is not None


def test_doi_from_id():
    assert doi_from_id("123456") is not None


def test_attribute_dict_with_doi_or_prefix_only_doi():
    assert len(_attribute_dict_with_doi_or_prefix("doi", None)) > 0


def test_attribute_dict_with_doi_or_prefix_only_prefix():
    assert len(_attribute_dict_with_doi_or_prefix(None, "prefix")) > 0


def test_datacite_post_bytes():
    datacite_post_bytes = _datacite_post_bytes("123456", None)
    assert len(datacite_post_bytes) > 0


def test_datacite_identifier_bad_args():
    with pytest.raises(ValueError):
        DataciteIdentifier(None, None, [], [], "", 2022)
    with pytest.raises(ValueError):
        DataciteIdentifier("doi", None, [], [], "", 2022)
    with pytest.raises(ValueError):
        DataciteIdentifier("doi", None, ["creator 1"], [], "", 2022)


def test_datacite_identifier_good_args():
    datacite_id = DataciteIdentifier("doi", None, ["creator 1"], ["title 1"], "publisher", 2022)
    assert datacite_id._is_doi is True
    assert datacite_id.__str__() is not None
    assert len(datacite_id.metadata_dict()) > 0
    prefix_datacite_id = DataciteIdentifier(None, "prefix", ["creator 1"], ["title 1"], "publisher", 2022)
    assert prefix_datacite_id._is_doi is False
    assert prefix_datacite_id.__str__() is not None
    assert len(datacite_id.metadata_dict()) > 0


def test_igsn_identifier_good_args():
    datacite_id = IGSNIdentifier("doi", None, ["creator 1"], ["title 1"], "publisher", 2022)
    assert datacite_id._is_doi is True
    assert datacite_id.__str__() is not None
    assert len(datacite_id.metadata_dict()) > 0
    prefix_datacite_id = IGSNIdentifier(None, "prefix", ["creator 1"], ["title 1"], "publisher", 2022)
    assert prefix_datacite_id._is_doi is False
    assert prefix_datacite_id.__str__() is not None
    assert len(datacite_id.metadata_dict()) > 0
