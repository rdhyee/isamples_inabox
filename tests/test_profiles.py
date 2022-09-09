import pytest

from isb_web.profiles import get_profile_from_qsa, get_all_profiles_response_headers, _get_profile_from_headers, ProfilesMediatypesException


def test_get_profile_from_qsa_isamples():
    profile = get_profile_from_qsa("isamples")
    assert profile is not None


def test_get_profile_from_qsa_source():
    profile = get_profile_from_qsa("source")
    assert profile is not None


def test_get_profile_from_qsa_isamples_uri():
    profile = get_profile_from_qsa("<https://w3id.org/isample/schema>")
    assert profile is not None


def test_get_all_profiles_response_headers():
    headers = get_all_profiles_response_headers("https://hyde.cyverse.org/")
    assert len(headers) > 0


def test_get_profile_from_header():
    headers = {
        "Accept-Profile": "<https://w3id.org/isample/schema>"
    }
    profile = _get_profile_from_headers(headers)
    assert profile is not None


def test_get_profile_from_header_invalid():
    with pytest.raises(ProfilesMediatypesException):
        headers = {
            "Accept-Profile": "123456"
        }
        _get_profile_from_headers(headers)
