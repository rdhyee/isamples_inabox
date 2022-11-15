from typing import Optional, Mapping

import connegp
import fastapi


# taken and adapted from https://github.com/RDFLib/pyLDAPI
from isb_lib.core import MEDIA_JSON


class ProfilesMediatypesException(ValueError):
    pass


class Profile:
    uri: str
    token: str

    def __init__(self, uri: str, token: str):
        self.uri = uri
        self.token = token


ISAMPLES_PROFILE = Profile("https://w3id.org/isample/schema", "isamples")
SOURCE_PROFILE = Profile("https://w3id.org/isample/source_record", "source")
ALL_SUPPORTED_PROFILES = [ISAMPLES_PROFILE, SOURCE_PROFILE]
DEFAULT_PROFILE = SOURCE_PROFILE
# If the _profile query string argument is this value, treat the request as a list profiles request
ALL_PROFILES_QSA_VALUE = "all"
ALT_PROFILES_QSA_VALUE = "alt"


def content_profile_headers(profile: Profile) -> dict:
    return {
        "Content-Profile": profile.uri
    }


# taken and adapted from https://github.com/RDFLib/pyLDAPI
def get_profile_from_qsa(profiles_string: Optional[str] = None) -> Optional[Profile]:
    """
    Reads _profile Query String Argument and returns the first Profile it finds
    Ref: https://www.w3.org/TR/dx-prof-conneg/#qsa-getresourcebyprofile
    :return: The profile to use, or None if not found
    :rtype: Profile
    """
    if profiles_string is not None:
        pqsa = connegp.ProfileQsaParser(profiles_string)
        if pqsa.valid:
            for profile in pqsa.profiles:
                if profile['profile'].startswith('<'):
                    # convert this valid URI/URN to a token
                    for supported_profile in ALL_SUPPORTED_PROFILES:
                        if supported_profile.uri == profile['profile'].strip('<>'):
                            return supported_profile
                else:
                    # convert this valid URI/URN to a token
                    for supported_profile in ALL_SUPPORTED_PROFILES:
                        if supported_profile.token == profile["profile"]:
                            return supported_profile

    return None


def _get_profile_from_headers(headers: Mapping) -> Optional[Profile]:
    if headers.get("Accept-Profile") is not None:
        try:
            ap = connegp.AcceptProfileHeaderParser(headers.get("Accept-Profile"))
            if ap.valid:
                for p in ap.profiles:
                    # convert this valid URI/URN to a token
                    for supported_profile in ALL_SUPPORTED_PROFILES:
                        if supported_profile.uri == p["profile"]:
                            return supported_profile
            raise Exception("No profile found")
        except Exception:
            msg = "You have requested a profile using an Accept-Profile header that is incorrectly formatted."
            raise ProfilesMediatypesException(msg)
    return None


# taken and adapted from https://github.com/RDFLib/pyLDAPI
def get_profile_from_http(request: fastapi.Request) -> Optional[Profile]:
    """
    Reads an Accept-Profile HTTP header and returns a list of Profile tokens in descending weighted order
    Ref: https://www.w3.org/TR/dx-prof-conneg/#http-getresourcebyprofile
    :return: The profile to use, or None if not found
    :rtype: Profile
    """
    return _get_profile_from_headers(request.headers)


def get_all_profiles_response_headers(base_url_str: str) -> dict:
    individual_links: list[str] = []
    for profile in ALL_SUPPORTED_PROFILES:
        if profile == DEFAULT_PROFILE:
            rel = "canonical"
        else:
            rel = "alternate"
        individual_links.append(f"<{base_url_str}?_profile={profile.token}>; "
                                f"rel=\"{rel}\"; type=\"{MEDIA_JSON}\"; "
                                f"profile=\"{profile.uri}\", ")
    link_header_value = "".join(individual_links).rstrip(", ")
    return {
        "Content-Profile": "<http://www.w3.org/ns/dx/conneg/altr>",
        "Link": link_header_value
    }
