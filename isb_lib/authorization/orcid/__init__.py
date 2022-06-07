import logging

import requests
from requests import Response

ORCID_API_CONTENT_TYPE = "application/orcid+json"
ORCID_API_VERSION = "2.1"
ORCID_API_ENDPOINT = "record"
ORCID_API_HOSTNAME = "pub.sandbox.orcid.org"


def _orcid_auth_url(orcid_id: str) -> str:
    return f"https://{ORCID_API_HOSTNAME}/v{ORCID_API_VERSION}/{orcid_id}/{ORCID_API_ENDPOINT}"


def _orcid_auth_headers(token: str) -> dict:
    return {"Content-Type": ORCID_API_CONTENT_TYPE, "Authorization": f"Bearer {token}"}


def _orcid_auth_response(
    rsession: requests.Session, token: str, orcid_id: str
) -> Response:
    response = rsession.get(
        _orcid_auth_url(orcid_id),
        headers=_orcid_auth_headers(token),
    )
    return response


def authorize_token_for_orcid_id(
    token: str, orcid_id: str, rsession: requests.Session = requests.session()
) -> bool:
    response = _orcid_auth_response(rsession, token, orcid_id)
    if response.status_code == 200:
        return True
    else:
        logging.error(
            f"Error checking token {token} for orcid {orcid_id}, response: {response.json()}"
        )
        return False
