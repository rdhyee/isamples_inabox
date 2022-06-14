import logging
from typing import Optional

import requests
from requests import Response
from isb_web import config

ORCID_API_CONTENT_TYPE = "application/orcid+json"
ORCID_API_VERSION = "2.1"
ORCID_API_ENDPOINT = "record"
ORCID_API_HOSTNAME = config.Settings().orcid_hostname
ORCID_TOKEN_REDIRECT_URI = config.Settings().orcid_token_redirect_uri
ORCID_CLIENT_ID = config.Settings().orcid_client_id
ORCID_CLIENT_SECRET = config.Settings().orcid_client_secret
ORCID_CODE_GRANT_TYPE = "authorization_code"


def _orcid_token_url():
    return f"https://{ORCID_API_HOSTNAME}/oauth/token"


def _orcid_auth_url(orcid_id: str) -> str:
    return f"https://{ORCID_API_HOSTNAME}/v{ORCID_API_VERSION}/{orcid_id}/{ORCID_API_ENDPOINT}"


def _orcid_auth_headers(token: str) -> dict:
    # Be flexible about downstream clients including the Bearer prefix or not
    bearer_prefix = "Bearer "
    if not token.startswith(bearer_prefix):
        token = f"{bearer_prefix}{token}"
    return {"Content-Type": ORCID_API_CONTENT_TYPE, "Authorization": token}


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


def _orcid_post_body(code: str) -> dict:
    return {
        "code": code,
        "redirect_uri": ORCID_TOKEN_REDIRECT_URI,
        "client_id": ORCID_CLIENT_ID,
        "client_secret": ORCID_CLIENT_SECRET,
        "grant_type": ORCID_CODE_GRANT_TYPE,
    }


def _orcid_token_response(rsession: requests.Session, code: str) -> Response:
    body = _orcid_post_body(code)
    response = rsession.post(_orcid_token_url(), data=body)
    return response


def exchange_code_for_token(
    code: str, rsession: requests.Session = requests.session()
) -> Optional[dict]:
    response = _orcid_token_response(rsession, code)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(
            f"Error converting authorization code: {code} to token.  Response: {response.json()}"
        )
        return None
