import datetime
import json
import typing
import logging
import requests
from requests import Response
from requests.auth import HTTPBasicAuth

from isb_lib.core import parsed_date
from isb_lib.identifiers.identifier import DataciteIdentifier, IGSNIdentifier

CONTENT_TYPE = "application/vnd.api+json"
DATACITE_URL = "https://api.test.datacite.org"


def _dois_headers():
    headers = {"content-type": CONTENT_TYPE}
    return headers


def _dois_auth(password, username):
    auth = HTTPBasicAuth(username, password)
    return auth


def _validate_response(response) -> bool:
    if response.status_code < 200 or response.status_code >= 300:
        logging.error(
            "Error requesting new DOI, status code: %s, response %s",
            response.status_code,
            str(response.json()),
        )
        return False
    return True


def _post_to_datacite(
    rsession: requests.Session, post_data_str: bytes, username: str, password: str
) -> Response:
    response = rsession.post(
        dois_url(),
        headers=_dois_headers(),
        data=post_data_str,
        auth=_dois_auth(password, username),
    )
    return response


def _doi_or_none(response: Response) -> typing.Optional[str]:
    if not _validate_response(response):
        return None
    json_response = response.json()
    draft_id = json_response["data"]["id"]
    # use the DOI prefix since we creating DOIs with datacite
    return doi_from_id(draft_id)


# See datacite docs here: https://support.datacite.org/docs/api-create-dois
def datacite_metadata_from_core_record(
    prefix: typing.Optional[str], doi: typing.Optional[str], igsn: bool, authority: str, core_record: dict
) -> dict:
    # Datacite requires the following fields:
    # DOI or prefix -- handled via params
    registrant = core_record.get("registrant")
    if registrant is None:
        raise ValueError("Registrant is a required field in order to register a DOI")
    # creators -- mapped from registrant?
    # title -- mapped from label?
    label = core_record.get("label")
    if label is None:
        raise ValueError("Label is a required field in order to register a DOI")
    # publisher -- mapped from authority -- where does the authority come from?
    #           -- alternatively we could read this somewhere out of the data
    # publicationYear -- mapped from producedBy_resultTime, if not present defaults to current year
    produced_by = core_record.get("producedBy")
    publication_year = datetime.datetime.now().year
    if produced_by is not None:
        raw_result_time = produced_by.get("resultTime")
        if raw_result_time is not None:
            parsed_result_time = parsed_date(raw_result_time)
            if parsed_result_time is not None:
                publication_year = parsed_result_time.year
    if igsn:
        identifier = DataciteIdentifier(doi, prefix, [registrant], [label], authority, publication_year)
    else:
        identifier = IGSNIdentifier(doi, prefix, [registrant], [label], authority, publication_year)
    datacite_metadata = {"data": {"type": "dois", "attributes": identifier.metadata_dict()}}
    return datacite_metadata


def dois_url():
    url = f"{DATACITE_URL}/dois"
    return url


def create_draft_doi(
    rsession: requests.Session,
    prefix: str,
    doi: str,
    igsn: bool,
    username: str,
    password: str,
) -> typing.Optional[str]:
    attribute_dict = _attribute_dict_with_doi_or_prefix(doi, prefix)
    data_dict = {"type": "dois", "attributes": attribute_dict}
    request_data = {"data": data_dict}
    post_data_str = json.dumps(request_data).encode("utf-8")
    response = _post_to_datacite(rsession, post_data_str, username, password)
    return _doi_or_none(response)


def _attribute_dict_with_doi_or_prefix(doi, prefix):
    attribute_dict = {}
    if doi is not None:
        attribute_dict["doi"] = doi
    else:
        attribute_dict["prefix"] = prefix
    return attribute_dict


def create_doi(
    rsession: requests.Session, json_data: bytes, username: str, password: str
) -> typing.Optional[str]:
    response = _post_to_datacite(rsession, json_data, username, password)
    return _doi_or_none(response)


def doi_from_id(raw_id: str) -> str:
    return f"doi:{raw_id}"
