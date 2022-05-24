import datetime
from io import BufferedReader

import click
import click_config_file
from click import Context

import isb_lib.core
import isb_lib.identifiers.datacite as datacite
import requests
import logging
import json

from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import SQLModelDAO, save_draft_thing_with_id, save_thing


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url):
    isb_lib.core.things_main(ctx, db_url, None)


@main.command("create_draft_doi")
@click.option(
    "--num_dois",
    type=int,
    default=1,
    help="Number of draft dois to create.",
)
@click.option(
    "--prefix",
    type=str,
    default=None,
    help="The datacite prefix to use when creating identifiers.",
)
@click.option("--doi", type=str, default=None, help="The full DOI to register")
@click.option("--igsn", is_flag=True)
@click.option(
    "--username",
    type=str,
    default=None,
    help="The datacite username to use when creating identifiers.",
)
@click.password_option(hide_input=True)
@click.pass_context
def create_draft_dois(
    ctx: Context, num_dois: int, prefix: str, doi: str, igsn: bool, username: str, password: str
):
    # If the doi is specified, then num_identifiers can be only 1
    if doi is not None:
        num_dois = 1
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    for i in range(num_dois):
        draft_id = datacite.create_draft_doi(
            requests.session(), prefix, doi, igsn, username, password
        )
        if draft_id is not None:
            logging.info("Successfully created draft DOI %s", draft_id)
            draft_thing = save_draft_thing_with_id(session, draft_id)
            logging.info("Successfully saved draft thing with id %s", draft_thing.id)


@main.command("create_doi")
@click.option("--file", type=click.File("r"))
@click.option(
    "--prefix",
    type=str,
    default=None,
    help="The datacite prefix to use when creating identifiers.",
)
@click.option("--doi", type=str, default=None, help="The full DOI to register")
@click.option("--igsn", is_flag=True)
@click.option(
    "--username",
    type=str,
    default=None,
    help="The datacite username to use when creating identifiers.",
)
@click.password_option(hide_input=True)
@click.pass_context
def create_doi(ctx: Context, file: BufferedReader, prefix: str, doi: str, igsn: bool, username: str, password: str):
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    file_contents = file.read()
    file_contents_dict = json.loads(file_contents)
    datacite_metadata_dict = datacite.datacite_metadata_from_core_record(
        prefix, doi, igsn, "AUTHORITY", file_contents_dict
    )
    # TODO WE HAVE THE SAME ENCODING ELSEWHERE IN DATACITE CODE SHOULD STANDARDIZE
    result = datacite.create_doi(
        requests.session(),
        json.dumps(datacite_metadata_dict).encode("utf-8"),
        username,
        password,
    )
    logging.info("Successfully saved DOI to DataCite %s", result)
    if result is not None:
        new_thing = Thing()
        new_thing.id = result
        new_thing.resolved_status = 200
        # TODO: pipe in authority -- from where?
        new_thing.authority_id = "AUTHORITY"
        new_thing.resolved_url = datacite.dois_url()
        new_thing.resolved_content = file_contents_dict
        new_thing.tcreated = datetime.datetime.now()
        save_thing(session, new_thing)
        logging.info("Sucessfully saved thing with id %s", result)
    else:
        logging.error(
            "Unable to save DOI to DataCite.  See console log for additional information."
        )


if __name__ == "__main__":
    main()
