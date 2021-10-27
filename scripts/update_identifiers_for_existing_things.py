from time import sleep

import click
import click_config_file
from sqlmodel import select

import isb_lib
import isb_lib.core
from isb_lib.models.thing import Thing, ThingIdentifier
from isb_web.sqlmodel_database import SQLModelDAO

arks_to_bp = [
    "ark:/21547/Duf242514eacdfe6ca78a925008f7c003082",
    "ark:/21547/DsN2abdc7db9aa0e9d1b24061c87005b561e",
    "ark:/21547/Dsw2T1um44.1",
]
opencontext_ids = [
    "http://opencontext.org/subjects/5d917e24-52ec-4c10-8a51-a1586c1c451f",
    "http://opencontext.org/subjects/DD2E7987-6313-4FAD-C4BE-A8980D181854",
]


def insert_geome_identifiers(session, thing):
    # For now, we will fail all requests for parent IDs, because events appear in multiple samples
    # and would violate referential integrity if we made pointers to children from the event ID
    # parent = thing.resolved_content.get("parent")
    # if parent is not None:
    #     event_ark = parent["bcid"]
    #     if event_ark in arks_to_bp:
    #         print()
    #     event_identifier = ThingIdentifier(guid=event_ark, thing_id=thing.primary_key)
    #     session.add(event_identifier)
    children = thing.resolved_content.get("children")
    if children is not None:
        for child in children:
            child_ark = child["bcid"]
            if child_ark in arks_to_bp:
                print()
            child_identifier = ThingIdentifier(
                guid=child_ark, thing_id=thing.primary_key
            )
            session.add(child_identifier)


def insert_open_context_identifiers(session, thing):
    citation_uri = thing.resolved_content["citation uri"]
    if citation_uri is not None and type(citation_uri) is str:
        open_context_uri = isb_lib.normalized_id(citation_uri)
        open_context_identifier = ThingIdentifier(
            guid=open_context_uri, thing_id=thing.primary_key
        )
        if open_context_uri in arks_to_bp:
            print()
        session.add(open_context_identifier)


def insert_standard_identifier(session, thing):
    if thing.id in arks_to_bp:
        print()
    thing_identifier = ThingIdentifier(guid=thing.id, thing_id=thing.primary_key)
    session.add(thing_identifier)


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnositcs on 9999"
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, None, verbosity, heart_rate)
    session = SQLModelDAO((ctx.obj["db_url"])).get_session()
    # start off cleaning out any existing records
    rows_deleted = session.query(ThingIdentifier).delete()
    print("Deleted %d rows before recreating entries", rows_deleted)
    session.commit()
    sleep(10)
    index = 0
    page_size = 10000
    max_index = 150000
    last_thing_id = 0
    while index < max_index:
        statement = select(Thing)
        if last_thing_id > 0:
            statement = statement.where(Thing.primary_key > last_thing_id)
        statement = statement.order_by(Thing.primary_key.asc()).limit(page_size)
        batch_count = 0
        for thing in session.exec(statement):
            if thing.authority_id == "GEOME":
                insert_geome_identifiers(session, thing)
            elif thing.authority_id == "OPENCONTEXT":
                insert_open_context_identifiers(session, thing)
            insert_standard_identifier(session, thing)
            last_thing_id = thing.primary_key
            batch_count += 1
        session.commit()
        if batch_count < page_size - 1:
            break
        index += page_size
        print("About to start index " + str(index))


if __name__ == "__main__":
    main()
