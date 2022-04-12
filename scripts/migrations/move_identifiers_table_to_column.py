import json
import logging

import click

import isb_lib.core
from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import SQLModelDAO, all_thing_identifier_objects, things_with_null_identifiers, \
    insert_identifiers


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
@click.pass_context
def main(ctx, db_url, verbosity):
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    session = SQLModelDAO((ctx.obj["db_url"]), echo=True).get_session()
    transform_thing_identifiers(session)
    update_null_thing_identifiers(session)


def update_null_thing_identifiers(session):
    """Hit any remainders that didn't get their identifiers updated in the first batch"""
    things = things_with_null_identifiers(session)
    for thing in things:
        insert_identifiers(thing)
        session.add(thing)
    session.commit()


def transform_thing_identifiers(session):
    last_id = 0
    batch_size = 100000
    total_processed = 0
    all_thing_identifiers = all_thing_identifier_objects(session, last_id, batch_size)
    current_identifier_strings = []
    while len(all_thing_identifiers) > 0:
        update_batch_values = []

        last_thing_id = None
        for identifier in all_thing_identifiers:
            if len(current_identifier_strings) == 0 or identifier.thing_id == last_thing_id:
                current_identifier_strings.append(identifier.guid)
            else:
                append_identifiers(current_identifier_strings, last_thing_id, update_batch_values)
                current_identifier_strings = [identifier.guid]
            last_thing_id = identifier.thing_id
        append_identifiers(current_identifier_strings, last_thing_id, update_batch_values)
        total_processed += batch_size
        logging.info(f"About to run bulk update of {batch_size} records")
        session.bulk_update_mappings(
            mapper=Thing, mappings=update_batch_values
        )
        session.commit()
        logging.info(f"Finished bulk update, have processed {total_processed} records")
        if last_id == last_thing_id:
            # Hit loop termination condition because we didn't advance the identifier
            logging.info(f"Done.  Processed {total_processed} records")
            break
        last_id = last_thing_id
        all_thing_identifiers = all_thing_identifier_objects(session, last_id, batch_size)
        current_identifier_strings = []
        logging.info(f"Selected next {batch_size} identifiers")


def append_identifiers(current_identifier_strings, last_thing_id, update_batch_values):
    identifiers_json = json.dumps(current_identifier_strings)
    update_batch_values.append(
        {
            "primary_key": last_thing_id,
            "identifiers": identifiers_json
        }
    )


"""
Creates a text dump of the source collection file, one line of text per record
"""
if __name__ == "__main__":
    main()
