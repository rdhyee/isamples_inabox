import logging

import click
import isb_lib.core
from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import SQLModelDAO, all_thing_identifiers


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
    update_opencontext_ids(session)


def update_opencontext_ids(session):
    thing_ids_to_pks = all_thing_identifiers(session, "OPENCONTEXT")
    update_batch_values = []
    batch_size = 100000
    total_processed = 0
    num_processed = 0
    for key, val in thing_ids_to_pks.items():
        num_processed += 1
        if not key.startswith("http"):
            update_batch_values.append(
                {
                    "primary_key": val,
                    "id": key
                }
            )
        if num_processed % batch_size == 0:
            logging.info(f"About to run bulk update of {len(update_batch_values)} records")
            session.bulk_update_mappings(
                mapper=Thing, mappings=update_batch_values
            )
            session.commit()
            total_processed += len(update_batch_values)
            num_processed = 0
            update_batch_values = []
    session.bulk_update_mappings(
        mapper=Thing, mappings=update_batch_values
    )
    session.commit()
    total_processed += len(update_batch_values)
    logging.info(f"Processed {total_processed} records")


"""
Updates OpenContext identifiers to ARKs
"""
if __name__ == "__main__":
    main()
