import click
import click_config_file
from sqlmodel import Session, select

import isb_lib
import isb_lib.core
import isb_lib.sesar_adapter
import isb_lib.geome_adapter
import isb_lib.opencontext_adapter
import isb_lib.smithsonian_adapter
from isamples_metadata import (
    SESARTransformer,
    GEOMETransformer,
    OpenContextTransformer,
    SmithsonianTransformer,
)
from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import SQLModelDAO

BATCH_SIZE = 50000


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
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, verbosity):
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    session = SQLModelDAO(db_url).get_session()
    assign_h3(session)


def assign_h3(session: Session):
    authorities = {
        isb_lib.smithsonian_adapter.SmithsonianItem.AUTHORITY_ID: SmithsonianTransformer.geo_to_h3,
        isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID: SESARTransformer.geo_to_h3,
        isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID: GEOMETransformer.GEOMETransformer.geo_to_h3,
        isb_lib.opencontext_adapter.OpenContextItem.AUTHORITY_ID: OpenContextTransformer.geo_to_h3,
    }
    for authority, geo_to_h3 in authorities.items():
        thing_select = things_no_h3_select(authority)
        print(f"Starting h3 migration for {authority}")
        i = 0
        update_batch_values = []
        last_id = None
        while True:
            if last_id is not None:
                thing_select = things_no_h3_select(authority)
                thing_select = thing_select.filter(Thing.primary_key > last_id)
            results = session.exec(thing_select).all()
            if len(results) == 0:
                # hit the end
                break
            for row in results:
                i += 1
                primary_key = row[0]
                # Guard against degenerate records in the database -- could happen if there are problems with the
                # initial import
                if row[1] is not None:
                    h3 = geo_to_h3(row[1])
                    if h3 is not None:
                        update_batch_values.append({"primary_key": primary_key, "h3": h3})
                last_id = primary_key
            # It's possible the length could be 0 since we don't have lat/lon for all records
            if len(update_batch_values) > 0:
                save_batch(session, update_batch_values)
            else:
                print("Skipping update batch as all h3 values were None")
            update_batch_values = []


def things_no_h3_select(authority):
    thing_select = (
        select(Thing.primary_key, Thing.resolved_content)
        .limit(BATCH_SIZE)
        .order_by(Thing.primary_key.asc())
        .filter(Thing.authority_id == authority)
        .filter(Thing.h3 == None)  # noqa: E711
    )
    return thing_select


def save_batch(session, update_batch_values):
    print(f"About to bulk update h3 value for {len(update_batch_values)} things")
    session.bulk_update_mappings(mapper=Thing, mappings=update_batch_values)
    print("Done.  Committing.")
    session.commit()


"""
Assign h3 values for existing Things
"""
if __name__ == "__main__":
    main()
