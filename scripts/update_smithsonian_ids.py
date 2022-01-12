import logging
import typing
import click
import click_config_file
import isb_lib.core
from sqlalchemy import select
from sqlalchemy import update
import igsn_lib.models
import igsn_lib.models.thing


def _fixed_smithsonian_id(id: typing.AnyStr) -> typing.AnyStr:
    id_no_n2t = id.removeprefix("http://n2t.net/")
    return id_no_n2t


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
    isb_lib.core.things_main(ctx, db_url, verbosity, heart_rate)
    session = isb_lib.core.get_db_session(db_url)
    index = 0
    page_size = 10000
    max_index = 195000
    count = 0
    while index < max_index:
        iterator = session.execute(
            select(
                igsn_lib.models.thing.Thing._id,
                igsn_lib.models.thing.Thing.id,
            ).where(igsn_lib.models.thing.Thing.authority_id == "SMITHSONIAN")
            .slice(index, index + page_size)
        )
        for row in iterator:
            dict = row._asdict()
            _id = dict["_id"]
            id = dict["id"]
            stripped_id = _fixed_smithsonian_id(id)
            if stripped_id is not None:
                count += 1
                session.execute(
                    update(igsn_lib.models.thing.Thing)
                    .where(igsn_lib.models.thing.Thing._id == _id)
                    .values(id=stripped_id)
                )
            else:
                print("updated is None, skipping")
        session.commit()
        index += page_size
        logging.info(f"going to next page, index is {index}")
    print(f"num records is {count}")


"""
Updates existing Smithsonian records in a Things db to have their id column stripped of the n2t prefix.
"""
if __name__ == "__main__":
    main()
