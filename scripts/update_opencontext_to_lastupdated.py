import datetime
import logging
import typing
import click
import click_config_file
import isb_lib.core
from sqlalchemy import select
from sqlalchemy import update
import igsn_lib.models
import igsn_lib.models.thing


def _sesar_last_updated(dict: typing.Dict) -> typing.Optional[datetime.datetime]:
    description = dict.get("description")
    if description is not None:
        log = description.get("log")
        if log is not None:
            for record in log:
                if "lastUpdated" == record.get("type"):
                    return record["timestamp"]
    return None


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
@click_config_file.configuration_option(config_file_name="opencontext.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, verbosity, heart_rate)
    session = isb_lib.core.get_db_session(db_url)
    index = 1850000
    page_size = 10000
    max_index = 4700000
    count = 0
    while index < max_index:
        iterator = session.execute(
            select(
                igsn_lib.models.thing.Thing._id,
                igsn_lib.models.thing.Thing.resolved_content,
            ).where(igsn_lib.models.thing.Thing.authority_id == "SESAR")
            .slice(index, index + page_size)
        )
        for row in iterator:
            dict = row._asdict()
            id = dict["_id"]
            resolved_content = dict["resolved_content"]
            updated = _sesar_last_updated(resolved_content)
            if updated is not None:
                count += 1
                session.execute(
                    update(igsn_lib.models.thing.Thing)
                    .where(igsn_lib.models.thing.Thing._id == id)
                    .values(tcreated=updated)
                )
            else:
                print("updated is None, skipping")
        session.commit()
        index += page_size
        logging.info(f"going to next page, index is {index}")
    print(f"num records is {count}")


"""
Updates existing OpenContext records in a Things db to have their tcreated column based on the OpenContext "updated"
field in the JSON as opposed to the previous implementation, which used the OpenContext "published" field.
"""
if __name__ == "__main__":
    main()
