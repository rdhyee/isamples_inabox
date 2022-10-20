import click
import click_config_file
from click import Context
import json

import isb_lib.core
from isb_web.sqlmodel_database import (
    dump_things_with_ids_to_file,
    SQLModelDAO,
    load_things_from_file,
)


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx: Context, db_url: str):
    isb_lib.core.things_main(ctx, db_url, None)


@main.command("dump")
@click.option(
    "--input_file",
    type=str,
    default=None,
    help="The file path to the list of IDs to dump.  This is a JSON file with key IDs and value expected predictions ",
)
@click.option(
    "--output_file",
    type=str,
    default=None,
    help="The file path to dump the output to.",
)
@click.pass_context
def dump(ctx: Context, input_file: str, output_file: str):
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    identifiers = []
    with open(input_file, "r") as id_file:
        test_model_values_dict = json.load(id_file)
        for key in test_model_values_dict.keys():
            identifiers.append(key)
    dump_things_with_ids_to_file(session, identifiers, output_file)


@main.command("load")
@click.option(
    "--input_file",
    type=str,
    default=None,
    help="The file path to the dumped file from the load command.",
)
@click.pass_context
def load(ctx: Context, input_file: str):
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    load_things_from_file(session, input_file)


if __name__ == "__main__":
    main()
