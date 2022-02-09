import typing
import click
import click_config_file
import isb_lib.core
from isb_web.sqlmodel_database import SQLModelDAO

excluded_keys = ["id", "bcid", "uri", "@id"]
SEPARATOR = "###"


def process_dictionary(dictionary: typing.Dict) -> str:
    """Process all the keys in a dictionary and return all the values in a string, recursing through embedded dicts"""
    line = ""
    for key in dictionary:
        if key not in excluded_keys:
            value = dictionary[key]
            if type(value) is dict:
                line = line + process_dictionary(value)
            elif type(value) is str:
                line = line + dictionary[key] + SEPARATOR
            elif type(value) is list:
                for subvalue in value:
                    if type(subvalue) is dict:
                        line = line + process_dictionary(subvalue)
                    else:
                        line = line + str(subvalue) + SEPARATOR
            else:
                line = line + str(dictionary[key]) + SEPARATOR
    return line


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
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnostics on 9999"
)
@click.option(
    "-a",
    "--authority",
    default="SMITHSONIAN",
    help="Which authority to use when selecting and dumping the resolved_content",
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate, authority):
    isb_lib.core.things_main(ctx, db_url, None, verbosity, heart_rate)
    session = SQLModelDAO((ctx.obj["db_url"])).get_session()
    sql = f"""select resolved_content from thing tablesample system(1) where
    resolved_status=200 and authority_id='{authority}'"""
    rs = session.execute(sql)
    filename = f"{authority}.txt"
    lines = []
    for row in rs:
        resolved_content = row._asdict()["resolved_content"]
        row_string = process_dictionary(resolved_content)
        row_string = row_string + "\n"
        print("row_string is " + row_string)
        lines.append(row_string)
    with open(filename, "w") as file:
        file.writelines(lines)


"""
Creates a text dump of the source collection file, one line of text per record
"""
if __name__ == "__main__":
    main()
