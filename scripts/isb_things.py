import json
import logging
import sys

import click
import click_config_file
import os.path
from click import Context
from tabulate import tabulate

import isb_lib.core
from isb_lib.data_import import csv_import

from isb_lib.data_import.csv_import import things_from_isamples_package
from isb_lib.sitemaps import build_sitemap
from isb_lib.sitemaps.gh_pages_sitemap import GHPagesSitemapIndexIterator
from isb_web import config
from isb_web.sqlmodel_database import SQLModelDAO


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option("-s", "--solr_url", default=None, help="Solr index URL")
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
def main(ctx, db_url, solr_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, config.Settings().database_url, config.Settings().solr_url, verbosity, heart_rate)


@main.command("validate")
@click.option(
    "-f",
    "--file",
    type=str,
    help="Path to the CSV file containing the samples to load",
    required=True
)
def validate_isamples_package(file: str):
    package = csv_import.create_isamples_package(file)
    report = package.validate()
    if report.valid:
        print("Validation successful.")
    else:
        print_report_errors(report)
        sys.exit(-1)


def print_report_errors(report):
    errors = report.flatten(['code', 'message'])
    print(tabulate(errors, headers=['code', 'message']))


@main.command("load")
@click.option(
    "-f",
    "--file",
    type=str,
    help="Path to the CSV file containing the samples to load",
    required=True
)
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=-1,
    help="Maximum records to load, -1 for all",
)
@click.pass_context
def load_records(ctx: Context, file: str, max_records: int):
    package = csv_import.create_isamples_package(file)
    report = package.validate()
    if not report.valid:
        print_report_errors(report)
        return
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    things = things_from_isamples_package(session, package, max_records)
    logging.info(f"Successfully imported {len(things)} things.")


@main.command("sitemap")
@click.option(
    "-f",
    "--file",
    type=str,
    help="Path to the CSV file containing the samples to load",
    required=True
)
@click.option(
    "-d",
    "--directory",
    type=str,
    help="Path to the JSON sitemap directory",
    required=True
)
@click.option(
    "-u",
    "--url",
    type=str,
    help="URL prefix to use when writing out the sitemap files",
    required=True
)
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=-1,
    help="Maximum records to load, -1 for all",
)
def write_json_sitemap(file: str, directory: str, url: str, max_records: int):
    package = csv_import.create_isamples_package(file)
    isb_core_dicts = csv_import.isb_core_dicts_from_isamples_package(package)
    for core_dict in isb_core_dicts:
        dict_id = core_dict["@id"]
        target_path = os.path.join(directory, f"{dict_id}.json")
        with open(target_path, "w", newline="") as target_file:
            target_file.write(json.dumps(core_dict, indent=2, default=str))
    build_sitemap(directory, url, GHPagesSitemapIndexIterator(directory))


def _validate_resolved_content(thing: isb_lib.models.thing.Thing) -> dict:
    return isb_lib.core.validate_resolved_content(config.Settings().authority_id, thing)


def reparse_as_core_record(thing: isb_lib.models.thing.Thing) -> list[dict]:
    # No transformation necessary since we already import the data in our solr format
    resolved_content = _validate_resolved_content(thing)
    return [resolved_content]


@main.command("populate_isb_core_solr")
@click.pass_context
def populate_isb_core_solr(ctx):
    db_url = ctx.obj["db_url"]
    solr_url = ctx.obj["solr_url"]
    solr_importer = isb_lib.core.CoreSolrImporter(
        db_url=db_url,
        authority_id=config.Settings().authority_id,
        db_batch_size=1000,
        solr_batch_size=1000,
        solr_url=solr_url
    )
    allkeys = solr_importer.run_solr_import(
        reparse_as_core_record
    )
    logging.info(f"Total keys= {len(allkeys)}")


if __name__ == "__main__":
    main()
