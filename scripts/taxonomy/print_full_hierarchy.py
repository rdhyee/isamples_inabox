import click
import click_config_file
from isamples_metadata import SESARTransformer

import isb_lib
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO
from create_label_hierarchy import getFullLabel, getHierarchyMapping
from scripts.taxonomy.classification import get_classification_result, parse_SESAR_thing

@click.command()
@click.option(
    "-d",
    "--db_url",
    default=None,
    help="Postgres database URL",
)
@click.option("-s", "--solr_url", default=None, help="Solr index URL")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1,
    help="Maximum records to load, -1 for all",
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
# You can specify the filename by doing --config <file> as the cmdline option
@click_config_file.configuration_option()
@click.pass_context
def main(ctx, db_url: str, solr_url: str, max_records: int, verbosity: str):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity)
    db_session = SQLModelDAO(db_url).get_session()
    thing_iterator = ThingRecordIterator(
        db_session,
        authority_id="SESAR",
        page_size=max_records
    )
    context_mapping = getHierarchyMapping("context")
    material_mapping = getHierarchyMapping("material")
    specimen_mapping = getHierarchyMapping("specimen")

    idx = 0

    for thing in thing_iterator.yieldRecordsByPage():
        print(f"thing is {thing.id}")
        transformed = SESARTransformer.SESARTransformer(
            thing.resolved_content
        ).transform()

        context_label = transformed['hasContextCategory']
        material_label = transformed['hasMaterialCategory']
        specimen_label = transformed['hasSpecimenCategory']

        full_context = [getFullLabel(context_label, context_mapping)]
        full_material = [getFullLabel(material_label, material_mapping)]
        full_specimen = [getFullLabel(specimen_label, specimen_mapping)]

        print(f"context: {full_context} material: {full_material} "
              f"specimen: {full_specimen}")

        # conduct classification using model
        text, gold_material, gold_sample = parse_SESAR_thing(
            thing.resolved_content
        )

        if gold_material!= None:
            print(text, gold_material, gold_sample)
            print(get_classification_result(text))
            print("------------")

            idx+=1
            if idx==1000:
                break


if __name__ == "__main__":
    main()
