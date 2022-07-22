import click
import click_config_file
from isamples_metadata import SESARTransformer

import isb_lib
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO
from create_hierarchy_json import getFullLabel, getHierarchyMapping
from classification_helper import classify_by_machine, classify_by_rule
from SESARClassifierInput import SESARClassifierInput


def get_classification_result(description_map, text, collection, labelType):
    """Return the classification result"""
    # first pass : see if the record falls in the defined rules
    label = classify_by_rule(description_map, text, collection, labelType)
    if label:
        return (label, -1)  # set sentinel value as probability
    else:
        # second pass : pass the record to the model
        machine_prediction = classify_by_machine(text, collection, labelType)
        return machine_prediction  # (predicted label, probability)


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
    default=1000,
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

    for thing in thing_iterator.yieldRecordsByPage():
        # print(f"thing is {thing.id}")
        transformed = SESARTransformer.SESARTransformer(
            thing.resolved_content
        ).transform()

        # parse the thing to classifier input form
        parsed = SESARClassifierInput(thing.resolved_content).parse_thing()

        description_map = parsed.get_description_map()
        material_text = parsed.get_material_text()

        # get the material label prediction result of the record
        label, prob = get_classification_result(
            description_map, material_text, "SESAR", "material"
        )

        print(
            f"Predicted (probability, label) : {label}, {prob}"
        )

        # gold label of the record
        context_label = transformed['hasContextCategory']
        material_label = transformed['hasMaterialCategory']
        specimen_label = transformed['hasSpecimenCategory']

        full_context = [getFullLabel(context_label, context_mapping)]
        full_material = [getFullLabel(material_label, material_mapping)]
        full_specimen = [getFullLabel(specimen_label, specimen_mapping)]

        # print out the full hierarchy of the label
        print(f"context: {full_context} material: {full_material} "
              f"specimen: {full_specimen}")
        print("-----------------")
    db_session.close()


if __name__ == "__main__":
    main()
