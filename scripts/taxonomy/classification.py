import click
import click_config_file
from isamples_metadata import SESARTransformer

import isb_lib
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO
from create_hierarchy_json import getFullLabel, getHierarchyMapping
from classification_helper import classify_by_machine, classify_by_rule

# gold label field of SESAR
SESAR_sample_field, SESAR_material_field = "sampleType", "material"


def build_concatenated_text(description_map, labelType):
    """Return the concatenated text of informative fields in the
    description_map"""
    concatenated_text = ""
    for key, value in description_map.items():
        if key == "igsnPrefix":
            continue
        elif labelType == "material" and key != SESAR_material_field:
            concatenated_text += value + " , "
        elif labelType == "sample" and key != SESAR_sample_field:
            concatenated_text += value + " , "

    return concatenated_text[:-2]  # remove the last comma


def parse_SESAR_thing(thing):
    """Return a map that stores the informative fields,
    the concatenated text versions for the material label classifier
    and the sample label classifier, and the gold labels
    """
    # define the informative fields to extract
    description_field = {
        "supplementMetadata": [
            "geologicalAge", "classificationComment",
            "purpose", "primaryLocationType", "geologicalUnit",
            "locality", "localityDescription", "fieldName",
            "purpose", "cruiseFieldPrgrm",
        ],
        "igsnPrefix": [],
        "collectionMethod": [],
        "material": [],
        "sampleType": [],
        "description": [],
        "collectionMethodDescr": [],
    }

    description_map = {}  # saves value of description_field

    gold_sample, gold_material = None, None  # gold label default value

    # parse the thing and extract data from informative fields
    for key, value in thing["description"].items():
        if key in description_field:
            # gold label fields
            if key == SESAR_sample_field:
                gold_sample = value
            elif key == SESAR_material_field:
                gold_material = value

            # fields that do not have subfields
            if len(description_field[key]) == 0:
                description_map[key] = value

            # fields that have subfields
            else:
                for sub_key in value:
                    if sub_key in description_field[key]:
                        description_map[key + "_" + sub_key] = value[sub_key]

    # build the concatenated text from the description_map
    material_text = build_concatenated_text(description_map, "material")
    sample_text = build_concatenated_text(description_map, "sample")

    return (
        description_map,
        material_text, sample_text,
        gold_material, gold_sample
    )


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

        # extract text and gold label from object
        (
            description_map,
            material_text, sample_text,
            gold_material, gold_sample
        ) = parse_SESAR_thing(thing.resolved_content)
        print(material_text)

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

    db_session.close()


if __name__ == "__main__":
    main()
