import click
import click_config_file
from isamples_metadata import SESARTransformer

import isb_lib
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO
from create_hierarchy_json import getFullLabel, getHierarchyMapping
from model import get_model


def parse_SESAR_thing(thing):
    """Return the concatenated_text of the object and its gold label
    """
    description_field = {
        "supplementMetadata": [
            "geologicalAge", "classificationComment",
            "purpose", "primaryLocationType", "geologicalUnit",
            "locality", "localityDescription", "fieldName",
            "launchId", "purpose", "originalArchive", "cruiseFieldPrgrm",
            "collector", "collectorDetail", "igsnPrefix"
        ],
        "collectionMethod": [],
        "material": [],
        "sampleType": [],
        "description": [],
        "collectionMethodDescr": [],
        "contributors": ["contributor"]
    }

    concatenated_text = ""
    sample_field, material_field = "sampleType", "material"  # gold label field

    gold_sample, gold_material = None, None  # gold label value

    # extract data from informative fields
    for key, value in thing["description"].items():
        if key in description_field:
            # key is a field we want to extract
            # we don't have to look at subfields
            if key == sample_field:
                gold_sample = value
            elif key == material_field:
                gold_material = value
            elif len(description_field[key]) == 0:
                concatenated_text += value + " , "
            # we have to extract from subfields
            else:
                # special case : contributors_contributor_name
                if key == "contributors" and len(value) > 0:
                    field = value[0][description_field[key][0]][0]["name"]
                    concatenated_text += field + " , "
                else:
                    for sub_key in value:
                        if sub_key in description_field[key]:
                            concatenated_text += value[sub_key] + " , "

    concatenated_text = concatenated_text[:-2]  # remove last comma

    return concatenated_text, gold_material, gold_sample


# output the classification result
def get_classification_result(input, modelType, labelType):
    # load the model
    if modelType == "SESAR" and labelType == "material":
        model = get_model("scripts/taxonomy/assets/SESAR_material_config.json")
    return model.predict(input)


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
        print(f"thing is {thing.id}")
        transformed = SESARTransformer.SESARTransformer(
            thing.resolved_content
        ).transform()

        # extract text and gold label from object
        text, gold_material, gold_sample = parse_SESAR_thing(
            thing.resolved_content
        )

        # conduct classification by using text as input to the model
        print(text, gold_material, gold_sample)
        print(get_classification_result(text, "SESAR", "material"))

        # print the gold label of the record
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
