import click
import click_config_file
from isamples_metadata import SESARTransformer, OpenContextTransformer

import isb_lib
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO
from scripts.taxonomy.create_hierarchy_json import getFullLabel, getHierarchyMapping
from isamples_metadata.taxonomy.SESARClassifierInput import SESARClassifierInput
from isamples_metadata.taxonomy.OpenContextClassifierInput import OpenContextClassifierInput
from isamples_metadata.taxonomy.metadata_models import MetadataModelLoader, SESARMaterialPredictor, OpenContextMaterialPredictor


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
    "-a",
    "--authority_id",
    default="SESAR",
    help="Collection type",
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click_config_file.configuration_option()
@click.pass_context
def main(
    ctx, db_url: str, solr_url: str, max_records: int,
    authority_id: str, verbosity: str
):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity)
    db_session = SQLModelDAO(db_url).get_session()
    thing_iterator = ThingRecordIterator(
        db_session,
        authority_id=authority_id,
        page_size=max_records
    )

    context_mapping = getHierarchyMapping("context")
    material_mapping = getHierarchyMapping("material")
    specimen_mapping = getHierarchyMapping("specimen")

    sesar_model = MetadataModelLoader.get_sesar_material_model()
    oc_material_model = MetadataModelLoader.get_oc_material_model()
    for thing in thing_iterator.yieldRecordsByPage():
        # print(f"thing is {thing.id}")
        igsn = thing.resolved_content["igsn"]
        if authority_id == "SESAR":
            transformed = SESARTransformer.SESARTransformer(
                thing.resolved_content
            ).transform()

            # parse the thing to classifier input form
            sesar_input = SESARClassifierInput(thing.resolved_content)
            sesar_input.parse_thing()

            material_text = sesar_input.get_material_text()
            print(igsn, material_text)
            # get the material label prediction result of the record
            # load the model predictor
            smp = SESARMaterialPredictor(sesar_model)
            results = smp.predict_material_type(
                thing.resolved_content
            )
            for result in results:
                print(
                    f"Predicted (probability, label) : {result.value}, {result.confidence}"
                )

            # gold label of the record
            context_label = transformed['hasContextCategory']
            material_label = transformed['hasMaterialCategory']
            specimen_label = transformed['hasSpecimenCategory']

            full_context = [getFullLabel(context_label, context_mapping)]
            full_material = [getFullLabel(material_label, material_mapping)]
            full_specimen = [getFullLabel(specimen_label, specimen_mapping)]

            # print out the full hierarchy of the label
            print(
                f"Gold label (hierarchical) :  "
                f"material: {full_material} / ",
                f"specimen: {full_specimen} / "
                f"context: {full_context} ",
            )

            print("-----------------")
        else:

            transformed = OpenContextTransformer.OpenContextTransformer(
                thing.resolved_content
            ).transform()

            # parse the thing to classifier input form
            oc_input = OpenContextClassifierInput(thing.resolved_content)
            oc_input.parse_thing()

            material_text = oc_input.get_material_text()
            # sample_text = oc_input.get_sample_text()

            # print out the classifier input form of this record
            print(material_text)

            # get the classification result
            ocm = OpenContextMaterialPredictor(oc_material_model)
            results = ocm.predict_material_type(
                thing.resolved_content
            )
            for result in results:
                print(
                    f"Predicted (probability, label) : {result.value}, {result.confidence}"
                )
    db_session.close()


if __name__ == "__main__":
    main()
