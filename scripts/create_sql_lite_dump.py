import json

import click
import click_config_file
import isb_lib.core
from isamples_metadata import Transformer
from isb_lib.models.isb_core_record import ISBCoreRecord
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
import requests
from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import update
import logging

# Use this to map between the solr column names and the sqlite column names and control what is
# written to the dump.
SOLR_TO_SQLITE_FIELD_MAPPINGS = {
    "id": "id",
    "isb_core_id": "isb_core_id",
    "indexUpdatedTime": "isb_updated_time",
    "sourceUpdatedTime": "source_updated_time",
    "label": "label",
    "searchText": "search_text",
    "description": "sample_description",
    "hasContextCategory": "context_categories",
    "hasMaterialCategory": "material_categories",
    "hasSpecimenCategory": "specimen_categories",
    "keywords": "keywords",
    "informalClassification": "informal_classifications",
    "producedBy_isb_core_id": "produced_by_isb_core_id",
    "producedBy_label": "produced_by_label",
    "producedBy_description": "produced_by_description",
    "producedBy_hasFeatureOfInterest": "produced_by_feature_of_interest",
    "producedBy_responsibility": "produced_by_responsibilities",
    "producedBy_resultTime": "produced_by_result_time",
    "producedBy_samplingSite_description": "produced_by_sampling_site_description",
    "producedBy_samplingSite_label": "produced_by_sampling_site_label",
    "producedBy_samplingSite_location_elevationInMeters": "produced_by_sampling_site_elevation_in_meters",
    "producedBy_samplingSite_location_latitude": "produced_by_sampling_site_location_latitude",
    "producedBy_samplingSite_location_longitude": "produced_by_sampling_site_location_longitude",
    "producedBy_samplingSite_placeName": "produced_by_sampling_site_place_names",
    "registrant": "registrants",
    "samplingPurpose": "sampling_purposes",
    "curation_label": "curation_label",
    "curation_description": "curation_description",
    "curation_accessConstraints": "curation_access_constraints",
    "curation_location": "curation_location",
    "curation_responsibility": "curation_responsibility",
    "relatedResource_isb_core_id": "related_resources_isb_core_id",
    "source": "source",
}

NUMERIC_COLUMNS = [
    "produced_by_sampling_site_elevation_in_meters",
    "produced_by_sampling_site_location_latitude",
    "produced_by_sampling_site_location_longitude",
]


# Filter out any placeholder values that shouldn't appear in the dump
def _filtered_value(value):
    if type(value) is str and value == Transformer.NOT_PROVIDED:
        return ""
    return value


@click.command()
@click.option(
    "-d",
    "--db_url",
    default=None,
    help="The SQLite database file URL for storage.  Doesn't need to exist beforehand.",
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
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnostics on 9999"
)
@click.option(
    "-q",
    "--query",
    default=None,
    help="The solr query to use when fetching the records for the dump",
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, solr_url, verbosity, heart_rate, query):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity, heart_rate)
    engine = create_engine(ctx.obj["db_url"], echo=False)
    SQLModel.metadata.drop_all(engine, tables=[ISBCoreRecord.__table__])
    SQLModel.metadata.create_all(engine, tables=[ISBCoreRecord.__table__])
    session = Session(engine)
    batch_size = 50000
    session_commit_frequency = 50000
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, query, batch_size, 0, "id asc")
    num_records = 0
    current_batch = []
    for solr_record in iterator:
        # We ended up using the core API as creating new records was too slow…
        # https://docs.sqlalchemy.org/en/14/core/tutorial.html
        # new_record = ISBCoreRecord()
        new_record = {}
        for key, value in SOLR_TO_SQLITE_FIELD_MAPPINGS.items():
            solr_value = solr_record.get(key)
            if type(solr_value) is list:
                filtered_solr_values = [_filtered_value(value) for value in solr_value]
                solr_value = json.dumps(filtered_solr_values)
            elif type(solr_value) is str:
                # Don't include placeholder empty values in the dump
                solr_value = _filtered_value(solr_value)
            # Key is source column solr name, value is dest column sqlite name in sqlite
            if value in NUMERIC_COLUMNS:
                if type(solr_value) is float:
                    new_record[value] = solr_value
                else:
                    # There was weirdness here where if we set None, the insert statement broke.  A hack!
                    new_record[value] = 0.0
            else:
                if solr_value is not None:
                    new_record[value] = solr_value
                else:
                    # There was weirdness here where if we set None, the insert statement broke.  A hack!
                    new_record[value] = ""
        current_batch.append(new_record)
        num_records += 1
        if num_records % session_commit_frequency == 0:
            logging.info(f"Committing records, have processed {num_records}")
            session.bulk_insert_mappings(
                mapper=ISBCoreRecord, mappings=current_batch, return_defaults=False
            )
            session.commit()
            current_batch = []
    # Commit any remainder
    session.bulk_insert_mappings(
        mapper=ISBCoreRecord, mappings=current_batch, return_defaults=False
    )
    cleanup_inserted_zeros(session)


def cleanup_inserted_zeros(session):
    # Manually update numeric 0s to None since we can't insert them that way.  Don't want to have lat/lon set to 0.0,0.0
    statement = (
        update(ISBCoreRecord)
        .where(ISBCoreRecord.produced_by_sampling_site_location_latitude == 0.0)
        .values(produced_by_sampling_site_location_latitude=None)
    )
    session.execute(statement)
    statement = (
        update(ISBCoreRecord)
        .where(ISBCoreRecord.produced_by_sampling_site_location_longitude == 0.0)
        .values(produced_by_sampling_site_location_longitude=None)
    )
    session.execute(statement)
    statement = (
        update(ISBCoreRecord)
        .where(ISBCoreRecord.produced_by_sampling_site_elevation_in_meters == 0.0)
        .values(produced_by_sampling_site_elevation_in_meters=None)
    )
    session.execute(statement)
    session.commit()


"""
Dumps the iSamples in a Box Solr Core records to a SQLite file
"""
if __name__ == "__main__":
    main()
