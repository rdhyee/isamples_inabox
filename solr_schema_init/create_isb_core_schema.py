import json
import typing
import requests
import time

# Sleep for 10 seconds to allow for the Solr Docker container to start up.
time.sleep(10)

COLLECTION_NAME = "isb_core_records"

CREATE_COLLECTION_API = f"http://solr:8983/solr/admin/collections?action=CREATE&name={COLLECTION_NAME}&numShards=1&replicationFactor=1"
CONFIG_API = f"http://solr:8983/solr/{COLLECTION_NAME}/config"
SOLR_API = f"http://solr:8983/api/collections/{COLLECTION_NAME}/"
MEDIA_JSON = "application/json"


def pj(o):
    print(json.dumps(o, indent=2))


def listFields():
    headers = {"Accept": MEDIA_JSON}
    _schema = requests.get(f"{SOLR_API}schema", headers=headers).json()
    return _schema.get("schema", {}).get("fields")


def listFieldTypes():
    headers = {"Accept": MEDIA_JSON}
    _schema = requests.get(f"{SOLR_API}schema", headers=headers).json()
    return _schema.get("schema", {}).get("fieldTypes")


def createField(
    fname, ftype="string", stored=True, indexed=True, default=None, multivalued=False, docValues=False
):
    print(f"going to create field {fname}")
    headers = {"Content-Type": MEDIA_JSON}
    data = {
        "add-field": {
            "name": fname,
            "type": ftype,
            "stored": stored,
            "indexed": indexed,
        }
    }
    if multivalued:
        data["add-field"]["multiValued"] = multivalued
    if docValues:
        data["add-field"]["docValues"] = True
    if default is not None:
        data["add-field"]["default"] = default
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def deleteField(fname):
    headers = {"Content-Type": MEDIA_JSON}
    data = {
        "delete-field": {
            "name": fname,
        }
    }
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def createCopyField(source, dest, maxChars=None):
    headers = {"Content-Type": MEDIA_JSON}
    copyFieldData = {"source": source, "dest": [dest]}
    if maxChars is not None:
        copyFieldData["maxChars"] = maxChars
    data = {"add-copy-field": copyFieldData}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def deleteCopyField(source, dest):
    headers = {"Content-Type": MEDIA_JSON}
    copyFieldData = {"source": source, "dest": [dest]}
    data = {"delete-copy-field": copyFieldData}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def replaceFieldType(field_type_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"replace-field-type": field_type_dict}
    encoded_data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=encoded_data)
    pj(res.json())


def addFieldType(field_type_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"add-field-type": field_type_dict}
    encoded_data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=encoded_data)
    pj(res.json())


def addDynamicField(dynamic_field_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"add-dynamic-field": dynamic_field_dict}
    encoded_data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=encoded_data)
    pj(res.json())


def createCollection():
    print(f"Going to attempt to create collection {COLLECTION_NAME}")
    headers = {"Content-Type": MEDIA_JSON}
    res = requests.get(f"{CREATE_COLLECTION_API}", headers=headers)
    print("Response is: " + str(res))
    if res.status_code == 400:
        print("Collection already exists.  Exiting.")
        exit(0)
    pj(res.json())
    # Make sure to disable auto field creation, as this is known harmful behavior!
    data = {
        "set-user-property": {
            "update.autoCreateFields": "false"
        }
    }
    encoded_data = json.dumps(data).encode("utf-8")
    config_res = requests.post(CONFIG_API, headers=headers, data=encoded_data)
    pj(config_res.json())


print("Going to create collection in create_isb_core_schema")
#############
createCollection()
print("Going to create fields in create_isb_core_schema")
# Internal iSamples bookkeeping columns
createField("isb_core_id", "string", True, True, None)
# Catch-all search field that all text should copy to
createField("searchText", "text_en", True, True, None, True)
createField("source", "string", True, True, None)
createCopyField("source", "searchText")
# The time the record was last updated in the source db
createField("sourceUpdatedTime", "pdate", True, True, None)
# The time the record was last updated in the iSamples index
createField("indexUpdatedTime", "pdate", True, True, None)
#############


createField("label", "string", True, True, None)
createCopyField("label", "searchText")
createField("description", "string", True, True, None)
createField("description_text", "text_en", True, True, None)
createCopyField("description", "description_text")
createCopyField("description", "searchText")
createField("hasContextCategory", "string", True, True, None, True)
createField("hasContextCategoryConfidence", "pfloat", True, True, None, True)
createField("hasMaterialCategory", "string", True, True, None, True)
createField("hasMaterialCategoryConfidence", "pfloat", True, True, None, True)
createField("hasSpecimenCategory", "string", True, True, None, True)
createField("hasSpecimenCategoryConfidence", "pfloat", True, True, None, True)
createField("keywords", "string", True, True, None, True)
createCopyField("keywords", "searchText")
createField("informalClassification", "string", True, True, None, True)
createCopyField("informalClassification", "searchText")
createField("producedBy_isb_core_id", "string", True, True, None)
createField("producedBy_label", "string", True, True, None)
createCopyField("producedBy_label", "searchText")
createField("producedBy_description", "string", True, True, None)
createField("producedBy_description_text", "text_en", True, True, None)
createCopyField("producedBy_description", "producedBy_description_text")
createCopyField("producedBy_description", "searchText")
createField("producedBy_hasFeatureOfInterest", "string", True, True, None)
createCopyField("producedBy_hasFeatureOfInterest", "searchText")
createField("producedBy_responsibility", "string", True, True, None, True)
createCopyField("producedBy_responsibility", "searchText")
createField("producedBy_resultTime", "pdate", True, True, None)
createField("producedBy_samplingSite_description", "string", True, True, None)
createCopyField("producedBy_samplingSite_description", "searchText")
createField(
    "producedBy_samplingSite_description_text", "text_en", True, True, None
)
createCopyField("producedBy_samplingSite_description", "producedBy_samplingSite_description_text")
createField("producedBy_samplingSite_label", "string", True, True, None)
createCopyField("producedBy_samplingSite_label", "searchText")
createField("producedBy_samplingSite_location_elevationInMeters", "pfloat", True, True, None)
createField("producedBy_samplingSite_placeName", "string", True, True, None, True)
createCopyField("producedBy_samplingSite_placeName", "searchText")
createField("registrant", "string", True, True, None, True)
createCopyField("registrant", "searchText")
createField("samplingPurpose", "string", True, True, None, True)
createCopyField("samplingPurpose", "searchText")
createField("curation_label", "string", True, True, None)
createCopyField("curation_label", "searchText")
createField("curation_description", "string", True, True, None)
createField("curation_description_text", "text_en", True, True, None)
createCopyField("curation_description", "curation_description_text")
createCopyField("curation_description", "searchText")
createField("curation_accessContraints", "string", True, True, None)
createField("curation_location", "string", True, True, None)
createCopyField("curation_location", "searchText")
createField("curation_responsibility", "string", True, True, None)
createField("relatedResource_isb_core_id", "string", True, True, None, True)

replaceFieldType({
    "name": "location_rpt",
    "class": "solr.SpatialRecursivePrefixTreeFieldType",
    "geo": True,
    "omitNorms": True,
    "omitTermFreqAndPositions": True,
    "spatialContextFactory": "JTS",
    "termOffsets": False,
    "termPositions": False,
    "omitPositions": True,
    "autoIndex": True
})
addFieldType({
    "name": "bbox",
    "class": "solr.BBoxField",
    "geo": True,
    "numberType": "pdouble",
    "distanceUnits": "kilometers"
})
addDynamicField({
    "name": "*_ll",
    "type": "location",
    "indexed": True,
    "stored": True
})
addDynamicField({
    "name": "*_bb",
    "type": "bbox"
})
addDynamicField({
    "name": "*_rpt",
    "type": "location_rpt",
    "multiValued": True,
    "indexed": True,
    "stored": True
})

createField("producedBy_samplingSite_location_ll", "location", True, True, None)
createField("producedBy_samplingSite_location_bb", "bbox", True, True, None)
createField("producedBy_samplingSite_location_rpt", "location_rpt", True, True, None)
createField("producedBy_samplingSite_location_latitude", "pfloat", True, True, None)
createField("producedBy_samplingSite_location_longitude", "pfloat", True, True, None)

addFieldType({
    "name": "date_range",
    "class": "solr.DateRangeField",
    "omitNorms": True,
})
createField("producedBy_resultTimeRange", "date_range", True, True, None)

createField("producedBy_samplingSite_location_h3_0", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_1", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_2", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_3", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_4", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_5", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_6", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_7", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_8", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_9", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_10", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_11", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_12", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_13", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_14", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_15", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_cesium_height", "pfloat", True, True, None)
# Nested document support
# Note that the solr docs indicate we need these fields, but they already existed in our schema, keeping here for
# reference purposes
# createField("_root_", "string", False, True)
# addFieldType({
#     "name": "_nest_path_",
#     "class": "solr.NestPathField",
# })
# createField("_nest_path_", "nest_path", True, True)
createField("_nest_parent_", "string", True, True)
createField("relation_target", "string", True, True)
createField("relation_type", "string", True, True)
# permit information fields
createField("compliesWith", "string", True, True, None, True)
createField("authorizedBy", "string", True, True, None, True)

pj(listFields())
