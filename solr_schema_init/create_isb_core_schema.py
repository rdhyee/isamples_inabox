import json
import typing

import requests

SOLR_API = "http://localhost:8983/api/collections/isb_core_records/"
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
    fname, ftype="string", stored=True, indexed=True, default=None, multivalued=False
):
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
    if not default is None:
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


def createCopyField(source, dest):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"add-copy-field": {"source": source, "dest": [dest]}}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())

def replaceFieldType(field_type_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"replace-field-type": field_type_dict}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())

def addFieldType(field_type_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"add-field-type": field_type_dict}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())

def addDynamicField(dynamic_field_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"add-dynamic-field": dynamic_field_dict}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())

#############
# Internal iSamples bookkeeping columns
# createField("isb_core_id", "string", True, True, None)
# createField("source", "string", True, True, None)
# The time the record was last updated in the source db
createField("sourceUpdatedTime", "pdate", True, True, None)
# The time the record was last updated in the iSamples index
createField("indexUpdatedTime", "pdate", True, True, None)
#############


# createField("label", "string", True, True, None)
# createField("description", "string", True, True, None)
# createField("description_text", "text_general", True, True, None)
# createCopyField("description", "description_text")
# createField("hasContextCategory", "string", True, True, None, True)
# createField("hasMaterialCategory", "string", True, True, None, True)
# createField("hasSpecimenCategory", "string", True, True, None, True)
# createField("keywords", "string", True, True, None, True)
# createField("informalClassification", "string", True, True, None, True)
# createField("producedBy_isb_core_id", "string", True, True, None)
# createField("producedBy_label", "string", True, True, None)
# createField("producedBy_description", "string", True, True, None)
# createField("producedBy_description_text", "text_general", True, True, None)
# createCopyField("producedBy_description", "producedBy_description_text")
# createField("producedBy_hasFeatureOfInterest", "string", True, True, None)
# createField("producedBy_responsibility", "string", True, True, None, True)
# createField("producedBy_resultTime", "pdate", True, True, None)
# createField("producedBy_samplingSite_description", "string", True, True, None)
# createField(
#     "producedBy_samplingSite_description_text", "text_general", True, True, None
# )
# createCopyField("producedBy_samplingSite_description", "producedBy_samplingSite_description_text")
# createField("producedBy_samplingSite_label", "string", True, True, None)
# createField("producedBy_samplingSite_location_elevationInMeters", "pfloat", True, True, None)
# createField("producedBy_samplingSite_location_latlon", "location", True, True, None)
# createField("producedBy_samplingSite_placeName", "string", True, True, None, True)
# createField("registrant", "string", True, True, None, True)
# createField("samplingPurpose", "string", True, True, None, True)
# createField("curation_label", "string", True, True, None)
# createField("curation_description", "string", True, True, None)
# createField("curation_description_text", "text_general", True, True, None)
# createCopyField("curation_description", "curation_description_text")
# createField("curation_accessContraints", "string", True, True, None)
# createField("curation_location", "string", True, True, None)
# createField("curation_responsibility", "string", True, True, None)
# createField("relatedResource_isb_core_id", "string", True, True, None, True)
#
# pj(listFields())

# replaceFieldType({
#     "name": "location_rpt",
#     "class": "solr.SpatialRecursivePrefixTreeFieldType",
#     "geo": True,
#     "omitNorms": True,
#     "omitTermFreqAndPositions": True,
#     "spatialContextFactory": "JTS",
#     "termOffsets": False,
#     "termPositions": False,
#     "omitPositions": True,
#     "autoIndex": True
# })
# addFieldType({
#     "name": "bbox",
#     "class": "solr.BBoxField",
#     "geo": True,
#     "numberType": "pdouble",
#     "distanceUnits": "kilometers"
# })
# addDynamicField({
#     "name": "*_ll",
#     "type": "location",
#     "indexed": True,
#     "stored": True
# })
# addDynamicField({
#     "name":"*_bb",
#     "type":"bbox"
# })
# addDynamicField({
#     "name":"*_rpt",
#     "type":"location_rpt",
#     "multiValued":True,
#     "indexed":True,
#     "stored":True
# })

#deleteField("producedBy_samplingSite_location_latlon")
#createField("producedBy_samplingSite_location_ll", "location", True, True, None)
#createField("producedBy_samplingSite_location_bb", "bbox", True, True, None)
#createField("producedBy_samplingSite_location_rpt", "location_rpt", True, True, None)