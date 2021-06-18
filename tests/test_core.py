import pytest
import isb_lib.core
import json
import requests

iterator_testcases = [
    (1, 1),
    (55, 55),
    (999, 999),
    (1000,1000),
    (1001, 1000),
    (2020, 1000),
]

@pytest.mark.parametrize("max_entries,expected_outcome", iterator_testcases)
def test_IdentifierIterator(max_entries, expected_outcome):
    itr = isb_lib.core.IdentifierIterator(max_entries=max_entries)
    cnt = 0
    for e in itr:
        cnt += 1
    assert cnt == expected_outcome

def test_coreRecordAsSolrDoc():
    core_doc_str = """
{
    "$schema": "../iSamplesSchemaBasicSMR.json",
    "@id": "https://data.isamples.org/digitalsample/igsn/EOI00002H",
    "label": "J730-GTHFS-16",
    "sampleidentifier": "igsn:EOI00002H",
    "description": "Not Provided",
    "hasContextCategory": ["Subsurface fluid reservoir"],
    "hasMaterialCategory": ["Gaseous material"],
    "hasSpecimenCategory": ["Container with fluid"],
    "keywords": ["Individual Sample"],
    "producedBy": {
        "label": "Sampler:Fluid:GTHFS",
        "description": "cruiseFieldPrgrm:TN300. launchPlatformName:Jason II. Sampler:Fluid:GTHFS. HFS gastight. Red-center-9. T=250C. launch type:ROV, navigation type:USBL",
        "hasFeatureOfInterest": "volcano",
        "responsibility": ["Evans_Leigh,,Collector","Andra Bobbitt,,Sample Owner"],
        "resultTime": "2013-09-14 01:30:00",
        "samplingSite": {
            "description": "Trevi:Jason Tmax=257.9 C. In the direct flow at this small anhydrite mound (anhydrite knocked over).",
            "label": "Not Provided",
            "location": {
                "elevation": "-1520.0 m",
                "latitude": 45.9463,
                "longitude": -129.9837
            },
            "placeName": ["Axial Seamount"]
        }
    },
    "registrant": "Andra Bobbitt",
    "samplingPurpose": ""
}    
    """
    core_doc = json.loads(core_doc_str)
    solr_dict = isb_lib.core.coreRecordAsSolrDoc(core_doc)
    assert "producedBy_samplingSite_location_latlon" in solr_dict
    isb_lib.core.solrAddRecords(requests.session(), [solr_dict], url="http://localhost:8983/api/collections/isb_core_records/")