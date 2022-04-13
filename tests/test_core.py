import pytest
import isb_lib.core
import json
import requests

TEST_LIVE_SERVER = 0

iterator_testcases = [
    (1, 1),
    (55, 55),
    (999, 999),
    (1000, 1000),
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


def _try_to_add_solr_doc(core_doc_str):
    core_doc = json.loads(core_doc_str)
    solr_dict = isb_lib.core._coreRecordAsSolrDoc(core_doc)
    if TEST_LIVE_SERVER:
        isb_lib.core.solrAddRecords(requests.session(), [solr_dict], url="http://localhost:8983/api/collections/isb_core_records/")
    return solr_dict


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
    solr_dict = _try_to_add_solr_doc(core_doc_str)
    assert "producedBy_samplingSite_location_ll" in solr_dict


def test_coreRecordAsSolrDoc2():
    core_doc_str = """
{
  "$schema": "../../iSamplesSchemaBasic0.2.json",
  "@id": "metadata/28722/k2ks6xw6t",
  "curation": {
    "accessConstraints": "Not Provided",
    "curationLocation": "Not Provided",
    "description": "Not Provided",
    "label": "Not Provided",
    "responsibility": "Not Provided"
  },
  "description": "early bce/ce: False | late bce/ce: False | updated: 2021-01-27T03:57:23Z | Has type: color patches (military patches)",
  "hasContextCategory": [
    "Site of past human activities"
  ],
  "hasMaterialCategory": [
    "Anthropogenic material"
  ],
  "hasSpecimenCategory": [
    "Artifact"
  ],
  "informalClassification": [],
  "keywords": [
    "International Space Station",
    "Zvezda Service Module"
  ],
  "label": "Object 68",
  "producedBy": {
    "@id": "Not Provided",
    "description": "http://opencontext.org/projects/e682f907-6e4a-44cc-8a5f-3e2c73001673",
    "hasFeatureOfInterest": "Not Provided",
    "label": "Archaeology of the International Space Station",
    "responsibility": [
      "creator: Justin Walsh",
      "creator: Alice Gorman",
      "creator: Wendy Salmond"
    ],
    "resultTime": "2021-01-27T02:47:12Z",
    "samplingSite": {
      "location": {
        "latitude": false,
        "longitude": false
      },
      "placeName": [
        "International Space Station",
        "Zvezda Service Module"
      ]
    }
  },
  "relatedResource": [],
  "sampleidentifier": "ark:/28722/k2ks6xw6t"
}
    """
    solr_dict = _try_to_add_solr_doc(core_doc_str)
    assert "producedBy_samplingSite_location_latlon" not in solr_dict


def test_date_year_only():
    date_str = "1985"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.day == 1
    assert datetime.month == 1
    assert datetime.year == 1985


def test_date_year_month_day():
    date_str = "1947-08-06"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.day == 6
    assert datetime.month == 8
    assert datetime.year == 1947


def test_date_year_month():
    date_str = "2020-07"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.month == 7
    assert datetime.year == 2020
    # default to the first of the month since it wasn't in the original
    assert datetime.day == 1


def test_date_with_time():
    date_str = "2019-12-08 15:54:00"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.year == 2019
    assert datetime.month == 12
    assert datetime.day == 8
    assert datetime.hour == 15
    assert datetime.minute == 54
    assert datetime.second == 0
    assert datetime.tzinfo.zone == 'UTC'


def test_isamples_date():
    datetime = isb_lib.core.parsed_datetime_from_isamples_format("2020-07-16T11:25:16Z")
    assert datetime is not None
    assert datetime.year == 2020
    assert datetime.month == 7
    assert datetime.day == 16
    assert datetime.hour == 11
    assert datetime.minute == 25
    assert datetime.second == 16


def test_isamples_date_with_ms():
    datetime = isb_lib.core.parsed_datetime_from_isamples_format("2020-07-16T11:25:16.123Z")
    assert datetime is not None
    assert datetime.year == 2020
    assert datetime.month == 7
    assert datetime.day == 16
    assert datetime.hour == 11
    assert datetime.minute == 25
    assert datetime.second == 16
