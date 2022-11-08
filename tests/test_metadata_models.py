from isamples_metadata.taxonomy.metadata_models import (
    MetadataModelLoader,
    SESARMaterialPredictor,
    OpenContextMaterialPredictor,
    OpenContextSamplePredictor
)
import pytest
import json

SESAR_test_values = [
    "./test_data/SESAR/raw/EOI00002Hjson-ld.json",
    "./test_data/SESAR/raw/IEEJR000Mjson-ld.json",
    "./test_data/SESAR/raw/IE22301MWjson-ld.json",
]


@pytest.mark.parametrize("sesar_source_path", SESAR_test_values)
def test_sesar_prediction_equal(sesar_source_path):
    _test_sesar_material_model(
        sesar_source_path
    )


def _test_sesar_material_model(sesar_source_path):
    with open(sesar_source_path) as source_file:
        sesar_source_record = json.load(source_file)
    # set dummy model config
    sesar_config = {
        "BERT_MODEL": "distilbert-base-uncased",
        "FINE_TUNED_MODEL": "distilbert-base-uncased",
        "CLASS_NAMES": [
            "Biology",
            "EarthMaterial",
            "Gas",
            "Ice",
            "Liquid",
            "Material",
            "Mineral",
            "NotApplicable",
            "Organic Material",
            "Other",
            "Particulate",
            "Rock",
            "Sediment",
            "Soil",
            "experimentalMaterial"
        ],
        "MAX_SEQUENCE_LEN": 256
    }
    sesar_model = MetadataModelLoader.get_sesar_material_model(sesar_config)
    # load the model predictor
    smp = SESARMaterialPredictor(sesar_model)
    assert smp is not None
    pred_results = smp.predict_material_type(sesar_source_record)
    # extract the highest confidence prediction
    result = pred_results[0]
    label, prob = result.value, result.confidence
    assert type(label) == str and type(prob) == int or type(prob) == float


OpenContext_test_values = [
    "./test_data/OpenContext/raw/ark-28722-k2b85cg1p.json",
    "./test_data/OpenContext/raw/ark-28722-k26d5xr5z.json",
    "./test_data/OpenContext/raw/ark-28722-k2vq31x46.json",
]


@pytest.mark.parametrize("opencontext_source_path", OpenContext_test_values)
def test_opencontext_prediction_equal(opencontext_source_path):
    _test_opencontext_material_model(
        opencontext_source_path
    )
    _test_opencontext_sample_model(
        opencontext_source_path
    )


def _test_opencontext_material_model(opencontext_source_path):
    with open(opencontext_source_path) as source_file:
        oc_source_record = json.load(source_file)
    # set dummy model config
    oc_material_config = {
        "BERT_MODEL": "distilbert-base-uncased",
        "FINE_TUNED_MODEL": "distilbert-base-uncased",
        "CLASS_NAMES": [
            "mat:anthropogenicmetal",
            "mat:anyanthropogenicmeterial",
            "mat:biogenicnonorganicmaterial",
            "mat:mineral",
            "mat:organicmaterial",
            "mat:otheranthropogenicmaterial",
            "mat:rock"
        ],
        "MAX_SEQUENCE_LEN": 256
    }
    oc_model = MetadataModelLoader.get_oc_material_model(oc_material_config)
    # load the model predictor
    ocmp = OpenContextMaterialPredictor(oc_model)
    assert ocmp is not None
    pred_results = ocmp.predict_material_type(oc_source_record)
    # extract the highest confidence prediction
    result = pred_results[0]
    label, prob = result.value, result.confidence
    assert type(label) == str and type(prob) == int or type(prob) == float


def _test_opencontext_sample_model(opencontext_source_path):
    with open(opencontext_source_path) as source_file:
        oc_source_record = json.load(source_file)
    # set dummy model config
    oc_sample_config = {
        "BERT_MODEL": "distilbert-base-uncased",
        "FINE_TUNED_MODEL": "distilbert-base-uncased",
        "CLASS_NAMES": [
            "architectural element",
            "clothing",
            "coin",
            "container",
            "domestic item",
            "object",
            "ornament",
            "parts of lived things",
            "photograph",
            "sherd",
            "tile",
            "utility item",
            "weapon",
            "weight"
        ],
        "MAX_SEQUENCE_LEN": 256
    }
    oc_model = MetadataModelLoader.get_oc_material_model(oc_sample_config)
    # load the model predictor
    ocsp = OpenContextSamplePredictor(oc_model)
    assert ocsp is not None
    pred_results = ocsp.predict_sample_type(oc_source_record)
    result = pred_results[0]
    label, prob = result.value, result.confidence
    assert type(label) == str and type(prob) == int or type(prob) == float
