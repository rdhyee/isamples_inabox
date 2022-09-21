import collections
import logging
import json
import os
from isb_web import config

from isamples_metadata.Transformer import Transformer
from scripts.taxonomy.Model import Model
from scripts.taxonomy.SESARClassifierInput import SESARClassifierInput
from scripts.taxonomy.OpenContextClassifierInput import OpenContextClassifierInput


class MetadataModelLoader:
    """Class that instantiates the pretrained models"""

    # loaded models that will be used for classification
    _SESAR_MATERIAL_MODEL = None
    _OPENCONTEXT_MATERIAL_MODEL = None
    _OPENCONTEXT_SAMPLE_MODEL = None

    @staticmethod
    def load_model_from_path(collection, label_type):
        """
            Set the pretrained models by loading them from the file system
            Prerequisite: In order to use this, make sure that there is a pydantic settings file on the
            at the root of this repository named "isamples_web_config.env" with the config file path, and the model path
            defined

            :param collection : the collection type of the sample
            :param label_type : the field that we want to predict
        """
        # load the model config file
        if collection == "SESAR" and label_type == "material":
            config_path = config.Settings().sesar_material_config_path
        elif collection == "OPENCONTEXT" and label_type == "material":
            config_path = config.Settings().opencontext_material_config_path
        elif collection == "OPENCONTEXT" and label_type == "sample":
            config_path = config.Settings().opencontext_sample_config_path
        # read the model config file
        if not os.path.exists(config_path):
            logging.error(
                "Unable to locate pretrained models",
            )
            return
        with open(config_path) as json_file:
            config_json = json.load(json_file)
        # use the model config to get the pretrained model
        model = Model(config_json)

        # initialize the model fields
        if collection == "SESAR" and label_type == "material":
            MetadataModelLoader._SESAR_MATERIAL_MODEL = model
        elif collection == "OPENCONTEXT" and label_type == "material":
            MetadataModelLoader._OPENCONTEXT_MATERIAL_MODEL = model
        elif collection == "OPENCONTEXT" and label_type == "sample":
            MetadataModelLoader._OPENCONTEXT_SAMPLE_MODEL = model

    @staticmethod
    def initialize_models():
        """
            Invokes the load_model function to load all of the possible models
            that are available based on the config
        """
        MetadataModelLoader.load_model_from_path("SESAR", "material")
        MetadataModelLoader.load_model_from_path("OPENCONTEXT", "material")
        MetadataModelLoader.load_model_from_path("OPENCONTEXT", "sample")

    @staticmethod
    def get_sesar_material_model():
        return MetadataModelLoader._SESAR_MATERIAL_MODEL

    @staticmethod
    def get_oc_material_model():
        return MetadataModelLoader._OPENCONTEXT_MATERIAL_MODEL

    @staticmethod
    def get_oc_sample_model():
        return MetadataModelLoader._OPENCONTEXT_SAMPLE_MODEL


class SESARMaterialPredictor:
    """Material label predictor of SESAR collection"""
    def __init__(self, model: Model):
        self._model = model
        self._model_valid = model is not None
        self._description_map = None

    def checkInformative(self, text):
        """Checks if the record is informative"""
        # if record does not have description field
        #   && no CV words in content
        #       && only contains sampleType
        informative = False
        if "description" not in self._description_map:
            for cv in SESARClassifierInput.SESAR_CV_words:
                if cv in text:
                    informative = True
                    break
        if not informative and "sampleType" in self._description_map and \
                text == self._description_map["sampleType"]:
            informative = False
        else:
            informative = True
        return informative

    def checkInvalid(self, field_to_value):
        """Checks if the record is invalid (not a sample record)

        """
        if field_to_value["igsnPrefix"] != "":
            for test_igsn in SESARClassifierInput.SESAR_test_igsn:
                if test_igsn in field_to_value["igsnPrefix"]:
                    return True
        if field_to_value["sampleType"] == "Hole" or \
                field_to_value["sampleType"] == "Site":
            return True
        return False

    def classify_by_sampleType(self, field_to_value):
        """
        Use the sampleType field in the SESAR record
        If falls into any of the rules -> return defined label
        If it does not -> return None
        """
        if "IODP" in field_to_value["cruiseFieldPrgrm"] or \
                "ODP" in field_to_value["cruiseFieldPrgrm"]:
            if "core" in field_to_value["sampleType"].lower():
                return "Mixture of sediment and rock"
            if field_to_value["sampleType"] == "Individual Sample":
                return "Sediment or Rock"
            if "macrofossil" in field_to_value["description"].lower():
                return "Rock"
        if "dredge" in field_to_value["sampleType"].lower():
            return "Natural Solid Material"
        if field_to_value["primaryLocationType"] == "wetland" and \
                "core" in field_to_value["sampleType"].lower():
            return "Material"
        if field_to_value["sampleType"] == "U-channel":
            return "Sediment"
        if field_to_value["sampleType"] == "CTP":
            return "Liquid"
        if field_to_value["sampleType"] == "Individual Sample>Cylinder":
            return "Material"
        # if the record cannot be classified by the defined rules
        return None

    def classify_by_rule(self, text):
        """ Checks if the record can be classified by rule
        If the record corresponds to a rule, returns the rule-defined label
        Else return None
        """
        # 1. check if record does not have enough information
        # if not enough information given,
        # give "Material" label
        if not self.checkInformative(text):
            return "Material"

        # 2. rule-based classification
        # extract fields that we need to consider for the rules
        fields_to_check = [
            "sampleType",
            "cruiseFieldPrgrm",
            "igsnPrefix",
            "description",
            "primaryLocationType"
        ]
        # build a map that stores the fields that we are interested
        field_to_value = collections.defaultdict(str)
        for key, value in self._description_map.items():
            if key in fields_to_check:
                field_to_value[key] = value

        # check if record is invalid
        # i.e., not a sample
        if self.checkInvalid(field_to_value):
            return "Invalid"

        # check if the fields fall into the rules
        # if it does not, return None
        result = self.classify_by_sampleType(field_to_value)
        if result:
            # map to controlled vocabulary
            return SESARClassifierInput.source_to_CV[result]
        else:
            return None

    def classify_by_machine(self, text):
        """ Returns the machine prediction on the given
        input record
        """
        prediction, prob = self._model.predict(text)
        return (
            SESARClassifierInput.source_to_CV[prediction],
            prob
        )

    def predict_material_type(
        self, source_record: dict
    ):
        """
        Invoke the pre-trained BERT model to predict the material type label for the specified string inputs.

        :param source_record: the raw source of a record
        :return: iSamples CV that corresponds to the label that is the prediction result of the field
        """
        if not self._model_valid:
            logging.error(
                "Returning Transformer.NOT_PROVIDED since we couldn't load the model at path %s.",
                config.Settings().sesar_material_model_path
            )
            return Transformer.NOT_PROVIDED

        # extract the data that the model requires for classification
        sesar_input = SESARClassifierInput(source_record)
        sesar_input.parse_thing()
        self._description_map = sesar_input.get_description_map()
        # get the input string for prediction
        input_string = sesar_input.get_material_text()
        # get the prediction result
        # first pass : see if the record falls in the defined rules
        label = self.classify_by_rule(input_string)
        if label:
            return (label, -1)  # set sentinel value as probability
        else:
            # second pass : deriving the prediction by machine
            # we pass the text to a pretrained model to get the prediction result
            # load the model
            machine_prediction = self.classify_by_machine(input_string)
            label, prob = machine_prediction
            return (label, prob)


class OpenContextMaterialPredictor:
    """Material label predictor of OpenContext collection"""
    def __init__(self, model: Model):
        self._model = model
        self._model_valid = model is not None
        self._description_map = None

    def classify_by_machine(self, text):
        """ Returns the machine prediction on the given
        input record
        """
        prediction, prob = self._model.predict(text)
        return (
            prediction,
            prob
        )

    def predict_material_type(
        self, source_record: dict
    ):
        """
        Invoke the pre-trained BERT model to predict the material type label for the specified string inputs.

        :param source_record: the raw source of a record
        :return: String label that is the prediction result of the field
        """
        if not self._model_valid:
            logging.error(
                "Returning Transformer.NOT_PROVIDED since we couldn't load the model at path %s.",
                config.Settings().opencontext_material_model_path
            )
            return Transformer.NOT_PROVIDED

        # extract the data that the model requires for classification
        oc_input = OpenContextClassifierInput(source_record)
        oc_input.parse_thing()
        self.description_map = oc_input.get_description_map()
        input_string = oc_input.get_material_text()

        # get the prediction result with necessary fields provided
        label, prob = self.classify_by_machine(input_string)
        return (label, prob)


class OpenContextSamplePredictor:
    """Sample label predictor of OpenContext collection"""
    def __init__(self, model: Model):
        self._model = model
        self._model_valid = model is not None
        self._description_map = None

    def classify_by_machine(self, text):
        """ Returns the machine prediction on the given
        input record
        """
        prediction, prob = self._model.predict(text)
        return (
            prediction,
            prob
        )

    def predict_sample_type(
        self, source_record: dict
    ):
        """
        Invoke the pre-trained BERT model to predict the sample type label for the specified string inputs.

        :param source_record: the raw source of a record
        :return: String label that is the prediction result of the field
        """
        if not self._model_valid:
            logging.error(
                "Returning Transformer.NOT_PROVIDED since we couldn't load the model at path %s.",
                config.Settings().opencontext_sample_model_path
            )
            return Transformer.NOT_PROVIDED

        # extract the data that the model requires for classification
        oc_input = OpenContextClassifierInput(source_record)
        oc_input.parse_thing()
        self.description_map = oc_input.get_description_map()
        input_string = oc_input.get_sample_text()

        # get the prediction result with necessary fields provided
        label, prob = self.classify_by_machine(input_string)
        return (label, prob)
