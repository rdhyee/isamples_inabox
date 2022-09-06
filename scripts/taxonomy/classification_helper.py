import collections
from model import get_model
from SESARClassifierInput import SESARClassifierInput


def checkInformative(description_map, text, collection):
    """Checks if the record is informative"""
    if collection == "SESAR":
        # if record does not have description field
        #   && no CV words in content
        #       && only contains sampleType
        informative = False
        if "description" not in description_map:
            for cv in SESARClassifierInput.SESAR_CV_words:
                if cv in text:
                    informative = True
                    break
        if not informative and "sampleType" in description_map and \
                text == description_map["sampleType"]:
            informative = False
        else:
            informative = True

    return informative


def checkInvalid(collection, field_to_value):
    """Checks if the record is invalid,
    i.e., not a sample record"""
    if collection == "SESAR":
        if field_to_value["igsnPrefix"] != "":
            for test_igsn in SESARClassifierInput.SESAR_test_igsn:
                if test_igsn in field_to_value["igsnPrefix"]:
                    return True
        if field_to_value["sampleType"] == "Hole" or \
                field_to_value["sampleType"] == "Site":
            return True
    return False


def classify_by_sampleType(field_to_value):
    """Use the sampleType field in the SESAR record and
    check if it falls into any of the defined rules
    If it does not, return None"""
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


def classify_by_rule(description_map, text, collection, labelType):
    """ Checks if the record can be classified by rule
    Returns the label if the record corresponds to a rule,
    if not it would return None
    """
    if collection == "SESAR" and labelType == "material":

        # check if record does not have enough information
        # if not enough information given,
        # give "Material" label
        if not checkInformative(description_map, text, collection):
            return "Material"

        # fields that we need to consider for the rules
        fields_to_check = [
            "sampleType",
            "cruiseFieldPrgrm",
            "igsnPrefix",
            "description",
            "primaryLocationType"
        ]
        field_to_value = collections.defaultdict(str)
        for key, value in description_map.items():
            if key in fields_to_check:
                field_to_value[key] = value

        # check if record is invalid
        # i.e., not a sample
        if checkInvalid(collection, field_to_value):
            return "Invalid"

        # check if the fields fall into the rules
        # if it does not, return None
        result = classify_by_sampleType(field_to_value)
        if not result:
            return result
        else:
            # map to controlled vocabulary
            return SESARClassifierInput.source_to_CV[result]


def classify_by_machine(text, collection, labelType):
    """ Returns the machine prediction on the given
    input record
    """
    # model checkpoint can be downloaded at
    # https://drive.google.com/drive/folders/1FreG1_ivysxPMXH0httxw4Ihftx-R2N6
    # config file should have "FINE_TUNED_MODEL" : path/to/model/checkpoint
    if collection == "SESAR" and labelType == "material":
        model = get_model("scripts/taxonomy/assets/SESAR_material_config.json")
        prediction, prob = model.predict(text)
        return (
            SESARClassifierInput.source_to_CV[prediction],
            prob
        )
