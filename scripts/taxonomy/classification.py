from model import get_SESARMaterialModel


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
def get_classification_result(input):
    # load the model
    SESAR_material_model = get_SESARMaterialModel()
    return SESAR_material_model.predict(input)
