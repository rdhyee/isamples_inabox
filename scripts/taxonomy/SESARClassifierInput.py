class SESARClassifierInput:
    """Takes thing object to convert it into data that the classifier
    requires as input"""
    def __init__(self, thing):
        self.thing = thing
        # gold label field of SESAR
        self.SESAR_material_field = "material"
        self.SESAR_sample_field = "sampleType"
        # fields to fill in through parsing the thing object
        self.description_map = {}
        self.gold_material, self.gold_sample = None, None
        self.material_text, self.sample_text = "", ""

    def build_text(self, description_map, labelType):
        """Return the concatenated text of informative fields in the
        description_map"""
        concatenated_text = ""
        for key, value in description_map.items():
            if key == "igsnPrefix":
                continue
            elif labelType == "material" and key != self.SESAR_material_field:
                concatenated_text += value + " , "
            elif labelType == "sample" and key != self.SESAR_sample_field:
                concatenated_text += value + " , "

        return concatenated_text[:-2]  # remove the last comma

    def parse_thing(self):
        """Return a map that stores the informative fields,
        the concatenated text versions for the material label classifier
        and the sample label classifier, and the gold labels
        """
        # define the informative fields to extract
        description_field = {
            "supplementMetadata": [
                "geologicalAge", "classificationComment",
                "purpose", "primaryLocationType", "geologicalUnit",
                "locality", "localityDescription", "fieldName",
                "purpose", "cruiseFieldPrgrm",
            ],
            "igsnPrefix": [],
            "collectionMethod": [],
            "material": [],
            "sampleType": [],
            "description": [],
            "collectionMethodDescr": [],
        }

        description_map = {}  # saves value of description_field

        # parse the thing and extract data from informative fields
        for key, value in self.thing["description"].items():
            if key in description_field:
                # gold label fields
                if key == self.SESAR_sample_field:
                    self.gold_sample = value
                elif key == self.SESAR_material_field:
                    self.gold_material = value

                # fields that do not have subfields
                if len(description_field[key]) == 0:
                    description_map[key] = value

                # fields that have subfields
                else:
                    for sub_key in value:
                        if sub_key in description_field[key]:
                            description_map[key + "_" + sub_key] = \
                                value[sub_key]

        # build the concatenated text from the description_map
        self.material_text = self.build_text(description_map, "material")
        self.sample_text = self.build_text(description_map, "sample")
        self.description_map = description_map  # save the description_map

    def get_material_text(self):
        return self.material_text

    def get_sample_text(self):
        return self.sample_text

    def get_gold_material(self):
        return self.gold_material

    def get_gold_sample(self):
        return self.gold_sample

    def get_description_map(self):
        return self.description_map
