from isamples_metadata.taxonomy.ClassifierInput import ClassifierInput


class SESARClassifierInput(ClassifierInput):
    """Takes SESAR thing object to convert it into data that the classifier
    requires as input"""

    # SESAR test record igsn prefix
    SESAR_test_igsn = [
        "IEKTS",
        "IEKCM",
        "IEKEL",
        "IESDE",
        "MEG",
        "MCT IEJKH",
        "IESER",
        "IELL2",
        "IELL1",
        "LLS",
        "IEHS1",
        "HSU",
        "IECAO",
        "IESBC"
    ]

    # material related words
    SESAR_CV_words = [
        'anthropogenic',
        'biogenic',
        'dispersed',
        'fluid',
        'frozen',
        'gaseous',
        'ice',
        'liquid',
        'material',
        'media',
        'metal',
        'mineral',
        'mixed',
        'natural',
        'non-aqueous',
        'non-organic',
        'organic',
        'particulate',
        'rock',
        'sediment',
        'soil',
        'soil,',
        'solid',
        'water'
    ]

    # SESAR source labels to CV mapping
    source_to_CV = {
        "Biology": "Biogenic non-organic material",
        "EarthMaterial": "Natural Solid Material",
        "Gas": "Gaseous material",
        "Ice": "Ice",
        "Liquid": "Liquid water",
        "Material": "Material",
        "Mineral": "Mineral",
        "Organic Material": "Organic Material",
        "Other": "Material",
        "Particulate": "Particulate",
        "Rock": "Rock",
        "Sediment": "Sediment",
        "Soil": "Soil",
        "experimentalMaterial": "Material",
        "Sediment or Rock": "Natural Solid Material",
        "Natural Solid Material": "Natural Solid Material",
        "Mixed soil, sediment, rock": "Mixed soil, sediment, rock",
        "NotApplicable": "Material",
    }

    def __init__(self, thing):
        super().__init__(thing)
        # gold label field of SESAR
        self.SESAR_material_field = "material"
        self.SESAR_sample_field = "sampleType"

    def build_text(self, description_map, labelType, field_order):
        """Return the concatenated text of informative fields in the
        description_map"""
        concatenated_text = ""
        # use the order of the fields to build text
        # to have consistent text content
        for field in field_order:
            if field in description_map:
                key, value = field, description_map[field]
                if key == "igsnPrefix":
                    continue
                elif value == "":
                    # empty field
                    continue
                elif labelType == "material" and key != self.SESAR_material_field:
                    concatenated_text += value + " , "
                elif labelType == "sample" and key != self.SESAR_sample_field:
                    concatenated_text += value + " , "

        return concatenated_text[:-2]  # remove the last comma

    def construct_map_order(self, description_field):
        # construct order of keys
        # by using description_field dict
        # this is to enforce consistent text field for future purposes
        field_order = []
        for key, subkeys in description_field.items():
            if len(subkeys) == 0:
                field_order.append(key)
            else:
                for subkey in subkeys:
                    field_order.append(key + "_" + subkey)
        return field_order

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
        field_order = self.construct_map_order(description_field)

        description_map = {}  # saves value of description_field
        # parse the thing and extract data from informative fields
        for key, value in self.thing["description"].items():
            # gold label fields
            if key == self.SESAR_sample_field:
                self.gold_sample = value
            if key == self.SESAR_material_field:
                self.gold_material = value
            if key in description_field:
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
        self.material_text = self.build_text(description_map, "material", field_order)
        self.sample_text = self.build_text(description_map, "sample", field_order)
        self.description_map = description_map  # save the description_map
