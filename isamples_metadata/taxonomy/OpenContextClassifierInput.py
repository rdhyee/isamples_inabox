from isamples_metadata.taxonomy.ClassifierInput import ClassifierInput


class OpenContextClassifierInput(ClassifierInput):
    """Takes OpenContext thing object to convert it into data that the classifier
    requires as input"""

    def __init__(self, thing):
        super().__init__(thing)
        # gold label field of SESAR
        self.OC_material_field = "Consists of_label"
        self.OC_sample_field = "Has type_label"

    def build_text(self, description_map, labelType, field_order):
        """Return the concatenated text of informative fields in the
        description_map"""
        concatenated_text = ""
        for field in field_order:
            if field in description_map:
                # key would be one of the description_field defined in parse_thing
                # e.g. key : item category / value : record's key value such as "Animal Bone"
                key = field
                value = str(description_map[key])
                if value == "":
                    continue
                elif key == "context label":
                    splitted = value.split("/")
                    splitted = " , ".join(splitted)
                    concatenated_text += splitted + " , "
                elif labelType == "material" and key != self.OC_material_field:
                    concatenated_text += value + " , "
                elif labelType == "sample" and key != self.OC_sample_field:
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
            "Has type": ["label"],
            "Consists of": ["label"],
            "early bce/ce": [],
            "late bce/ce": [],
            "project label": [],
            "item category": [],
            "context label": [],
            "Temporal Coverage_label": [],
            "Has taxonomic identifier_label": [],
            "Has anatomical identification_label": [],
        }
        field_order = self.construct_map_order(description_field)
        description_map = {}  # saves value of description_field
        # parse the thing and extract data from informative fields
        for key, value in self.thing.items():
            if key in description_field:
                # gold label fields
                if key == "Has type":
                    self.gold_sample = value[0]["label"]

                elif key == "Consists of":
                    self.gold_material = value[0]["label"]

                # fields that do not have subfields
                if len(description_field[key]) == 0:
                    description_map[key] = value

                # fields that have subfields
                else:
                    for sub_key in value[0]:
                        if sub_key in description_field[key]:
                            description_map[key + "_" + sub_key] = \
                                value[0][sub_key]

        # build the concatenated text from the description_map
        self.material_text = self.build_text(description_map, "material", field_order)
        self.sample_text = self.build_text(description_map, "sample", field_order)
        self.description_map = description_map  # save the description_map
