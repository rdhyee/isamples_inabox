from abc import abstractmethod


class ClassifierInput:
    """Takes thing object to convert it into data that the classifier
    requires as input"""
    def __init__(self, thing):
        self.thing = thing
        # fields to fill in through parsing the thing object
        self.description_map = {}
        self.gold_material, self.gold_sample = None, None
        self.material_text, self.sample_text = "", ""

    @abstractmethod
    def build_text(self, description_map, labelType):
        """Return the concatenated text of informative fields in the
        description_map"""
        pass

    @abstractmethod
    def parse_thing(self):
        """Return a map that stores the informative fields,
        the concatenated text versions for the material label classifier
        and the sample label classifier, and the gold labels
        """
        pass

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
