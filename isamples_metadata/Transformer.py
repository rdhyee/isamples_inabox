from abc import ABC, abstractmethod
import typing
from typing import Optional

import h3

NOT_PROVIDED = "Not Provided"


class Transformer(ABC):
    """Abstract base class for various iSamples provider transformers"""

    NOT_PROVIDED = "Not Provided"

    FEET_PER_METER = 3.28084

    DESCRIPTION_SEPARATOR = " | "

    N2T_PREFIX = "https://n2t.net/"

    N2T_ARK_PREFIX = f"{N2T_PREFIX}ark:/"

    N2T_NO_HTTPS_PREFIX = "http://n2t.net/"

    N2T_ARK_NO_HTTPS_PREFIX = f"{N2T_NO_HTTPS_PREFIX}ark:/"

    RULE_BASED_CONFIDENCE = 1.0

    HUMAN_ENTERED_CONFIDENCE = 2.0

    DEFAULT_H3_RESOLUTION = 15

    @staticmethod
    def _transform_key_to_label(
        key: str,
        source_dict: typing.Dict,
        dest_list: typing.List[str],
        label: Optional[str] = None,
    ):
        if label is None:
            label = key
        value = source_dict.get(key)
        if value is not None and len(str(value)) > 0:
            dest_list.append(f"{label}: {value}")

    @staticmethod
    def _formatted_date(
        year: Optional[str], month: Optional[str], day: Optional[str]
    ) -> str:
        result_time_pieces = []
        if year is not None and len(year) > 0:
            result_time_pieces.append(year)
        if month is not None and len(month) > 0:
            result_time_pieces.append(month.zfill(2))
        if day is not None and len(day) > 0:
            result_time_pieces.append(day.zfill(2))
        return "-".join(result_time_pieces)

    def __init__(self, source_record: typing.Dict):
        self.source_record = source_record

    def transform(self) -> typing.Dict:
        """Do the actual work of transforming a provider record into an iSamples record.

        Arguments:
            source_record -- The provider record to be transformed
        Return value:
            The provider record transformed into an iSamples record
        """
        context_categories = self.has_context_categories()
        material_categories = self.has_material_categories()
        specimen_categories = self.has_specimen_categories()
        transformed_record = {
            "$schema": "../../iSamplesSchemaBasic0.2.json",
            "@id": self.id_string(),
            "label": self.sample_label(),
            "sampleidentifier": self.sample_identifier_string(),
            "description": self.sample_description(),
            "hasContextCategory": context_categories,
            "hasContextCategoryConfidence": self.has_context_category_confidences(context_categories),
            "hasMaterialCategory": material_categories,
            "hasMaterialCategoryConfidence": self.has_material_category_confidences(material_categories),
            "hasSpecimenCategory": specimen_categories,
            "hasSpecimenCategoryConfidence": self.has_specimen_category_confidences(specimen_categories),
            "informalClassification": self.informal_classification(),
            "keywords": self.keywords(),
            "producedBy": {
                "@id": self.produced_by_id_string(),
                "label": self.produced_by_label(),
                "description": self.produced_by_description(),
                "hasFeatureOfInterest": self.produced_by_feature_of_interest(),
                "responsibility": self.produced_by_responsibilities(),
                "resultTime": self.produced_by_result_time(),
                "samplingSite": {
                    "description": self.sampling_site_description(),
                    "label": self.sampling_site_label(),
                    "location": {
                        "elevation": self.sampling_site_elevation(),
                        "latitude": self.sampling_site_latitude(),
                        "longitude": self.sampling_site_longitude(),
                    },
                    "placeName": self.sampling_site_place_names(),
                },
            },
            "registrant": self.sample_registrant(),
            "samplingPurpose": self.sample_sampling_purpose(),
            "curation": {
                "label": self.curation_label(),
                "description": self.curation_description(),
                "accessConstraints": self.curation_access_constraints(),
                "curationLocation": self.curation_location(),
                "responsibility": self.curation_responsibility(),
            },
            "relatedResource": self.related_resources(),
            "authorizedBy": self.authorized_by(),
            "compliesWith": self.complies_with(),
        }
        for index in range(0, 15):
            h3_at_resolution = self.h3_function()(self.source_record, index)
            field_name = f"producedBy_samplingSite_location_h3_{index}"
            transformed_record[field_name] = h3_at_resolution
        return transformed_record

    @abstractmethod
    def id_string(self) -> str:
        """The value for the @id key in the iSamples record"""
        pass

    @abstractmethod
    def sample_identifier_string(self) -> str:
        pass

    @abstractmethod
    def sample_label(self) -> str:
        """A label for the sample in source_record"""
        pass

    @abstractmethod
    def sample_description(self) -> str:
        """A textual description of the sample in source_record"""
        pass

    @abstractmethod
    def sample_registrant(self) -> str:
        """The name of the sample registrant in source_record"""
        pass

    @abstractmethod
    def sample_sampling_purpose(self) -> str:
        """The samplingPurpose of the sample registrant in source_record"""
        pass

    @staticmethod
    def _rule_based_confidence_list_for_categories_list(category_list: list) -> typing.Optional[list]:
        if category_list is None:
            return None
        confidences = []
        for _ in category_list:
            confidences.append(Transformer.RULE_BASED_CONFIDENCE)
        return confidences

    @abstractmethod
    def has_context_categories(self) -> typing.List[str]:
        """Map from the source record into an iSamples context category"""
        pass

    def has_context_category_confidences(self, context_categories: list[str]) -> typing.Optional[typing.List[float]]:
        """If a machine-predicted label is used for context, subclasses should return non-None confidence values"""
        return Transformer._rule_based_confidence_list_for_categories_list(context_categories)

    @abstractmethod
    def has_material_categories(self) -> typing.List[str]:
        """Map from the source record into an iSamples material category"""
        pass

    def has_material_category_confidences(self, material_categories: list[str]) -> typing.Optional[typing.List[float]]:
        """If a machine-predicted label is used for material, subclasses should return non-None confidence values"""
        return Transformer._rule_based_confidence_list_for_categories_list(material_categories)

    @abstractmethod
    def has_specimen_categories(self) -> typing.List[str]:
        """Map from the source record into an iSamples specimen category"""
        pass

    def has_specimen_category_confidences(self, specimen_categories: list[str]) -> typing.Optional[typing.List[float]]:
        """If a machine-predicted label is used for specimen, subclasses should return non-None confidence values"""
        return Transformer._rule_based_confidence_list_for_categories_list(specimen_categories)

    @abstractmethod
    def informal_classification(self) -> typing.List[str]:
        """An informal scientificName"""
        pass

    @abstractmethod
    def keywords(self) -> typing.List[str]:
        """The keywords for the sample in source record"""
        pass

    @abstractmethod
    def produced_by_id_string(self) -> str:
        """The id for the producedBy dictionary, likely used for parent identifiers"""
        pass

    @abstractmethod
    def produced_by_label(self) -> str:
        """The label for the producedBy dictionary"""
        pass

    @abstractmethod
    def produced_by_description(self) -> str:
        """The description for the producedBy dictionary"""
        pass

    @abstractmethod
    def produced_by_feature_of_interest(self) -> str:
        """The feature of interest for the producedBy dictionary"""
        pass

    @abstractmethod
    def produced_by_responsibilities(self) -> typing.List[str]:
        """The responsibility list for the producedBy dictionary"""
        pass

    @abstractmethod
    def produced_by_result_time(self) -> str:
        """The result time for the producedBy dictionary"""
        pass

    @abstractmethod
    def sampling_site_description(self) -> str:
        """The sampling site description"""
        pass

    @abstractmethod
    def sampling_site_label(self) -> str:
        """The sampling site label"""
        pass

    @abstractmethod
    def sampling_site_elevation(self) -> str:
        """The sampling site elevation"""
        pass

    @abstractmethod
    def sampling_site_latitude(self) -> typing.Optional[typing.SupportsFloat]:
        """The sampling site latitude"""
        pass

    @abstractmethod
    def sampling_site_longitude(self) -> typing.Optional[typing.SupportsFloat]:
        """The sampling site longitude"""
        pass

    @abstractmethod
    def sampling_site_place_names(self) -> typing.List:
        """The sampling site longitude"""
        pass

    # region Curation information

    # For the curation fields, not all of the collections have them, so provide stubs returning the empty sentinel
    def curation_label(self) -> str:
        return Transformer.NOT_PROVIDED

    def curation_description(self) -> str:
        return Transformer.NOT_PROVIDED

    def curation_access_constraints(self) -> str:
        return Transformer.NOT_PROVIDED

    def curation_location(self) -> str:
        return Transformer.NOT_PROVIDED

    def curation_responsibility(self) -> str:
        return Transformer.NOT_PROVIDED

    # endregion

    def related_resources(self) -> typing.List[typing.Dict]:
        return []

    @abstractmethod
    def last_updated_time(self) -> typing.Optional[str]:
        """Return the time the record was last modified in the source collection"""
        pass

    @abstractmethod
    def authorized_by(self) -> typing.List[str]:
        """Returns a pointer to the associated permit(s) authorizing the activity, ideally in the form of one or more
        URIs."""
        pass

    @abstractmethod
    def complies_with(self) -> typing.List[str]:
        """Returns a pointer to the associated compliance documentation, ideally in the form of one or more URIs."""
        pass

    @abstractmethod
    def h3_function(self) -> typing.Callable:
        pass


class AbstractCategoryMapper(ABC):
    _destination: str

    @abstractmethod
    def matches(
        self,
        potential_match: str,
        auxiliary_match: typing.Optional[str] = None,
    ) -> bool:
        """Whether a particular String input matches this category mapper"""
        pass

    def append_if_matched(
        self,
        potential_match: str,
        auxiliary_match: typing.Optional[str] = None,
        categories_list: typing.List[str] = list(),
    ):
        if self.matches(potential_match, auxiliary_match):
            categories_list.append(self._destination)

    @property
    def destination(self):
        return self._destination

    @destination.setter
    def destination(self, destination):
        self._destination = destination


class AbstractCategoryMetaMapper(ABC):
    _categoriesMappers: list[AbstractCategoryMapper] = []

    @classmethod
    def categories(
        cls,
        source_category: str,
        auxiliary_source_category: typing.Optional[str] = None,
    ):
        categories: list[str] = []
        if source_category is not None:
            for mapper in cls._categoriesMappers:
                mapper.append_if_matched(
                    source_category, auxiliary_source_category, categories
                )
        if len(categories) == 0:
            categories.append(Transformer.NOT_PROVIDED)
        return categories

    @classmethod
    def categories_mappers(cls) -> list[AbstractCategoryMapper]:
        return []

    def __init_subclass__(cls, **kwargs):
        cls._categoriesMappers = cls.categories_mappers()


class StringConstantCategoryMapper(AbstractCategoryMapper):
    """A mapper that always matches.  Use this as the end of the road."""

    def __init__(
        self,
        destination_category: str,
    ):
        self._destination = destination_category

    def matches(
        self,
        potential_match: str,
        auxiliary_match: typing.Optional[str] = None,
    ) -> bool:
        return True


class StringEqualityCategoryMapper(AbstractCategoryMapper):
    """A mapper that matches iff the potentialMatch exactly matches one of the list of predefined categories"""

    def __init__(
        self,
        categories: list[str],
        destination_category: str,
    ):
        categories = [keyword.lower() for keyword in categories]
        categories = [keyword.strip() for keyword in categories]
        self._categories = categories
        self._destination = destination_category

    def matches(
        self,
        potential_match: str,
        auxiliary_match: typing.Optional[str] = None,
    ) -> bool:
        return potential_match.lower().strip() in self._categories


class StringEndsWithCategoryMapper(AbstractCategoryMapper):
    """A mapper that matches if the potentialMatch ends with the specified string"""

    def __init__(self, ends_with: str, destination_category: str):
        self._endsWith = ends_with.lower().strip()
        self._destination = destination_category

    def matches(
        self,
        potential_match: str,
        auxiliary_match: typing.Optional[str] = None,
    ) -> bool:
        return potential_match.lower().strip().endswith(self._endsWith)


class StringOrderedCategoryMapper(AbstractCategoryMapper):
    """A mapper that runs through a list of mappers and chooses the first one that matches"""

    def __init__(self, submappers: typing.List[AbstractCategoryMapper]):
        self._submappers = submappers

    def matches(
        self,
        potential_match: str,
        auxiliary_match: typing.Optional[str] = None,
    ) -> bool:
        for mapper in self._submappers:
            if mapper.matches(potential_match, auxiliary_match):
                # Note that this isn't thread-safe -- we expect one of these objects per thread
                self.destination = mapper.destination
                return True
        return False


class StringPairedCategoryMapper(AbstractCategoryMapper):
    """A mapper that matches iff the potentialMatch matches both the primaryMatch and secondaryMatch"""

    def __init__(
        self,
        primary_match: str,
        auxiliary_match: str,
        destination_category: str,
    ):
        self._primaryMatch = primary_match.lower().strip()
        self._auxiliaryMatch = auxiliary_match.lower().strip()
        self._destination = destination_category

    def matches(
        self,
        potential_match: str,
        auxiliary_match: typing.Optional[str] = None,
    ) -> bool:
        return (
            potential_match is not None
            and auxiliary_match is not None
            and potential_match.lower().strip() == self._primaryMatch
            and auxiliary_match.lower().strip() == self._auxiliaryMatch
        )


def geo_to_h3(latitude: typing.Optional[float], longitude: typing.Optional[float], resolution: int = Transformer.DEFAULT_H3_RESOLUTION) -> typing.Optional[str]:
    if latitude is not None and longitude is not None:
        return h3.geo_to_h3(latitude, longitude, resolution)
    else:
        return None
