import typing
import re

from isamples_metadata.isamplesfasttext import SMITHSONIAN_FEATURE_PREDICTOR
import isamples_metadata.Transformer
from isamples_metadata.Transformer import (
    Transformer,
    AbstractCategoryMetaMapper,
    StringEqualityCategoryMapper,
    AbstractCategoryMapper, StringConstantCategoryMapper,
)


class SpecimenCategoryMetaMapper(AbstractCategoryMetaMapper):
    # Leaving this here for documentation purposes, but given the state of the mapping we
    # end up defaulting things to this anyway, so we don't actually end up using this.
    _organism_part_mapper = StringEqualityCategoryMapper(
        [
            "Tissue & parts; leaf; Silica Gel",
            "Tissue & Parts; Other (see Verbatim); trophosome",
            "Tissue & Parts; Other (see Verbatim); plume",
            "Tissue & Parts; Hair",
            "DNA, RNA, Proteins; Whole genomic DNA",
            "DNA, RNA, Proteins; Unknown DNA, RNA, or Protein"
        ],
        "Organism part",
    )

    _biome_aggregation_mapper = StringEqualityCategoryMapper(
        [
            "Tissue & Parts; Mixed tissue sample; MH",
            "Environmental Sample; Host-Associated; Small Intestine RNA",
        ],
        "Biome aggregation",
    )

    _whole_organism_mapper = StringEqualityCategoryMapper(
        [
            "Tissue & Parts; Egg; Multiple eggs",
        ],
        "Whole organism",
    )

    _default_organism_part_mapper = StringConstantCategoryMapper("Organism part")

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._biome_aggregation_mapper,
            cls._whole_organism_mapper,
            cls._default_organism_part_mapper,
        ]


class SmithsonianTransformer(Transformer):
    RESPONSIBILITIES_SPLIT_RE = re.compile(r",|&")

    def _source_id(self) -> str:
        return self.source_record.get("id", "")

    def id_string(self) -> str:
        source_id = self._source_id()
        return f"metadata/{source_id.removeprefix(Transformer.N2T_ARK_NO_HTTPS_PREFIX)}"

    def sample_label(self) -> str:
        return f"{self.source_record.get('scientificName', Transformer.NOT_PROVIDED)} {self.source_record.get('materialSampleID')}"

    def sample_identifier_string(self) -> str:
        return self._source_id().removeprefix(Transformer.N2T_NO_HTTPS_PREFIX)

    def sample_description(self) -> str:
        description_pieces: list[str] = []
        self._transform_key_to_label(
            "basisOfRecord", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "occurrenceRemarks", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "catalogNumber", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "recordNumber", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "fieldNumber", self.source_record, description_pieces
        )
        self._transform_key_to_label("type", self.source_record, description_pieces)
        self._transform_key_to_label(
            "individualCount", self.source_record, description_pieces
        )
        self._transform_key_to_label("sex", self.source_record, description_pieces)
        self._transform_key_to_label(
            "lifeStage", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "preparations", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "disposition", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "otherCatalogNumbers", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "associatedMedia", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "associatedSequences", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "associatedOccurrences", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "startDayOfYear", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "endDayOfYear", self.source_record, description_pieces
        )
        return Transformer.DESCRIPTION_SEPARATOR.join(description_pieces)

    def has_context_categories(self) -> typing.List[str]:
        categories = SMITHSONIAN_FEATURE_PREDICTOR.predict_sampled_feature(
            [
                self.source_record.get("collectionCode", ""),
                self.source_record.get("habitat", ""),
                self.source_record.get("higherGeography", ""),
                self.source_record.get("locality", ""),
                self.source_record.get("higherClassification", ""),
            ]
        )
        return [categories]

    def has_material_categories(self) -> typing.List[str]:
        material_sample_type = self.source_record.get("materialSampleType")
        if material_sample_type == "Environmental sample":
            return ["Biogenic non organic material"]
        else:
            return ["Organic material"]

    def has_specimen_categories(self) -> typing.List[str]:
        preparation_type = self.source_record.get("preparationType", "")
        return SpecimenCategoryMetaMapper.categories(preparation_type)

    def informal_classification(self) -> typing.List[str]:
        return [self.source_record.get("scientificName", "")]

    def keywords(self) -> typing.List[str]:
        keywords = [self.source_record.get("collectionCode", "")]
        water_body = self.source_record.get("waterBody", "")
        if len(water_body) > 0:
            keywords.append(water_body)
        higher_classification = self.source_record.get("higherClassification", "")
        if len(higher_classification) > 0:
            keywords.extend(higher_classification.split(", "))
        keywords.append(self.source_record.get("scientificName"))
        # TODO: do we want to include the locations in keywords?  Some of the other collections did.
        # If so, which ones?
        return keywords

    def sample_registrant(self) -> str:
        return Transformer.NOT_PROVIDED

    def sample_sampling_purpose(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_id_string(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_label(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_description(self) -> str:
        description_pieces: list[str] = []
        self._transform_key_to_label("verbatimEventDate", self.source_record, description_pieces)
        return " | ".join(description_pieces)

    def produced_by_feature_of_interest(self) -> str:
        return Transformer.NOT_PROVIDED

    def _add_to_responsibilities(self, label: str, responsibilities: typing.List[str]):
        value = self.source_record[label]
        if len(value) > 0:
            for current in self.RESPONSIBILITIES_SPLIT_RE.split(value):
                responsibilities.append(f"{label}: {current.strip()}")

    def produced_by_responsibilities(self) -> typing.List[str]:
        responsibilities: list[str] = []
        self._add_to_responsibilities("recordedBy", responsibilities)
        self._add_to_responsibilities("scientificNameAuthorship", responsibilities)

        # unfortunately it looks like this field uses a ; separator for multiple people
        identified_by = self.source_record["identifiedBy"]
        if len(identified_by) > 0:
            for current in identified_by.split(";"):
                responsibilities.append(f"identifiedBy: {current}")
        return responsibilities

    def produced_by_result_time(self) -> str:
        return self._formatted_date(
            self.source_record.get("year", ""),
            self.source_record.get("month", ""),
            self.source_record.get("day", ""),
        )

    def sampling_site_description(self) -> str:
        description_pieces: list[str] = []
        self._transform_key_to_label(
            "locationID", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "geodeticDatum", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "georeferenceProtocol", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "georeferenceRemarks", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "verbatimLatitude", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "verbatimLongitude", self.source_record, description_pieces
        )
        return " | ".join(description_pieces)

    def sampling_site_label(self) -> str:
        return self.source_record.get("locality", Transformer.NOT_PROVIDED)

    def sampling_site_elevation(self) -> str:
        elevation_pieces: list[str] = []
        self._transform_key_to_label(
            "minimumDepthInMeters", self.source_record, elevation_pieces
        )
        self._transform_key_to_label(
            "maximumDepthInMeters", self.source_record, elevation_pieces
        )
        return " | ".join(elevation_pieces)

    @staticmethod
    def _float_or_none(string_val: typing.Optional[str]) -> typing.Optional[float]:
        if string_val is not None and len(string_val) > 0:
            return float(string_val)
        else:
            return None

    def sampling_site_latitude(self) -> typing.Optional[float]:
        return _content_latitude(self.source_record)

    def sampling_site_longitude(self) -> typing.Optional[float]:
        return _content_longitude(self.source_record)

    def sampling_site_place_names(self) -> typing.List:
        place_names = []
        locality = self.source_record.get("locality", "")
        if len(locality) > 0:
            place_names.append(locality)
        county = self.source_record.get("county", "")
        if len(county) > 0:
            place_names.append(county)
        state = self.source_record.get("stateProvince", "")
        if len(state) > 0:
            place_names.append(state)
        country = self.source_record.get("country", "")
        if len(country) > 0:
            place_names.append(country)
        island = self.source_record.get("island", "")
        if len(island) > 0:
            place_names.append(island)
        island_group = self.source_record.get("islandGroup", "")
        if len(island_group) > 0:
            place_names.append(island_group)
        water_body = self.source_record.get("waterBody", "")
        if len(water_body) > 0:
            place_names.append(water_body)
        continent = self.source_record.get("continent", "")
        if len(continent) > 0:
            place_names.append(continent)
        higher_geography = self.source_record.get("higherGeography", "")
        if len(higher_geography) > 0:
            place_names.append(higher_geography)
        return place_names

    def curation_responsibility(self) -> str:
        return f"{self.source_record.get('institutionCode')} {self.source_record.get('institutionID')}"

    def last_updated_time(self) -> typing.Optional[str]:
        # This doesn't appear to be available in the Smithsonian DwC
        return None

    def authorized_by(self) -> typing.List[str]:
        # Don't have this information
        return []

    def complies_with(self) -> typing.List[str]:
        # Don't have this information
        return []

    def h3_function(self) -> typing.Callable:
        return geo_to_h3


def _content_latitude(source_record: typing.Dict) -> typing.Optional[float]:
    # noinspection PyProtectedMember
    return SmithsonianTransformer._float_or_none(source_record.get("decimalLatitude"))


def _content_longitude(source_record: typing.Dict) -> typing.Optional[float]:
    # noinspection PyProtectedMember
    return SmithsonianTransformer._float_or_none(source_record.get("decimalLongitude"))


def geo_to_h3(content: typing.Dict, resolution: int = Transformer.DEFAULT_H3_RESOLUTION) -> typing.Optional[str]:
    return isamples_metadata.Transformer.geo_to_h3(_content_latitude(content), _content_longitude(content), resolution)
