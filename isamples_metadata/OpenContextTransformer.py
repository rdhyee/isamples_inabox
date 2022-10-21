import typing
from typing import Optional

import isamples_metadata.Transformer
from isamples_metadata.Transformer import (
    Transformer,
    AbstractCategoryMetaMapper,
    StringEqualityCategoryMapper,
    AbstractCategoryMapper,
)
from isamples_metadata.taxonomy.metadata_models import (
    MetadataModelLoader,
    OpenContextMaterialPredictor,
    OpenContextSamplePredictor
)


class MaterialCategoryMetaMapper(AbstractCategoryMetaMapper):
    _anthropogenicMaterialMapper = StringEqualityCategoryMapper(
        [
            "Architectural Element",
            "Bulk Ceramic",
            "Glass",
            "Object",
            "Pottery",
            "Sample",
            "Sample, Collection, or Aggregation",
            "Sculpture",
            "Stela",
        ],
        "Anthropogenic material",
    )

    _anthropogenicMetalMapper = StringEqualityCategoryMapper(
        [
            "Coin",
        ],
        "Anthropogenic metal",
    )

    _biogenicMapper = StringEqualityCategoryMapper(
        ["Animal Bone", "Human Bone", "Non Diagnostic Bone", "Shell"],
        "Biogenic non organic material",
    )

    _organicMapper = StringEqualityCategoryMapper(
        [
            "Biological subject, Ecofact",
            "Plant remains",
        ],
        "Organic material",
    )

    _rockMapper = StringEqualityCategoryMapper(["Groundstone"], "Rock")

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._anthropogenicMaterialMapper,
            cls._anthropogenicMetalMapper,
            cls._biogenicMapper,
            cls._organicMapper,
            cls._rockMapper,
        ]


class SpecimenCategoryMetaMapper(AbstractCategoryMetaMapper):
    _organismPartMapper = StringEqualityCategoryMapper(
        [
            "Animal Bone",
            "Human Bone",
            "Non Diagnostic Bone",
        ],
        "Organism part",
    )
    _anthropogenicAggregationMapper = StringEqualityCategoryMapper(
        ["Architectural Element", "Basket", "Bulk Ceramic", "Lot"],
        "Anthropogenic aggregation",
    )
    _biomeAggregationMapper = StringEqualityCategoryMapper(
        ["Biological subject, Ecofact", "Plant remains"], "Biome aggregation"
    )
    _artifactMapper = StringEqualityCategoryMapper(
        ["Coin", "Glass", "Groundstone", "Object", "Pottery", "Sculpture", "Stela"],
        "Artifact",
    )
    _otherSolidObjectMapper = StringEqualityCategoryMapper(
        ["Sample", "Sample, Collection, or Aggregation"], "Other solid object"
    )
    _organismProductMapper = StringEqualityCategoryMapper(
        [
            "Shell",
        ],
        "Organism product",
    )

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._organismPartMapper,
            cls._anthropogenicAggregationMapper,
            cls._biomeAggregationMapper,
            cls._artifactMapper,
            cls._otherSolidObjectMapper,
            cls._organismProductMapper,
        ]


class OpenContextTransformer(Transformer):

    def _citation_uri(self) -> str:
        return self.source_record.get("citation uri") or ""

    def id_string(self) -> str:
        citation_uri = self._citation_uri()
        return f"metadata/{citation_uri.removeprefix(Transformer.N2T_ARK_PREFIX)}"

    def sample_identifier_string(self) -> str:
        return self._citation_uri().removeprefix(Transformer.N2T_PREFIX)

    def sample_label(self) -> str:
        return self.source_record.get("label", Transformer.NOT_PROVIDED)

    def sample_description(self) -> str:
        description_pieces: list[str] = []
        self._transform_key_to_label(
            "early bce/ce", self.source_record, description_pieces
        )
        self._transform_key_to_label(
            "late bce/ce", self.source_record, description_pieces
        )
        self._transform_key_to_label("updated", self.source_record, description_pieces)
        for consists_of_dict in self.source_record.get("Consists of", []):
            self._transform_key_to_label(
                "label", consists_of_dict, description_pieces, "Consists of"
            )
        for has_type_dict in self.source_record.get("Has type", []):
            self._transform_key_to_label(
                "label", has_type_dict, description_pieces, "Has type"
            )
        for has_anatomical_dict in self.source_record.get(
            "Has anatomical identification", []
        ):
            self._transform_key_to_label(
                "label",
                has_anatomical_dict,
                description_pieces,
                "Has anatomical identification",
            )
        for temporal_coverage_dict in self.source_record.get("Temporal Coverage", []):
            self._transform_key_to_label(
                "label",
                temporal_coverage_dict,
                description_pieces,
                "Temporal coverage",
            )
        return Transformer.DESCRIPTION_SEPARATOR.join(description_pieces)

    def sample_registrant(self) -> str:
        pass

    def sample_sampling_purpose(self) -> str:
        pass

    def has_context_categories(self) -> typing.List[str]:
        return ["Site of past human activities"]

    def has_material_categories(self) -> typing.List[str]:
        item_category = self.source_record.get("item category") or ""
        if item_category == "":
            # TODO : need more specification on when to call the predict function
            # call the classifier for prediction
            ocm_model = MetadataModelLoader.get_oc_material_model()
            ocmp = OpenContextMaterialPredictor(ocm_model)
            return [prediction.value for prediction in ocmp.predict_material_type(self.source_record)]
        return MaterialCategoryMetaMapper.categories(item_category)

    def has_specimen_categories(self) -> typing.List[str]:
        item_category = self.source_record.get("item category") or ""
        if item_category == "":
            # TODO : need more specification on when to call the predict function
            # call the classifier for prediction
            ocm_model = MetadataModelLoader.get_oc_sample_model()
            ocsp = OpenContextSamplePredictor(ocm_model)
            return [prediction.value for prediction in ocsp.predict_sample_type(self.source_record)]
        return SpecimenCategoryMetaMapper.categories(item_category)

    def _context_label_pieces(self) -> typing.List[str]:
        context_label = self.source_record.get("context label")
        if type(context_label) is str and len(context_label) > 0:
            return context_label.split("/")
        else:
            return []

    def keywords(self) -> typing.List[str]:
        return self._context_label_pieces()

    def produced_by_id_string(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_label(self) -> str:
        return self.source_record.get("project label", Transformer.NOT_PROVIDED)

    def produced_by_description(self) -> str:
        return self.source_record.get("project uri", Transformer.NOT_PROVIDED)

    def produced_by_feature_of_interest(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_responsibilities(self) -> typing.List[str]:
        # from ekansa:
        # "Creator" is typically a project PI (Principle Investigator). They may or may not be the person that
        # collected the sample. If given, a "Contributor" is the person that originally collected or first
        # described the specimen.
        responsibilities = []
        creators = self.source_record.get("Creator")
        if creators is not None:
            for creator in creators:
                responsibilities.append(f"creator: {creator.get('label')}")
        contributors = self.source_record.get("Contributor")
        if contributors is not None:
            for contributor in contributors:
                responsibilities.append(f"collector: {contributor.get('label')}")
        return responsibilities

    def produced_by_result_time(self) -> str:
        return self.source_record.get("published", Transformer.NOT_PROVIDED)

    def sampling_site_description(self) -> str:
        return Transformer.NOT_PROVIDED

    def sampling_site_label(self) -> str:
        return self.source_record.get("context label", Transformer.NOT_PROVIDED)

    def sampling_site_elevation(self) -> str:
        return Transformer.NOT_PROVIDED

    def sampling_site_latitude(self) -> Optional[typing.SupportsFloat]:
        return _content_latitude(self.source_record)

    def sampling_site_longitude(self) -> Optional[typing.SupportsFloat]:
        return _content_longitude(self.source_record)

    def sampling_site_place_names(self) -> typing.List:
        return self._context_label_pieces()

    def informal_classification(self) -> typing.List[str]:
        classifications = []
        for consists_of_dict in self.source_record.get("Has taxonomic identifier", []):
            classifications.append(consists_of_dict.get("label"))
        return classifications

    def last_updated_time(self) -> Optional[str]:
        return self.source_record.get("updated", None)

    def authorized_by(self) -> typing.List[str]:
        # Don't have this information
        return []

    def complies_with(self) -> typing.List[str]:
        # Don't have this information
        return []


def _content_latitude(content: typing.Dict) -> Optional[float]:
    return content.get("latitude", None)


def _content_longitude(content: typing.Dict) -> Optional[float]:
    return content.get("longitude", None)


def geo_to_h3(content: typing.Dict) -> Optional[str]:
    return isamples_metadata.Transformer.geo_to_h3(_content_latitude(content), _content_longitude(content))
