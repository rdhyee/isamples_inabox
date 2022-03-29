from typing import Optional
import sqlalchemy
from sqlmodel import SQLModel, Field


class ISBCoreRecord(SQLModel, table=True):
    id: Optional[str] = Field(
        default=None,
        nullable=False,
        primary_key=True,
        description="The identifier in the original data, which should be a unique identifier (e.g. ark or igsn)",
    )
    isb_core_id: Optional[str] = Field(
        default=None,
        nullable=False,
        description="The iSamples persistent identifier, which is how the sample is uniquely identified in iSamples",
    )
    isb_updated_time: Optional[str] = Field(
        default=None,
        nullable=False,
        description="The time the record was last updated in the iSamples index",
        index=False,
    )
    source_updated_time: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="The time the record was last updated in the original data, if available",
            index=False,
        ),
    )
    label: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A label for the record",
            index=False,
        ),
    )
    # Use the raw SQLAlchemy column in order to get the proper JSON behavior
    search_text: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="A collection of text suitable for use in a full-text search index",
        ),
    )
    sample_description: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A textual description of the sample",
            index=False,
        ),
    )
    context_categories: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="The context categories from the iSamples controlled vocabulary",
        ),
    )
    material_categories: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="The material categories from the iSamples controlled vocabulary",
        ),
    )
    specimen_categories: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="The specimen categories from the iSamples controlled vocabulary",
        ),
    )
    keywords: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Keywords for searching",
        ),
    )
    informal_classifications: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Informal scientific classification of the sample",
        ),
    )
    produced_by_isb_core_id: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="The iSB Core ID of the sample that produced this sample",
            index=False,
        ),
    )
    produced_by_label: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A label for how the sample was produced",
            index=False,
        ),
    )
    produced_by_description: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A text description for how the sample was produced",
            index=False,
        ),
    )
    produced_by_feature_of_interest: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A string specifying whether the sample was produced by a feature of interest",
            index=False,
        ),
    )
    produced_by_responsibilities: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="The name of the people or institution responsible for producing the sample",
        ),
    )
    produced_by_result_time: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="ISO8601 textual representation of the time the sample was collected",
            index=False,
        ),
    )
    produced_by_sampling_site_description: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A textual description of the sampling site",
            index=False,
        ),
    )
    produced_by_sampling_site_label: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A label for the sampling site",
            index=False,
        ),
    )
    produced_by_sampling_site_elevation_in_meters: Optional[float] = (
        Field(
            default=0.0,
            nullable=True,
            description="The elevation in meters of the sampling site",
            index=False,
        ),
    )
    produced_by_sampling_site_location_latitude: Optional[float] = (
        Field(
            default=None,
            nullable=True,
            description="The latitude of the sampling site",
            index=False,
        ),
    )
    produced_by_sampling_site_location_longitude: Optional[float] = (
        Field(
            default=None,
            nullable=True,
            description="The longitude of the sampling site",
            index=False,
        ),
    )
    produced_by_sampling_site_place_names: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="The place names of the sampling site",
        ),
    )
    registrants: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="The registrants of the sample",
        ),
    )
    sampling_purposes: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Why the sample was collected",
        ),
    )
    curation_label: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A label for the sample used during curation",
            index=False,
        ),
    )
    curation_description: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="A textual description of how the sample was curated",
            index=False,
        ),
    )
    curation_access_constraints: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="Access constraints around the sample curation",
            index=False,
        ),
    )
    curation_location: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="Where the sample is curated",
            index=False,
        ),
    )
    curation_responsibility: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="The institution or people responsible for curation",
            index=False,
        ),
    )
    related_resources_isb_core_id: Optional[list] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Identifiers for related resources",
        ),
    )
    source: Optional[str] = (
        Field(
            default=None,
            nullable=True,
            description="Source database where the original record is located",
            index=False,
        ),
    )
