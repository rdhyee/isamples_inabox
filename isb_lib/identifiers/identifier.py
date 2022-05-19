import datetime
from abc import ABC, abstractmethod
from typing import Optional, Any


class Identifier(ABC):
    def __init__(
        self,
        identifier: str,
        creators: list[str],
        titles: list[str],
        publisher: str,
        publication_year: int = datetime.datetime.now().year,
        resource_type: str = "PhysicalObject",
    ):
        self._identifier = identifier
        self._creators = creators
        self._titles = titles
        self._publisher = publisher
        self._publication_year = publication_year
        self._resource_type = resource_type

    @abstractmethod
    def metadata_dict(self) -> dict:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass


class DataciteIdentifier(Identifier):

    def __init__(
        self,
        doi: Optional[str],
        prefix: Optional[str],
        creators: list[str],
        titles: list[str],
        publisher: str,
        publication_year: int,
        resource_type: str = "PhysicalObject",
    ):
        if doi is None and prefix is None:
            raise ValueError("One of doi or prefix must be specified.")
        if len(creators) == 0:
            raise ValueError("One or more creators must be specified.")
        if len(titles) == 0:
            raise ValueError("One or more titles must be specified.")

        if doi is not None:
            super().__init__(doi, creators, titles, publisher, publication_year, resource_type)
            self._is_doi = True
        elif prefix is not None:
            super().__init__(prefix, creators, titles, publisher, publication_year, resource_type)
            self._is_doi = False

    def metadata_dict(self) -> dict[Any, Any]:
        metadata_dict: dict[Any, Any] = {}
        if self._is_doi:
            metadata_dict["doi"] = self._identifier
        else:
            metadata_dict["prefix"] = self._identifier
        metadata_dict["types"] = {"resourceTypeGeneral": self._resource_type}
        metadata_dict["creators"] = self._creators
        titles = []
        for title in self._titles:
            titles.append({"title": title})
        metadata_dict["titles"] = titles
        metadata_dict["publisher"] = self._publisher
        metadata_dict["publicationYear"] = self._publication_year
        return metadata_dict

    def __str__(self) -> str:
        if self._is_doi:
            return f"doi:{self._identifier}"
        else:
            return f"uassigned doi with prefix:{self._identifier}"


class IGSNIdentifier(DataciteIdentifier):
    def __str__(self) -> str:
        if self._is_doi:
            return f"igsn:{self._identifier}"
        else:
            return f"uassigned igsn with prefix:{self._identifier}"
