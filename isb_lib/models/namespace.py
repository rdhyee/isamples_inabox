from datetime import datetime
from typing import Optional

import igsn_lib
import igsn_lib.time
import sqlalchemy
from sqlmodel import SQLModel, Field

from isb_lib.models.string_list_type import StringListType


class Namespace(SQLModel, table=True):
    primary_key: Optional[int] = Field(
        # Need to use SQLAlchemy here because we can't have the Python attribute named _id or SQLModel won't see it
        sa_column=sqlalchemy.Column(
            "_id",
            sqlalchemy.Integer,
            primary_key=True,
            doc="sequential integer primary key, good for paging",
        ),
    )
    shoulder: Optional[str] = Field(
        # The shoulder for the namespace, e.g. "1234/fk44" in "ark:1234/fk44w2w"
        sa_column=sqlalchemy.Column(
            "shoulder", sqlalchemy.String, unique=True, doc="The string value for the namespace's shoulder"
        )
    )
    allowed_people: Optional[list[str]] = Field(
        sa_column=sqlalchemy.Column(
            StringListType,
            nullable=True,
            default=None,
            doc="A list of orcid ids that have permission to mint identifiers in this namespace"
        )
    )
    tcreated: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the namespace was created.",
    )
    tstamp: datetime = Field(
        default=igsn_lib.time.dtnow(),
        description="The last time anything was modified in this namespace",
    )
    minter_state: Optional[dict] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Internal state of the minter associated with this namespace",
        ),
    )

    def _allowed_people_copy(self) -> list[str]:
        if self.allowed_people is not None:
            return self.allowed_people.copy()
        else:
            return []

    def add_allowed_person(self, orcid_id: str):
        """Due to intricacies of the SQLAlchemy type system, simply adding to the existing list doesnt seem to dirty
        the field.  Use this as a hack workaround"""
        new_people = self._allowed_people_copy()
        new_people.append(orcid_id)
        self.allowed_people = new_people

    def remove_allowed_person(self, orcid_id: str):
        """Due to intricacies of the SQLAlchemy type system, simply adding to the existing list doesnt seem to dirty
        the field.  Use this as a hack workaround"""
        new_people = self._allowed_people_copy()
        new_people.remove(orcid_id)
        self.allowed_people = new_people
