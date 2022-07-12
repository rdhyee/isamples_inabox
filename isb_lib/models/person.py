from typing import Optional

import sqlalchemy
from sqlmodel import SQLModel, Field


class Person(SQLModel, table=True):
    primary_key: Optional[int] = Field(
        # Need to use SQLAlchemy here because we can't have the Python attribute named _id or SQLModel won't see it
        sa_column=sqlalchemy.Column(
            "_id",
            sqlalchemy.Integer,
            primary_key=True,
            doc="sequential integer primary key, good for paging",
        ),
    )
    orcid_id: Optional[str] = Field(
        # Use SQLAlchemy to set the unique constraint
        sa_column=sqlalchemy.Column(
            "orcid_id", sqlalchemy.String, unique=True, doc="Orcid ID for the person"
        )
    )
