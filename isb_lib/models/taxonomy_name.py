from typing import Optional

import sqlalchemy
from sqlmodel import SQLModel, Field


class TaxonomyName(SQLModel, table=True):
    primary_key: Optional[int] = Field(
        # Need to use SQLAlchemy here because we can't have the Python attribute named _id or SQLModel won't see it
        sa_column=sqlalchemy.Column(
            "_id",
            sqlalchemy.Integer,
            primary_key=True,
            doc="sequential integer primary key, good for paging",
        ),
    )
    name: str = Field(
        default=None, index=True, nullable=False, description="The name to use for querying purposes"
    )
    kingdom: str = Field(
        default=None, index=False, nullable=False, description="The kingdom associated with the name"
    )