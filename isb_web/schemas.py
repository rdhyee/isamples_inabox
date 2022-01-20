import typing
import pydantic
import datetime
import fastapi
from sqlmodel import SQLModel


class Url(pydantic.BaseModel):
    url: str


@pydantic.dataclasses.dataclass
class ThingListRequest:
    offset: int = 0
    limit: int = 1000
    status: int = 200
    authority: str = fastapi.Query(None)


class ThingListEntry(SQLModel):
    id: str
    authority_id: str
    tcreated: typing.Optional[datetime.datetime]
    resolved_status: int
    resolved_url: typing.Optional[str]
    resolve_elapsed: float = None

    class Config:
        orm_mode = True


class ThingPage(SQLModel):
    total_records: int
    last_page: int
    params: dict
    data: typing.List[ThingListEntry]


class ThingType(pydantic.BaseModel):
    item_type: str
    count: int

    class Config:
        orm_mode: True


class ThingListmetaStatus(pydantic.BaseModel):
    status: str
    count: int

    class Config:
        orm_mode: True


class ThingListmetaAuth(pydantic.BaseModel):
    authority: str
    count: int

    class Config:
        orm_mode: True


class ThingListMeta(pydantic.BaseModel):
    status: typing.List[ThingListmetaStatus]
    authority: typing.List[ThingListmetaAuth]

    class Config:
        orm_mode: True


class RelationListMeta(pydantic.BaseModel):
    predicate: str
    count: int

    class Config:
        orm_mode = True


class RelationListEntry(pydantic.BaseModel):
    s: str
    p: str
    o: str
    name: typing.Optional[str]
    source: typing.Optional[str]
    tstamp: typing.Optional[datetime.datetime]

    class Config:
        orm_mode = True
