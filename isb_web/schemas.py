import typing
import pydantic
import datetime


class ThingListEntry(pydantic.BaseModel):
    id: str
    authority_id: str
    tcreated: typing.Optional[datetime.datetime]
    resolved_status: int
    resolved_url: str
    resolve_elapsed: float

    class Config:
        orm_mode = True

class ThingPage(pydantic.BaseModel):
    total_records: int
    last_page: int
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
