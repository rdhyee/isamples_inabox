import typing
import pydantic
import datetime


class ThingListEntry(pydantic.BaseModel):
    id: str
    tcreated: datetime.datetime
    resolved_status: int

    class Config:
        orm_mode = True


class ThingListmetaCount(pydantic.BaseModel):
    status: int
    count: int


class ThingListmetaAuth(pydantic.BaseModel):
    authority: str
    count: int


class ThingListMeta(pydantic.BaseModel):
    counts: typing.List[ThingListmetaCount]
    authorities: typing.List[ThingListmetaAuth]


