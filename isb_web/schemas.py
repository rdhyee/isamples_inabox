import typing
import pydantic
import datetime

class ThingListEntry(pydantic.BaseModel):
    id: str
    tcreated: datetime.datetime
    resolved_status: int

    class Config:
        orm_mode = True


class ThingListmetaBase(pydantic.BaseModel):
    status: int
    count: int


class ThingListMeta(pydantic.BaseModel):
    counts: typing.List[ThingListmetaBase]

