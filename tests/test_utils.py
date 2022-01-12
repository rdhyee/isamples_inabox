import datetime

from sqlmodel import Session

from isb_lib.models.thing import Thing


def _add_some_things(
    session: Session,
    num_things: int,
    authority_id: str,
    tcreated: datetime.datetime = None,
):
    for i in range(num_things):
        new_thing = Thing(
            id=str(i),
            authority_id=authority_id,
            resolved_url="http://foo.bar",
            resolved_status=200,
            resolved_content={"foo": "bar"},
        )
        if tcreated is not None:
            new_thing.tcreated = tcreated
        session.add(new_thing)
    session.commit()
