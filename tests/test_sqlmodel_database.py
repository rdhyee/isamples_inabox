import datetime

import pytest
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from isb_lib.core import ThingRecordIterator
from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import get_thing_with_id, read_things_summary, last_time_thing_created, \
    paged_things_with_ids


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def test_get_thing_with_id_no_things(session: Session):
    should_be_none = get_thing_with_id(session, "12345")
    assert should_be_none is None


def test_get_thing_with_id_thing(session: Session):
    id = "123456"
    new_thing = Thing(id=id, authority_id="test", resolved_url="http://foo.bar", resolved_status=200, resolved_content = { "foo": "bar" })
    session.add(new_thing)
    session.commit()
    shouldnt_be_none = get_thing_with_id(session, id)
    assert shouldnt_be_none is not None
    assert shouldnt_be_none.primary_key is not None
    assert id == shouldnt_be_none.id


def test_read_things_no_things(session: Session):
    count, pages, data = read_things_summary(session, 0, 0)
    assert 0 == count
    assert 0 == pages
    assert len(data) == 0


def test_read_things_with_things(session: Session):
    new_thing = Thing(id="123456", authority_id="test", resolved_url="http://foo.bar", resolved_status=200, resolved_content = { "foo": "bar" })
    session.add(new_thing)
    session.commit()
    count, pages, data = read_things_summary(session, 0, 0)
    assert 1 == count
    assert 0 == pages
    assert len(data) > 0


def test_last_time_thing_created(session: Session):
    test_authority = "test"
    created = last_time_thing_created(session, test_authority)
    assert created is None
    new_thing = Thing(id="123456", authority_id=test_authority, resolved_url="http://foo.bar", resolved_status=200,
                      resolved_content={"foo": "bar"}, tcreated=datetime.datetime.now())
    session.add(new_thing)
    session.commit()
    new_created = last_time_thing_created(session, test_authority)
    assert new_created is not None


def _add_some_things(session: Session, num_things: int, authority_id: str, tcreated: datetime.datetime = None):
    for i in range(num_things):
        new_thing = Thing(id=str(i), authority_id=authority_id, resolved_url="http://foo.bar", resolved_status=200,
                          resolved_content={"foo": "bar"})
        if tcreated is not None:
            new_thing.tcreated = tcreated
        session.add(new_thing)
    session.commit()


def test_paged_things_with_ids(session: Session):
    authority = "test_authority"
    old_tcreated = datetime.datetime(1978, month=11, day=22)
    _add_some_things(session, 10, authority, old_tcreated)
    things = paged_things_with_ids(session, authority, 200, 10, 0, None, 0)
    # This should be everything, check we get 10 back
    assert 10 == len(things)
    assert type(things[0]) is Thing
    # Now make sure we only get one since we're filtering by id (and we know internally there is a sequence)
    things_with_id = paged_things_with_ids(session, authority, 200, 10, 0, None, 9)
    assert 1 == len(things_with_id)
    now = datetime.datetime.now()
    # Now add some stuff with the current date, and verify we don't get the old ones anymore
    _add_some_things(session, 5, authority, now)
    things_with_tcreated = paged_things_with_ids(session, authority, 200, 10, 0, now, 0)
    assert 5 == len(things_with_tcreated)
    all_things = paged_things_with_ids(session, authority, 200, 100, 0, None, 0)
    assert 15 == len(all_things)


def test_thing_iterator(session: Session):
    authority_id = "test"
    num_things = 10
    _add_some_things(session, num_things, authority_id, None)
    iterator = ThingRecordIterator(session, authority_id, 200, 5, 0, None)
    count_iterated_things = 0
    for thing in iterator.yieldRecordsByPage():
        assert type(thing) is Thing
        count_iterated_things += 1
    assert num_things == count_iterated_things