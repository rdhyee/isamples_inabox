import datetime
import typing

import pytest
from sqlmodel import Session, SQLModel, create_engine, select
from sqlmodel.pool import StaticPool

from isb_lib.core import ThingRecordIterator
from isb_lib.models.thing import Thing, ThingIdentifier
from isb_web.sqlmodel_database import (
    get_thing_with_id,
    read_things_summary,
    last_time_thing_created,
    paged_things_with_ids,
    insert_identifiers,
    save_thing, things_for_sitemap,
)
from test_utils import _add_some_things


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
    new_thing = Thing(
        id=id,
        authority_id="test",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
    )
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
    new_thing = Thing(
        id="123456",
        authority_id="test",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
    )
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
    new_thing = Thing(
        id="123456",
        authority_id=test_authority,
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
        tcreated=datetime.datetime.now(),
    )
    session.add(new_thing)
    session.commit()
    new_created = last_time_thing_created(session, test_authority)
    assert new_created is not None


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


def test_things_for_sitemap(session: Session):
    authority = "test"
    _add_some_things(session, 20, authority)
    things = things_for_sitemap(session, None, 200, 100, 0, None)
    # should have a list of 20
    assert 20 == len(things)
    for thing in things:
        assert thing.authority_id == authority
    # remember this for later
    last_tstamp = things[-1].tstamp
    last_id = things[-1].primary_key

    different_authority = "different"
    _add_some_things(session, 20, different_authority)

    # fetch again, should still get the first ones because they have an older tstamp
    fetched_again_things = things_for_sitemap(session, None, 200, 100, 0, None)
    for thing in things:
        assert thing.authority_id == authority

    # Now fetch with the tstamp and id
    new_things = things_for_sitemap(session, None, 200, 100, 0, last_tstamp, last_id)
    # Should get 20, should have the new authority
    assert 20 == len(new_things)
    for new_thing in new_things:
        assert new_thing.authority_id == different_authority


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


def test_thing_with_identifier(session: Session):
    thing_id = "123456"
    new_thing = Thing(
        id=thing_id,
        authority_id="test",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
    )
    session.add(new_thing)
    session.commit()
    test_id = "ark:/123456"
    # Right now, query by that id shouldn't find anything
    thing_with_identifier = get_thing_with_id(session, test_id)
    assert thing_with_identifier is None
    new_identifier = ThingIdentifier(guid=test_id, thing_id=new_thing.primary_key)
    session.add(new_identifier)
    new_identifier2 = ThingIdentifier(guid="http://not.real", thing_id="67890")
    session.add(new_identifier2)
    session.commit()
    # Just added ID, should find it now
    thing_with_identifier = get_thing_with_id(session, test_id)
    assert thing_with_identifier is not None
    assert thing_id == thing_with_identifier.id


def _fetch_thing_identifiers(session: Session) -> typing.List[ThingIdentifier]:
    return session.exec(select(ThingIdentifier)).all()


def _test_insert_identifiers(
    session: Session, thing: Thing
) -> typing.List[ThingIdentifier]:
    session.add(thing)
    session.commit()
    insert_identifiers(session, thing)
    session.commit()
    # should have two ids for specified thing
    ids = _fetch_thing_identifiers(session)
    return ids


def test_insert_identifiers_geome(session: Session):
    thing_id = "123456"
    child_id = "1234567890"
    geome_thing = Thing(
        id=thing_id,
        authority_id="GEOME",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"children": [{"bcid": child_id}]},
    )
    identifiers = _test_insert_identifiers(session, geome_thing)
    assert 2 == len(identifiers)


def test_insert_identifiers_opencontext(session: Session):
    thing_id = "7890"
    citation_uri = "12345"
    opencontext_thing = Thing(
        id=thing_id,
        authority_id="OPENCONTEXT",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"citation uri": citation_uri},
    )
    identifiers = _test_insert_identifiers(session, opencontext_thing)
    assert 2 == len(identifiers)


def test_insert_identifiers_sesar(session: Session):
    thing_id = "7890"
    sesar_thing = Thing(
        id=thing_id,
        authority_id="SESAR",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={},
    )
    identifiers = _test_insert_identifiers(session, sesar_thing)
    assert 1 == len(identifiers)


def test_save_thing(session: Session) -> Thing:
    sesar_thing = Thing(
        id="12345",
        authority_id="SESAR",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={},
    )
    save_thing(session, sesar_thing)
    # we should have a ThingIdentifier now
    ids = _fetch_thing_identifiers(session)
    assert 1 == len(ids)
    return sesar_thing


def test_save_existing_thing(session: Session):
    existing_thing = test_save_thing(session)
    # touch the timestamp to update the Thing
    existing_thing.tstamp = datetime.datetime.now()
    save_thing(session, existing_thing)
    ids = _fetch_thing_identifiers(session)
    # should still have 1 id since we whacked the old ones during the save
    assert 1 == len(ids)
