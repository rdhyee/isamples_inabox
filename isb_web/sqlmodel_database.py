import datetime
import sqlalchemy
from typing import Optional, List
from sqlmodel import SQLModel, create_engine, Session, select
from isb_lib.models.thing import Thing
from isb_web.schemas import ThingPage


class SQLModelDAO:
    def __init__(self, db_url: str):
        # This is a strange initializer, but FastAPI wants us to construct the object before we know we
        # want to use it.  So, separate out the object construction from the database connection.
        # In unit tests, this ends up getting swapped out and unused, which is the source of the confusion.
        if db_url is not None:
            self.connect_sqlmodel(db_url)
        else:
            self.engine = None

    def connect_sqlmodel(self, db_url: str):
        self.engine = create_engine(db_url, echo=False)
        SQLModel.metadata.create_all(self.engine)

    def get_session(self) -> Session:
        return Session(self.engine)


def read_things_summary(
    session: Session,
    offset: int,
    limit: int = 100,
    status: int = 200,
    authority: Optional[str] = None,
) -> tuple[int, int, List[ThingPage]]:
    # Fetch summary records of Things (but not the full content), suitable for paging in an API
    count_statement = session.query(Thing)
    if authority is not None:
        count_statement = count_statement.filter(Thing.authority_id == authority)
    count_statement = count_statement.filter(Thing.resolved_status == status)
    overall_count = count_statement.count()
    if limit > 0:
        overall_pages = overall_count / limit
    else:
        overall_pages = 0
    things_statement = select(
        Thing.primary_key,
        Thing.id,
        Thing.authority_id,
        Thing.tcreated,
        Thing.resolved_status,
        Thing.resolved_url,
        Thing.resolve_elapsed,
    )
    if authority is not None:
        things_statement = things_statement.filter(Thing.authority_id == authority)
    things_statement = things_statement.filter(Thing.resolved_status == status)
    if offset > 0:
        things_statement = things_statement.offset(offset)
    if limit > 0:
        things_statement = things_statement.limit(limit)
    things_results = session.exec(things_statement)
    return overall_count, overall_pages, things_results.all()


def get_thing_with_id(session: Session, identifier: str) -> Optional[Thing]:
    statement = select(Thing).filter(Thing.id == identifier)
    return session.exec(statement).first()


def last_time_thing_created(
    session: Session, authority_id: str
) -> Optional[datetime.datetime]:
    # A bit of a hack to work around postgres perf issues.  Limit the number of records to examine by including a
    # time created date in the qualifier.
    one_year_ago = datetime.datetime(
        year=datetime.date.today().year - 1, month=1, day=1
    )
    created_select = (
        select(Thing.tcreated)
        .filter(Thing.authority_id == authority_id)
        .filter(Thing.tcreated >= one_year_ago)
        .limit(1)
        .order_by(Thing.tcreated.desc())
    )
    result = session.exec(created_select).first()
    return result


def paged_things_with_ids(
    session: Session,
    authority: Optional[str] = None,
    status: int = 200,
    limit: int = 100,
    offset: int = 0,
    min_time_created: datetime.datetime = None,
    min_id: int = 0
) -> List[Thing]:
    thing_select = select(Thing).filter(Thing.resolved_status == status)
    if authority is not None:
        thing_select = thing_select.filter(Thing.authority_id == authority)
    if offset > 0:
        thing_select = thing_select.offset(offset)
    if limit > 0:
        thing_select = thing_select.limit(limit)
    if min_id > 0:
        thing_select = thing_select.filter(Thing.primary_key > min_id)
    if min_time_created is not None:
        thing_select = thing_select.filter(Thing.tcreated >= min_time_created)
    thing_select = thing_select.order_by(Thing.primary_key.asc())
    return session.exec(thing_select).all()


def get_thing_meta(session: Session):
    dbq = session.query(
        sqlalchemy.sql.label("status", Thing.resolved_status),
        sqlalchemy.sql.label(
            "count", sqlalchemy.func.count(Thing.resolved_status)
        ),
    ).group_by(Thing.resolved_status)
    meta = {"status": dbq.all()}
    dbq = session.query(
        sqlalchemy.sql.label("authority", Thing.authority_id),
        sqlalchemy.sql.label(
            "count", sqlalchemy.func.count(Thing.authority_id)
        ),
    ).group_by(Thing.authority_id)
    meta["authority"] = dbq.all()
    return meta


def get_sample_types(session: Session):
    dbq = (
        session.query(
            sqlalchemy.sql.label("item_type", Thing.item_type),
            sqlalchemy.sql.label(
                "count", sqlalchemy.func.count(Thing.item_type)
            ),
        )
        .filter(Thing.resolved_status == 200)
        .group_by(Thing.item_type)
    )
    return dbq.all()