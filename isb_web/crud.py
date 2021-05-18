import sqlalchemy.sql
import sqlalchemy.orm

import igsn_lib.models.thing
import igsn_lib.models.relation


def thingExists(db: sqlalchemy.orm.Session, identifier):
    try:
        res = (
            db.query(igsn_lib.models.thing.Thing.id)
            .filter(igsn_lib.models.thing.Thing.id == identifier)
            .one()
        )
        return True
    except sqlalchemy.exc.NoResultFound:
        pass
    return False


def getThingMeta(db: sqlalchemy.orm.Session):
    dbq = db.query(
        sqlalchemy.sql.label("status", igsn_lib.models.thing.Thing.resolved_status),
        sqlalchemy.sql.label(
            "count", sqlalchemy.func.count(igsn_lib.models.thing.Thing.resolved_status)
        ),
    ).group_by(igsn_lib.models.thing.Thing.resolved_status)
    meta = {"counts": dbq.all()}
    dbq = db.query(
        sqlalchemy.sql.label("authority", igsn_lib.models.thing.Thing.authority_id),
        sqlalchemy.sql.label(
            "count", sqlalchemy.func.count(igsn_lib.models.thing.Thing.authority_id)
        ),
    ).group_by(igsn_lib.models.thing.Thing.authority_id)
    meta["authority"] = dbq.all()
    return meta


def getThings(
    db: sqlalchemy.orm.Session,
    offset: int = 0,
    limit: int = 1000,
    status: int = 200,
    authority_id: str = None,
):
    qry = db.query(igsn_lib.models.thing.Thing)
    qry = qry.filter(igsn_lib.models.thing.Thing.resolved_status == status)
    if not authority_id is None:
        qry = qry.filter(igsn_lib.models.thing.Thing.authority_id == authority_id)
    return qry.offset(offset).limit(limit).all()


def getThing(db: sqlalchemy.orm.Session, identifier: str):
    return (
        db.query(igsn_lib.models.thing.Thing)
        .filter(igsn_lib.models.thing.Thing.id == identifier)
        .first()
    )


def getRelatedObjects(db: sqlalchemy.orm.Session, identifier: str, predicate: str = None):
    """Relations where identifier is subject."""
    q = db.query(igsn_lib.models.relation.Relation).filter(
        igsn_lib.models.relation.Relation.s == identifier
    )
    if not predicate is None:
        q = q.filter(igsn_lib.models.relation.Relation.p == predicate)
    return q.all()


def getRelatedSubjects(db: sqlalchemy.orm.Session, identifier: str, predicate: str = None):
    """Relations where identifier is subject."""
    q = db.query(igsn_lib.models.relation.Relation).filter(
        igsn_lib.models.relation.Relation.o == identifier
    )
    if not predicate is None:
        q = q.filter(igsn_lib.models.relation.Relation.p == predicate)
    return q.all()


def getPredicateCounts(db: sqlalchemy.orm.Session):
    q = db.query(
        sqlalchemy.sql.label("predicate", igsn_lib.models.relation.Relation.p),
        sqlalchemy.sql.label(
            "count", sqlalchemy.func.count(igsn_lib.models.relation.Relation.p)
        ),
    ).group_by(igsn_lib.models.relation.Relation.p)
    return q.all()


def getSampleTypes(db: sqlalchemy.orm.Session):
    dbq = (
        db.query(
            sqlalchemy.sql.label("item_type", igsn_lib.models.thing.Thing.item_type),
            sqlalchemy.sql.label(
                "count", sqlalchemy.func.count(igsn_lib.models.thing.Thing.item_type)
            ),
        )
        .filter(igsn_lib.models.thing.Thing.resolved_status == 200)
        .group_by(igsn_lib.models.thing.Thing.item_type)
    )
    return dbq.all()
