import sqlalchemy.sql
import sqlalchemy.orm

import igsn_lib.models.thing


def getThingMeta(db: sqlalchemy.orm.Session):
    dbq = db.query(
        sqlalchemy.sql.label('status', igsn_lib.models.thing.Thing.resolved_status),
        sqlalchemy.sql.label('count', sqlalchemy.func.count(igsn_lib.models.thing.Thing.resolved_status)),
    ).group_by(igsn_lib.models.thing.Thing.resolved_status)
    meta = {"counts": dbq.all()}
    return meta


def getThings(
    db: sqlalchemy.orm.Session, offset: int = 0, limit: int = 1000, status: int = 200
):
    return db.query(igsn_lib.models.thing.Thing).offset(offset).limit(limit).all()


def getThing(db: sqlalchemy.orm.Session, identifier: str):
    return (
        db.query(igsn_lib.models.thing.Thing)
        .filter(igsn_lib.models.thing.Thing.id == identifier)
        .first()
    )


def getRelated(db: sqlalchemy.orm.Session, identifier: str):
    return (
        db.query(igsn_lib.models.thing.Thing)
        .filter(igsn_lib.models.thing.Thing.id == identifier)
        .first()
    )
