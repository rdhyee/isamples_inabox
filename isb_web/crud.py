import requests
import sqlalchemy.sql
import sqlalchemy.orm
import logging
import igsn_lib.models.thing
import igsn_lib.models.relation


def getLogger():
    return logging.getLogger("isb_web")


SOLR_RESERVED_CHAR_LIST = [
    "+",
    "-",
    "&",
    "|",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "~",
    "*",
    "?",
    ":",
]


def escapeSolrQueryTerm(term):
    term = term.replace("\\", "\\\\")
    for c in SOLR_RESERVED_CHAR_LIST:
        term = term.replace(c, "\{}".format(c))
    return term


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
    meta = {"status": dbq.all()}
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
    L = getLogger()
    L.info(f"offset: {offset}")
    L.info("%s %s %s %s %s", offset, "", "", "", "")
    nrec_qry = db.query(igsn_lib.models.thing.Thing.id)
    nrec_qry.filter(igsn_lib.models.thing.Thing.resolved_status == status)
    if not authority_id is None:
        nrec_qry = nrec_qry.filter(
            igsn_lib.models.thing.Thing.authority_id == authority_id
        )
    nrecs = nrec_qry.count()
    npages = nrecs / limit
    sql = (
        "SELECT _id, id, authority_id, tcreated, resolved_status, resolved_url, resolve_elapsed"
        " FROM thing WHERE resolved_status=:status"
    )
    params = {
        "status": status,
    }
    if authority_id is not None:
        sql = sql + " AND authority_id=:authority_id"
        params["authority_id"] = authority_id
    sql = sql + " ORDER BY _id OFFSET :offset FETCH NEXT :limit ROWS ONLY"
    params["offset"] = offset
    params["limit"] = limit
    qry = db.execute(sql, params)
    # qry = db.query(igsn_lib.models.thing.Thing)
    # qry = qry.filter(igsn_lib.models.thing.Thing.resolved_status == status)
    # if not authority_id is None:
    #    qry = qry.filter(igsn_lib.models.thing.Thing.authority_id == authority_id)
    # return nrecs, npages, qry.order_by(igsn_lib.models.thing.Thing._id).fetch(limit).all()
    return nrecs, npages, qry.all()


def getThing(db: sqlalchemy.orm.Session, identifier: str):
    return (
        db.query(igsn_lib.models.thing.Thing)
        .filter(igsn_lib.models.thing.Thing.id == identifier)
        .first()
    )


def getRelations(
    db: sqlalchemy.orm.Session,
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
):
    qry = db.query(igsn_lib.models.relation.Relation)
    if s is not None:
        qry = qry.filter(igsn_lib.models.relation.Relation.s == s)
    if p is not None:
        qry = qry.filter(igsn_lib.models.relation.Relation.p == p)
    if o is not None:
        qry = qry.filter(igsn_lib.models.relation.Relation.o == o)
    if source is not None:
        qry = qry.filter(igsn_lib.models.relation.Relation.source == source)
    if name is not None:
        qry = qry.filter(igsn_lib.models.relation.Relation.name == name)
    return qry.offset(offset).limit(limit).all()


def getRelationsSolr(
    rsession: requests.Session,
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    url: str = "http://localhost:8983/solr/isb_rel/",
):
    q = []
    if not s is None:
        q.append(f"s:{escapeSolrQueryTerm(s)}")
    if not p is None:
        q.append(f"p:{escapeSolrQueryTerm(p)}")
    if not o is None:
        q.append(f"o:{escapeSolrQueryTerm(o)}")
    if not source is None:
        q.append(f"source:{escapeSolrQueryTerm(source)}")
    if not name is None:
        q.append(f"name:{escapeSolrQueryTerm(name)}")
    if len(q) == 0:
        q.append("*:*")
    headers = {"Accept": "application/json"}
    params = {
        "q": " AND ".join(q),
        "wt": "json",
        "rows": limit,
        "start": offset,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params).json()
    return res.get("response", {}).get("docs", [])


def getPredicateCounts(db: sqlalchemy.orm.Session):
    q = db.query(
        sqlalchemy.sql.label("predicate", igsn_lib.models.relation.Relation.p),
        sqlalchemy.sql.label(
            "count", sqlalchemy.func.count(igsn_lib.models.relation.Relation.p)
        ),
    ).group_by(igsn_lib.models.relation.Relation.p)
    return q.all()


def getPredicateCountsSolr(
    rsession: requests.Session, url="http://localhost:8983/solr/isb_rel/"
):
    params = {"q": "*:*", "rows": "0", "facet": "true", "facet.field": "p"}
    headers = {"Accept": "application/json"}
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params).json()
    fc = res.get("facet_counts", {}).get("facet_fields", {}).get("p", [])
    result = []
    for i in range(0, len(fc), 2):
        entry = {"predicate": fc[i], "count": fc[i + 1]}
        result.append(entry)
    return result


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
