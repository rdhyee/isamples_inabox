import os
import uvicorn
import datetime
import typing
import requests
from fastapi.logger import logger as fastapi_logger
import fastapi.staticfiles
import fastapi.templating
import fastapi.middleware.cors
import sqlalchemy.orm
import accept_types
import pydantic
from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer

from isb_web import database
from isb_web import schemas
from isb_web import crud
from isb_web import config
from isb_web import isb_format
from isamples_metadata.SESARTransformer import SESARTransformer
from isamples_metadata.GEOMETransformer import GEOMETransformer
from isamples_metadata.OpenContextTransformer import OpenContextTransformer

import logging

THIS_PATH = os.path.dirname(os.path.abspath(__file__))
WEB_ROOT = config.Settings().web_root
MEDIA_JSON = "application/json"
MEDIA_NQUADS = "application/n-quads"

app = fastapi.FastAPI(root_path=WEB_ROOT)

app.add_middleware(
    fastapi.middleware.cors.CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount(
    "/static",
    fastapi.staticfiles.StaticFiles(directory=os.path.join(THIS_PATH, "static")),
    name="static",
)
templates = fastapi.templating.Jinja2Templates(
    directory=os.path.join(THIS_PATH, "templates")
)


def predicateToURI(p: str):
    """
    Convert a predicate to a URI for n-quads generation.

    TODO: This is a placeholder implementation.

    Args:
        p: predicate value

    Returns: URI-ified predicate
    """
    if (p.find(":")) >= 0:
        return p
    return f"https://isamples.org/def/predicates/{p}"


def getDb():
    """
    Get an instance of a database session with auto-close

    Returns:
        sqlalchemy.Session
    """
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/thing", response_model=schemas.ThingListMeta)
async def thing_list_metadata(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """Information about list identifiers"""
    meta = crud.getThingMeta(db)
    return meta

@app.get("/thing/", response_model=schemas.ThingPage)
async def thing_list(
    offset: int = fastapi.Query(0,ge=0),
    limit: int = fastapi.Query(1000,lt=10000, gt=0),
    status: int = 200,
    authority: str = fastapi.Query(None),
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """List identifiers of all Things on this service"""
    fastapi_logger.info("test")
    if limit <= 0:
        return "limit must be > 0"
    total_records, npages, things = crud.getThings(
        db, offset=offset, limit=limit, status=status, authority_id=authority
    )
    params = {"limit":limit, "offset":offset, "status":status, "authority":authority}
    return {"params":params, "last_page": npages, "total_records": total_records, "data": things}


@app.get("/thing/types", response_model=typing.List[schemas.ThingType])
async def thing_list_types(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """List of types of things with counts"""
    return crud.getSampleTypes(db)


@app.get("/thing/{identifier:path}", response_model=typing.Any)
async def get_thing(
    identifier: str,
    full: bool = False,
    format: isb_format.ISBFormat = isb_format.ISBFormat.ORIGINAL,
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """Record for the specified identifier"""
    item = crud.getThing(db, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    if full or format == isb_format.ISBFormat.FULL:
        return item
    if format == isb_format.ISBFormat.CORE:
        authority_id = item.authority_id
        if authority_id == "SESAR":
            content = SESARTransformer(item.resolved_content).transform()
        elif authority_id == "GEOME":
            content = GEOMETransformer(item.resolved_content).transform()
        elif authority_id == "OPENCONTEXT":
            content = OpenContextTransformer(item.resolved_content).transform()
        elif authority_id == "SMITHSONIAN":
            content = SmithsonianTransformer(item.resolved_content).transform()
        else:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Core format not available for authority_id: {authority_id}",
            )
    else:
        content = item.resolved_content
    return fastapi.responses.JSONResponse(
        content=content, media_type=item.resolved_media_type
    )


@app.get(
    "/related",
    response_model=typing.List[schemas.RelationListMeta],
)
# async def relation_metadata(db: sqlalchemy.orm.Session = fastapi.Depends((getDb))):
async def relation_metadata():
    """List of predicates with counts"""
    # return crud.getPredicateCounts(db)
    session = requests.session()
    return crud.getPredicateCountsSolr(session)


'''
@app.get(
    "/related/",
    response_model=typing.List[schemas.RelationListEntry],
    responses={
        200: {
            "content": {
                MEDIA_NQUADS: {
                    "example": "s p o [name]  .\n"
                    "s p o [name]  .\n"
                    "s p o [name]  .\n"
                }
            }
        }
    },
)
async def get_related(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    accept: typing.Optional[str] = fastapi.Header("application/json"),
):
    """Relations that match provided s, p, o, source, name.

    Each property is optional. Exact matches only.
    """
    return_type = accept_types.get_best_match(accept, [MEDIA_JSON, MEDIA_NQUADS])
    res = crud.getRelations(db, s, p, o, source, name, offset, limit)
    if return_type == MEDIA_NQUADS:
        rows = []
        for row in res:
            quad = [row.s, predicateToURI(row.p), row.o]
            if row.name is not None or row.name != "":
                quad.append(row.name)
            quad.append(".")
            rows.append(" ".join(quad))
        return fastapi.responses.PlainTextResponse(
            "\n".join(rows), media_type=MEDIA_NQUADS
        )
    return res
'''


@app.get(
    "/related/",
    response_model=typing.List[schemas.RelationListEntry],
    responses={
        200: {
            "content": {
                MEDIA_NQUADS: {
                    "example": "s p o [name]  .\n"
                    "s p o [name]  .\n"
                    "s p o [name]  .\n"
                }
            }
        }
    },
)
async def get_related_solr(
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    accept: typing.Optional[str] = fastapi.Header("application/json"),
):
    """Relations that match provided s, p, o, source, name.

    Each property is optional. Exact matches only.
    """
    return_type = accept_types.get_best_match(accept, [MEDIA_JSON, MEDIA_NQUADS])
    rsession = requests.Session()
    res = crud.getRelationsSolr(rsession, s, p, o, source, name, offset, limit)
    if return_type == MEDIA_NQUADS:
        rows = []
        for row in res:
            quad = [row.s, predicateToURI(row.p), row.o]
            if row.name is not None or row.name != "":
                quad.append(row.name)
            quad.append(".")
            rows.append(" ".join(quad))
        return fastapi.responses.PlainTextResponse(
            "\n".join(rows), media_type=MEDIA_NQUADS
        )
    return res


@app.get("/", include_in_schema=False)
async def root(request: fastapi.Request):
    return templates.TemplateResponse("index.html", {"request": request})


def main():
    logging.basicConfig(level=logging.DEBUG)
    uvicorn.run("isb_web.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] - %(message)s", "%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler() #RotatingFileHandler('/log/abc.log', backupCount=0)
    logging.getLogger().setLevel(logging.NOTSET)
    fastapi_logger.addHandler(handler)
    handler.setFormatter(formatter)

    fastapi_logger.info('****************** Starting Server *****************')
    main()
