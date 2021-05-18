import os
import uvicorn
import datetime
import typing
import fastapi.staticfiles
import fastapi.templating
import fastapi.middleware.cors
import sqlalchemy.orm
from isb_web import database
from isb_web import schemas
from isb_web import crud
from isb_web import config

THIS_PATH = os.path.dirname(os.path.abspath(__file__))
WEB_ROOT = config.Settings().web_root

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


def getDb():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/thing")
async def thing_list_metadata(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """Information about list identifiers"""
    meta = crud.getThingMeta(db)
    return meta


@app.get("/thing/", response_model=typing.List[schemas.ThingListEntry])
async def thing_list(
    offset: int = 0,
    limit: int = 1000,
    status: int = 200,
    authority: str = None,
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """List identifiers available on this service"""
    things = crud.getThings(
        db, offset=offset, limit=limit, status=status, authority_id=authority
    )
    return things


@app.get("/thing/types")
async def thing_list_types(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    return crud.getSampleTypes(db)


@app.get("/thing/{identifier:path}")
async def get_thing(
    identifier: str,
    full: bool = False,
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """Retrieve the record for the specified identifier"""
    item = crud.getThing(db, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    if full:
        return item
    return fastapi.responses.JSONResponse(
        content=item.resolved_content, media_type=item.resolved_media_type
    )


@app.get(
    "/related",
)
async def relation_metadata(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb))
):
    """Retrieve parent(s) of specified identifier"""
    return crud.getPredicateCounts(db)


@app.get(
    "/related/object/{predicate:default=to}/{identifier:path}",
)
async def get_subject_objects(
    identifier: str, db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
    predicate: str = "to"
):
    """Retrieve relations like {s=identifier, p, o}.

    Using predicate "to" or "*" returns all relations to subject.
    """
    if predicate in ["to", "*"]:
        predicate = None
    related = crud.getRelatedObjects(db, identifier, predicate)
    if not related is None:
        return related
    raise fastapi.HTTPException(
        status_code=404, detail=f"Thing not found: {identifier}"
    )


@app.get(
    "/related/subject/{predicate:default=to}/{identifier:path}",
)
async def get_object_subjects(
    identifier: str, db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
    predicate: str = "to"
):
    """Retrieve relations like {s, p, o=identifier}.

    Using predicate "to" or "*" returns all relations to object.
    """
    if predicate in ["to", "*"]:
        predicate = None
    related = crud.getRelatedSubjects(db, identifier, predicate)
    if not related is None:
        return related
    raise fastapi.HTTPException(
        status_code=404, detail=f"Thing not found: {identifier}"
    )


@app.get("/")
async def root(request: fastapi.Request):
    return templates.TemplateResponse("index.html", {"request": request})


def main():
    uvicorn.run("isb_web.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
