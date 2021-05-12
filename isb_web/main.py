import os
import uvicorn
import typing
import fastapi.staticfiles
import fastapi.templating
import fastapi.middleware.cors
import sqlalchemy.orm
from isb_web import database
from isb_web import schemas
from isb_web import crud

THIS_PATH = os.path.dirname(os.path.abspath(__file__))

app = fastapi.FastAPI()

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
    status:int = 200,
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """List identifiers available on this service"""
    things = crud.getThings(db, offset=offset, limit=limit, status=status)
    return things


@app.get("/thing/{identifier}")
async def get_thing(
    identifier: str, db: sqlalchemy.orm.Session = fastapi.Depends((getDb))
):
    """Retrieve the record for the specified identifier"""
    item = crud.getThing(db, identifier)
    if item is None:
        raise fastapi.HTTPException(status_code=404, detail=f"Thing not found: {identifier}")
    return fastapi.responses.JSONResponse(
        content=item.resolved_content, media_type=item.resolved_media_type
    )

@app.get("/thing/{identifier}/related")
async def get_thing_related(
    identifier: str, db: sqlalchemy.orm.Session = fastapi.Depends((getDb))
):
    """Retrieve related identifiers for the specified identifier"""
    item = crud.getThing(db, identifier)
    if item is None:
        raise fastapi.HTTPException(status_code=404, detail=f"Thing not found: {identifier}")
    return fastapi.responses.JSONResponse(
        content=item.related
    )



@app.get("/")
async def root(request: fastapi.Request):
    return templates.TemplateResponse("index.html", {"request": request})


def main():
    uvicorn.run("isb_web.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
