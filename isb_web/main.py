import os
import uvicorn
import typing
import re
import requests
import fastapi
from fastapi.logger import logger as fastapi_logger
import fastapi.staticfiles
import fastapi.templating
import fastapi.middleware.cors
import fastapi.responses
import accept_types
from fastapi.params import Query, Depends
from sqlmodel import Session

import isb_web
import isamples_metadata.GEOMETransformer
from isb_web import sqlmodel_database
from isb_web import schemas
from isb_web import crud
from isb_web import config
from isb_web import isb_format
from isb_web import isb_solr_query
from isamples_metadata.SESARTransformer import SESARTransformer
from isamples_metadata.OpenContextTransformer import OpenContextTransformer
from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer

import time
import json
import hashlib
import authlib.integrations.starlette_client
from starlette.middleware.sessions import SessionMiddleware

import logging

from isb_web.schemas import ThingPage
from isb_web.sqlmodel_database import SQLModelDAO

THIS_PATH = os.path.dirname(os.path.abspath(__file__))

WEB_ROOT = config.Settings().web_root
MEDIA_JSON = "application/json"
MEDIA_NQUADS = "application/n-quads"
MEDIA_GEO_JSON = "application/geo+json"

# Setup logging from the config, but don't
# blowup if the logging config can't be found
try:
    logging.config.fileConfig(config.Settings().logging_config, disable_existing_loggers=False)
except KeyError as e:
    logging.warning("Could not load logging configuration")
    pass
L = logging.getLogger("ISB")

#Cookie chaff
COOKIE_SECRET = config.Settings().cookie_secret

# OAuth2 application client id
CLIENT_ID = config.Settings().client_id

# OAuth2 application client secret
CLIENT_SECRET = config.Settings().client_secret

# OAuth2 endpoint for client authorization
AUTHORIZE_ENDPOINT = config.Settings().authorize_endpoint

# OAuth2 endpoint for retrieving a token after successful auth
ACCESS_TOKEN_ENDPOINT = config.Settings().access_token_endpoint

# OAuth redirect URL. This is set manually because of a
# disparity between nginx protocol and the gunicorn protocol
# advertised to fastAPI resulting in http instead of https
OAUTH_REDIRECT_URL = config.Settings().oauth_redirect_url

# An OAuth instance for generating the requests
oauth_client = authlib.integrations.starlette_client.OAuth()

# Register the GITHUB OAuth2 urls
oauth_client.register(
    name="github",
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    access_token_url=ACCESS_TOKEN_ENDPOINT,
    access_token_params=None,
    authorize_url=AUTHORIZE_ENDPOINT,
    authorize_params=None,
    api_base_url="https://api.github.com/",
    client_kwargs={"scope": "public_repo, user:email"},
)

tags_metadata = [
    {
        "name": "heatmaps",
        "description": "Heatmap representations of Things, suitable for consumption by mapping APIs",
    }
]

app = fastapi.FastAPI(root_path=WEB_ROOT, openapi_tags=tags_metadata)
dao = SQLModelDAO(None)

app.add_middleware(
    fastapi.middleware.cors.CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SessionMiddleware, secret_key=COOKIE_SECRET)

app.mount(
    "/static",
    fastapi.staticfiles.StaticFiles(directory=os.path.join(THIS_PATH, "static")),
    name="static",
)
app.mount(
    "/sitemaps",
    fastapi.staticfiles.StaticFiles(
        directory=os.path.join(THIS_PATH, "sitemaps"), check_dir=False
    ),
    name="sitemaps",
)
templates = fastapi.templating.Jinja2Templates(
    directory=os.path.join(THIS_PATH, "templates")
)

def isAllowedReferer(referer):
    """
    Checks referer against oauth_allowed_origins patterns

    Args:
        referer: String from request referer header

    Returns:
        boolean, True if allowed, False otherwise

    """
    for allowed in config.Settings().oauth_allowed_origins:
        m = re.match(allowed, referer)
        if m is not None:
            L.debug("Referer allowed: %s", referer)
            return True
    L.info("Login blocked for referer: %s", referer)
    return False



@app.get("/githubauth", include_in_schema=False)
async def authorize(request: fastapi.Request):
    """
    Client is redirected here after authenticating.

    The redirect includes parameters that are used to request a
    token from the authentication provider.

    Args:
        request: Starlette request instance

    Returns:
        Opens page notifying outcome

    """
    state = request.query_params.get("state", None)
    _cli = oauth_client.github
    token = await _cli.authorize_access_token(request)
    user = await _cli.get("user", token=token)
    data = user.json()
    data["origin"] = state
    data["token"] = token["access_token"]
    return templates.TemplateResponse(
        "loggedin.html", {
            "request": request,
            "username": data["login"],
            "name": data["name"],
            "avatar_url": data["avatar_url"],
            "info": data,
        }
    )


@app.get("/login", include_in_schema=False)
async def login(request: fastapi.Request):
    """
    Called by a browser when requesting to authenticate.

    Args:
        request: starlette request instance.

    Returns:
        Redirect to the authentication provider
    """
    referer = request.headers.get("referer", None)
    if referer is None or not isAllowedReferer(referer):
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Please login from an authorized source."
        )
    state = referer
    _cli = oauth_client.github
    # url_for is returning http instead of https, it's a gunicorn issue
    # redirect_url = request.url_for("authorize")
    return await _cli.authorize_redirect(request, OAUTH_REDIRECT_URL, state=state)


@app.on_event("startup")
def on_startup():
    dao.connect_sqlmodel(isb_web.config.Settings().database_url)


def get_session():
    with dao.get_session() as session:
        yield session


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


"""
@app.get("/oauth-callback", name="oauth-callback")
async def oauth_callback(access_token_state=fastapi.Depends(oauth2_authorize_callback)):
#async def oauth_callback(access_token_state):
    atoken, state = access_token_state
    print(f"TOKEN = {atoken}")
    print(f"STATE = {state}")
    token = await oauth2_client.get_access_token(atoken, "http://localhost:8000/")
    return token

@app.get("/login", include_in_schema=False)
async def login():
    url = await oauth2_client.get_authorization_url("http://localhost:8000/oauth-callback?state=%2ftest")
    print(url)
    return RedirectResponse(url)
"""


@app.get("/thing", response_model=schemas.ThingListMeta)
async def thing_list_metadata(
    session: Session = Depends(get_session),
):
    """Information about list identifiers"""
    meta = sqlmodel_database.get_thing_meta(session)
    return meta


@app.get("/thing/", response_model=ThingPage)
def thing_list(
    offset: int = fastapi.Query(0, ge=0),
    limit: int = fastapi.Query(1000, lt=10000, gt=0),
    status: int = 200,
    authority: str = fastapi.Query(None),
    session: Session = Depends(get_session),
):
    total_records, npages, things = sqlmodel_database.read_things_summary(
        session, offset, limit, status, authority
    )

    params = {
        "limit": limit,
        "offset": offset,
        "status": status,
        "authority": authority,
    }
    return {
        "params": params,
        "last_page": npages,
        "total_records": total_records,
        "data": things,
    }


@app.get("/thing/types", response_model=typing.List[schemas.ThingType])
async def thing_list_types(
    session: Session = Depends(get_session),
):
    """List of types of things with counts"""
    return sqlmodel_database.get_sample_types(session)


def set_default_params(params, defs):
    for k in defs.keys():
        fnd = False
        for row in params:
            if k == row[0]:
                fnd = True
                break
        if not fnd:
            params.append([k, defs[k]])
    return params


# TODO: Don't blindly accept user input!
@app.get("/thing/select", response_model=typing.Any)
async def get_solr_select(request: fastapi.Request):
    """Send select request to the Solr isb_core_records collection.

    See https://solr.apache.org/guide/8_9/common-query-parameters.html
    """
    # Somewhat sensible defaults
    defparams = {
        "wt": "json",
        "q": "*:*",
        "fl": "id",
        "rows": 10,
        "start": 0,
    }
    params = []
    # Update params with the provided parameters
    for k, v in request.query_params.multi_items():
        params.append([k, v])
    params = set_default_params(params, defparams)
    logging.warning(params)
    # response object is generated in the called method. This is necessary
    # for the streaming response as otherwise the iterator is consumed
    # before returning here, hence defeating the purpose of the streaming
    # response.
    return isb_solr_query.solr_query(params)


@app.get("/thing/select/info", response_model=typing.Any)
async def get_solr_luke_info():
    """Retrieve information about the record schema.

    Returns: JSON
    """
    return isb_solr_query.solr_luke()


# TODO: Don't blindly accept user input!
@app.get("/thing/select", response_model=typing.Any)
async def get_solr_select(request: fastapi.Request):
    """Send select request to the Solr isb_core_records collection.

    See https://solr.apache.org/guide/8_9/common-query-parameters.html
    """
    # Somewhat sensible defaults
    params = {
        "wt": "json",
        "q": "*:*",
        "fl": "id",
        "rows": 10,
        "start": 0,
    }

    # Update params with the provided parameters
    params.update(request.query_params)

    # response object is generated in the called method. This is necessary
    # for the streaming response as otherwise the iterator is consumed
    # before returning here, hence defeating the purpose of the streaming
    # response.
    return isb_solr_query.solr_query(params)


@app.post("/thing/select", response_model=typing.Any)
async def get_solr_query(
    request: fastapi.Request, query: typing.Any = fastapi.Body(...)
):
    logging.warning(query)
    return isb_solr_query.solr_query(request.query_params, query=query)


@app.get("/thing/select/info", response_model=typing.Any)
async def get_solr_luke_info():
    """Retrieve information about the record schema.

    Returns: JSON
    """
    return isb_solr_query.solr_luke()


@app.get("/thing/{identifier:path}", response_model=typing.Any)
async def get_thing(
    identifier: str,
    full: bool = False,
    format: isb_format.ISBFormat = isb_format.ISBFormat.ORIGINAL,
    session: Session = Depends(get_session),
):
    """Record for the specified identifier"""
    item = sqlmodel_database.get_thing_with_id(session, identifier)
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
            content = (
                isamples_metadata.GEOMETransformer.geome_transformer_for_identifier(
                    identifier, item.resolved_content
                ).transform()
            )
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
    "/things_geojson_heatmap",
    response_model=typing.Any,
    summary="Gets a GeoJSON heatmap of Things",
    tags=["heatmaps"],
)
async def get_things_geojson_heatmap(
    query: str = Query(
        default="*:*",
        description="Solr query to use for selecting Things. Details at https://solr.apache.org/guide/6_6/the-standard-query-parser.html#the-standard-query-parser",
    ),
    fq: str = Query(
        default="",
        description="Filter query to use for selecting Things. Details at https://solr.apache.org/guide/8_9/the-standard-query-parser.html#the-standard-query-parser",
    ),
    min_lat: float = Query(
        default=-90.0,
        description="The minimum latitude for the bounding box in the Solr query. Valid values are -90.0 <= min_lat <= 90.",
    ),
    max_lat: float = Query(
        default=90.0,
        description="The maximum latitude for the bounding box in the Solr query. Valid values are -90.0 <= max_lat <= 90.",
    ),
    min_lon: float = Query(
        default=-180.0,
        description="The minimum longitude for the bounding box in the Solr query. Valid values are -180.0 <= min_lon <= 180.",
    ),
    max_lon: float = Query(
        default=180.0,
        description="The maximum longitude for the bounding box in the Solr query. Valid values are -180.0 <= max_lon <= 180.",
    ),
):
    """
    Returns a GeoJSON heatmap of all Things matching the specified Solr query in the bounding box described by the
    latitude and longitude parameters.  The format of the response is a GeoJSON Feature Collection:
    https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
    """
    bounds = {
        isb_solr_query.MIN_LAT: min_lat,
        isb_solr_query.MAX_LAT: max_lat,
        isb_solr_query.MIN_LON: min_lon,
        isb_solr_query.MAX_LON: max_lon,
    }
    results = isb_solr_query.solr_geojson_heatmap(
        query, bounds, fq=fq, grid_level=None, show_bounds=False, show_solr_bounds=False
    )
    return fastapi.responses.JSONResponse(content=results, media_type=MEDIA_GEO_JSON)


@app.get(
    "/things_leaflet_heatmap",
    response_model=typing.Any,
    summary="Gets a Leaflet heatmap of Things",
    tags=["heatmaps"],
)
async def get_things_leaflet_heatmap(
    query: str = Query(
        default="*:*",
        description="Solr query to use for selecting Things. Details at https://solr.apache.org/guide/8_9/the-standard-query-parser.html#the-standard-query-parser",
    ),
    fq: str = Query(
        default="",
        description="Filter query to use for selecting Things. Details at https://solr.apache.org/guide/8_9/the-standard-query-parser.html#the-standard-query-parser",
    ),
    min_lat: float = Query(
        default=-90.0,
        description="The minimum latitude for the bounding box in the Solr query. Valid values are -90.0 <= min_lat <= 90.",
    ),
    max_lat: float = Query(
        default=90.0,
        description="The maximum latitude for the bounding box in the Solr query. Valid values are -90.0 <= max_lat <= 90.",
    ),
    min_lon: float = Query(
        default=-180.0,
        description="The minimum longitude for the bounding box in the Solr query. Valid values are -180.0 <= min_lon <= 180.",
    ),
    max_lon: float = Query(
        default=180.0,
        description="The maximum longitude for the bounding box in the Solr query. Valid values are -180.0 <= max_lon <= 180.",
    ),
):
    """
    Returns a Leaflet heatmap of all Things matching the specified Solr query in the bounding box described by the
    latitude and longitude parameters.  The format of the response is suitable for consumption by the Leaflet JavaScript
    library https://leafletjs.com
    """
    bounds = {
        isb_solr_query.MIN_LAT: min_lat,
        isb_solr_query.MAX_LAT: max_lat,
        isb_solr_query.MIN_LON: min_lon,
        isb_solr_query.MAX_LON: max_lon,
    }
    results = isb_solr_query.solr_leaflet_heatmap(query, bounds, fq=fq, grid_level=None)
    return fastapi.responses.JSONResponse(content=results, media_type=MEDIA_JSON)


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


@app.get("/map", include_in_schema=False)
async def root(request: fastapi.Request):
    return templates.TemplateResponse("spatial.html", {"request": request})


@app.get("/records_orig", include_in_schema=False)
async def root(request: fastapi.Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/", include_in_schema=False)
async def root(request: fastapi.Request):
    return fastapi.responses.RedirectResponse(url="/docs")


def main():
    logging.basicConfig(level=logging.DEBUG)
    uvicorn.run("isb_web.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    handler = (
        logging.StreamHandler()
    )  # RotatingFileHandler('/log/abc.log', backupCount=0)
    logging.getLogger().setLevel(logging.NOTSET)
    #fastapi_logger.addHandler(handler)
    #handler.setFormatter(formatter)

    # gunicorn_error_logger = logging.getLogger("gunicorn.error")
    # gunicorn_logger = logging.getLogger("gunicorn")
    # uvicorn_access_logger = logging.getLogger("uvicorn.access")
    # uvicorn_access_logger.handlers = gunicorn_error_logger.handlers

    # fastapi_logger.handlers = gunicorn_error_logger.handlers

    fastapi_logger.info("****************** Starting Server *****************")
    main()
