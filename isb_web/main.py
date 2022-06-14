import os

import uvicorn
import typing
import re
import requests
import fastapi
from fastapi import HTTPException
from fastapi.logger import logger as fastapi_logger
import fastapi.staticfiles
import fastapi.templating
import fastapi.middleware.cors
import fastapi.responses
import accept_types
import json
from fastapi.params import Query, Depends
from pydantic import BaseModel
from sqlmodel import Session

import isb_web
import isamples_metadata.GEOMETransformer
from isb_lib.core import MEDIA_GEO_JSON, MEDIA_JSON, MEDIA_NQUADS
from isb_lib.identifiers import datacite
from isb_lib.models.thing import Thing
from isb_lib.authorization import orcid
from isb_web import sqlmodel_database, analytics
from isb_web.analytics import AnalyticsEvent
from isb_web import schemas
from isb_web import crud
from isb_web import config
from isb_web import isb_format
from isb_web import isb_solr_query
from isamples_metadata.SESARTransformer import SESARTransformer
from isamples_metadata.OpenContextTransformer import OpenContextTransformer
from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer

import authlib.integrations.starlette_client
from starlette.middleware.sessions import SessionMiddleware

import logging

from isb_web.schemas import ThingPage
from isb_web.sqlmodel_database import SQLModelDAO
import isb_lib.stac

THIS_PATH = os.path.dirname(os.path.abspath(__file__))

# Setup logging from the config, but don't
# blowup if the logging config can't be found
try:
    logging.config.fileConfig(config.Settings().logging_config, disable_existing_loggers=False)
except KeyError:
    logging.warning("Could not load logging configuration")
    pass
L = logging.getLogger("ISB")

# Cookie chaff
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

THING_URL_PATH = config.Settings().thing_url_path
STAC_ITEM_URL_PATH = config.Settings().stac_item_url_path
STAC_COLLECTION_URL_PATH = config.Settings().stac_collection_url_path

app = fastapi.FastAPI(openapi_tags=tags_metadata)
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
app.mount(
    "/ui",
    fastapi.staticfiles.StaticFiles(
        directory=os.path.join(THIS_PATH, "ui"), check_dir=False, html=True
    ),
    name="ui",
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


@app.get("/orcid_token", include_in_schema=False)
def get_orcid_token(request: fastapi.Request, response: fastapi.Response):
    """
    Called by a browser as the redirect URI after successfully authenticating with orcid.  Immediately turns the orcid
    provided code into an orcid token response, and sets cookies to store the token data.

    Args:
        request: starlette request instance

    Returns:
        For now, the raw token payload, though this is likely to change to a ReactJS-backed page of some sort.
    """
    code = request.query_params.get("code")
    token_payload = orcid.exchange_code_for_token(code, requests.session())
    if token_payload is not None:
        expires = token_payload.get("expires_in")
        response.set_cookie(key="access_token", value=token_payload.get("access_token"), expires=expires)
        response.set_cookie(key="refresh_token", value=token_payload.get("refresh_token"), expires=expires)
        response.set_cookie(key="orcid", value=token_payload.get("orcid"), expires=expires)
        return token_payload
    else:
        return "Failure"


class MintIdentifierParams(BaseModel):
    orcid_id: str
    datacite_metadata: dict


@app.post("/mint_identifier", include_in_schema=False, response_model=typing.Any)
def mint_identifier(request: fastapi.Request, params: MintIdentifierParams):
    """Mints an identifier using the datacite API
    Args:
        request: The fastapi request
        params: Class that contains the credentials and the data to post to datacite
    Return: The minted identifier
    """
    token = request.headers.get("Authorization")
    authorized = False
    if token is not None and params.orcid_id is not None:
        authorized = orcid.authorize_token_for_orcid_id(token, params.orcid_id)
    if not authorized:
        raise HTTPException(status_code=401, detail="Invalid orcid id or token")
    post_data = json.dumps(params.datacite_metadata).encode("utf-8")
    result = datacite.create_doi(requests.session(), post_data, config.Settings().datacite_username,
                                 config.Settings().datacite_password)
    if result is not None:
        return result
    else:
        return "Error minting identifier"


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


@app.get("/thingpage/{identifier:path}", include_in_schema=False)
async def get_thing_page(request: fastapi.Request, identifier: str, session: Session = Depends(get_session)) -> templates.TemplateResponse:
    # Retrieve record from the database
    item = sqlmodel_database.get_thing_with_id(session, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    content = await thing_resolved_content(identifier, item)
    content_str = json.dumps(content)
    return templates.TemplateResponse(
        "thing.html", {
            "request": request,
            "thing_json": content_str
        }
    )


@app.get(f"/{THING_URL_PATH}", response_model=schemas.ThingListMeta)
async def thing_list_metadata(
    request: fastapi.Request,
    session: Session = Depends(get_session),
):
    """Information about list identifiers"""
    meta = sqlmodel_database.get_thing_meta(session)
    analytics.record_analytics_event(AnalyticsEvent.THING_LIST_METADATA, request)
    return meta


@app.get(f"/{THING_URL_PATH}/", response_model=ThingPage)
def thing_list(
    request: fastapi.Request,
    offset: int = fastapi.Query(0, ge=0),
    limit: int = fastapi.Query(1000, lt=10000, gt=0),
    status: int = 200,
    authority: str = fastapi.Query(None),
    session: Session = Depends(get_session),
):
    total_records, npages, things = sqlmodel_database.read_things_summary(
        session, offset, limit, status, authority
    )
    properties = {
        "authority": authority or "None"
    }
    analytics.record_analytics_event(AnalyticsEvent.THING_LIST, request, properties)
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


@app.get(f"/{THING_URL_PATH}/types", response_model=typing.List[schemas.ThingType])
async def thing_list_types(
    request: fastapi.Request,
    session: Session = Depends(get_session),
):
    """List of types of things with counts"""
    analytics.record_analytics_event(AnalyticsEvent.THING_LIST_TYPES, request)
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
@app.get(f"/{THING_URL_PATH}/select", response_model=typing.Any)
async def get_solr_select(request: fastapi.Request):
    """Send select request to the Solr isb_core_records collection.

    See https://solr.apache.org/guide/8_11/common-query-parameters.html
    """
    # Construct a list of K,V pairs to hand on to the solr request.
    # Can't use a standard dict here because we need to support possible
    # duplicate keys in the request query string.
    defparams = {
        "wt": "json",
        "q": "*:*",
        "fl": "id",
        "rows": 10,
        "start": 0,
    }
    properties = {
        "q": defparams["q"]
    }
    params = []
    # Update params with the provided parameters
    for k, v in request.query_params.multi_items():
        params.append([k, v])
        if k in properties:
            properties[k] = v
    params = set_default_params(params, defparams)
    logging.warning(params)
    analytics.record_analytics_event(AnalyticsEvent.THING_SOLR_SELECT, request, properties)

    # response object is generated in the called method. This is necessary
    # for the streaming response as otherwise the iterator is consumed
    # before returning here, hence defeating the purpose of the streaming
    # response.
    return isb_solr_query.solr_query(params)


@app.post(f"/{THING_URL_PATH}/select", response_model=typing.Any)
async def get_solr_query(
    request: fastapi.Request, query: typing.Any = fastapi.Body(...)
):
    # logging.warning(query)
    return isb_solr_query.solr_query(request.query_params, query=query)


@app.get(f"/{THING_URL_PATH}/stream", response_model=typing.Any)
async def get_solr_stream(request: fastapi.Request):
    '''
    Make a streaming request to the solr index.

    The Solr streaming API offers much richer interaction with the index, though
    also adds risk that a request may perform harmful actions. Here only
    search expressions are constructed from parameters similar to the select API
    to limit potential harm to the index.

    See https://solr.apache.org/guide/8_11/streaming-expressions.html

    Args:
        request: The http request

    Returns:
        fastapi.responses.StreamingResponse
    '''
    # Note that there may be duplicate keys in params, e.g. multiple fq=
    defparams = {
        "wt": "json",
        "q": "*:*",
        "fl": [
            "id",
            f"x:{isb_solr_query.LONGITUDE_FIELD}",
            f"y:{isb_solr_query.LATITUDE_FIELD}",
        ],
        "rows": isb_solr_query.MAX_STREAMING_ROWS,
        "start": 0,
        "select": "search",

        # if true, return counts per location
        "xycount": False,

        # if true, return only records with latitude,longitude
        "onlyxy": True,
    }
    properties = {
        "q": defparams["q"]
    }
    params = []
    # Update params with the provided parameters
    for k, v in request.query_params.multi_items():
        params.append([k, v])
        if k in properties:
            properties[k] = v
    params = set_default_params(params, defparams)
    # L.debug("Params: %s", params)
    analytics.record_analytics_event(AnalyticsEvent.THING_SOLR_STREAM, request, properties)
    return isb_solr_query.solr_searchStream(params)


@app.get(f"/{THING_URL_PATH}/select/info", response_model=typing.Any)
async def get_solr_luke_info(request: fastapi.Request):
    """Retrieve information about the record schema.

    Returns: JSON
    """
    analytics.record_analytics_event(AnalyticsEvent.THING_SOLR_LUKE_INFO, request)
    return isb_solr_query.solr_luke()


class ThingsSitemapParams(BaseModel):
    identifiers: list[str]


@app.post("/things", response_model=typing.Any)
async def get_things_for_sitemap(
    request: fastapi.Request,
    params: ThingsSitemapParams,
    session: Session = Depends(get_session),
):
    """Returns batched things suitable for sitemap ingestion
    Args:
        request: The fastapi request
        params: Class that contains the identifier list, JSON-encoded in the request body
        session: The database session to use to fetch things
    """
    content = sqlmodel_database.get_things_with_ids(session, params.identifiers)
    # things
    # for identifier in params.identifiers:
    #     thing = sqlmodel_database.get_thing_with_id(session, identifier)
    #     if thing is not None:
    #         content.append(thing)
    #     else:
    #         logging.error(f"No thing with identifier {identifier}")
    return content


@app.get(f"/{THING_URL_PATH}/{{identifier:path}}", response_model=typing.Any)
async def get_thing(
    request: fastapi.Request,
    identifier: str,
    full: bool = False,
    format: isb_format.ISBFormat = isb_format.ISBFormat.ORIGINAL,
    session: Session = Depends(get_session),
):
    properties = {
        "identifier": identifier
    }
    analytics.record_analytics_event(AnalyticsEvent.THING_BY_IDENTIFIER, request, properties)
    """Record for the specified identifier"""
    if format == isb_format.ISBFormat.SOLR:
        # Return solr representation of the record
        # Get the solr response, and return the doc portion or
        # and appropriate error condition
        status, doc = isb_solr_query.solr_get_record(identifier)
        if status == 200:
            return fastapi.responses.JSONResponse(
                content=doc, media_type="application/json"
            )
        raise fastapi.HTTPException(
            status_code=status,
            detail=f"Unable to retrieve solr record for identifier: {identifier}"
        )

    # Retrieve record from the database
    item = sqlmodel_database.get_thing_with_id(session, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    if full or format == isb_format.ISBFormat.FULL:
        return item
    if format == isb_format.ISBFormat.CORE:
        content = await thing_resolved_content(identifier, item)
    else:
        content = item.resolved_content
    return fastapi.responses.JSONResponse(
        content=content, media_type=item.resolved_media_type
    )


async def thing_resolved_content(identifier: str, item: Thing) -> dict:
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
    return content


@app.get(f"/{STAC_ITEM_URL_PATH}/{{identifier:path}}", response_model=typing.Any)
async def get_stac_item(
    request: fastapi.Request,
    identifier: str,
    session: Session = Depends(get_session),
):
    properties = {
        "identifier": identifier
    }
    analytics.record_analytics_event(AnalyticsEvent.STAC_ITEM_BY_IDENTIFIER, request, properties)
    # stac wants things to have filenames, so let these requests work, too.
    if identifier.endswith(".json"):
        identifier = identifier.removesuffix(".json")
    status, doc = isb_solr_query.solr_get_record(identifier)
    if status == 200:
        stac_item = isb_lib.stac.stac_item_from_solr_dict(
            doc, "http://isamples.org/stac/", "http://isamples.org/thing/"
        )
        if stac_item is not None:
            return fastapi.responses.JSONResponse(
                content=stac_item, media_type=MEDIA_GEO_JSON
            )
        else:
            # We don't have location data to make a stac item, return a 404
            status = 404

    raise fastapi.HTTPException(
        status_code=status,
        detail=f"Unable to retrieve stac item for identifier: {identifier}"
    )


@app.get(f"/{STAC_COLLECTION_URL_PATH}/{{filename:path}}", response_model=typing.Any)
def get_stac_collection(
    request: fastapi.Request,
    offset: int = fastapi.Query(0, ge=0),
    limit: int = fastapi.Query(1000, lt=10000, gt=0),
    authority: str = fastapi.Query(None),
    filename: str = None
):
    properties = {
        "authority": authority or "None"
    }
    analytics.record_analytics_event(AnalyticsEvent.STAC_COLLECTION, request, properties)

    solr_docs, has_next = isb_solr_query.solr_records_for_stac_collection(authority, offset, limit)
    stac_collection = isb_lib.stac.stac_collection_from_solr_dicts(solr_docs, has_next, offset, limit, authority)
    return fastapi.responses.JSONResponse(
        content=stac_collection, media_type=MEDIA_JSON
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
async def relation_metadata(request: fastapi.Request):
    """List of predicates with counts"""
    # return crud.getPredicateCounts(db)
    analytics.record_analytics_event(AnalyticsEvent.RELATION_METADATA, request)
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
    accept: typing.Optional[str] = fastapi.Header(MEDIA_JSON),
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
    request: fastapi.Request,
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    accept: typing.Optional[str] = fastapi.Header(MEDIA_JSON),
):
    """Relations that match provided s, p, o, source, name.

    Each property is optional. Exact matches only.
    """
    analytics.record_analytics_event(AnalyticsEvent.RELATED_SOLR, request)
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
async def map(request: fastapi.Request):
    return templates.TemplateResponse("spatial.html", {"request": request})


@app.get("/records_orig", include_in_schema=False)
async def records_orig(request: fastapi.Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/", include_in_schema=False)
async def root(request: fastapi.Request):
    return fastapi.responses.RedirectResponse(url=f"{request.scope.get('root_path')}/docs")


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
    # fastapi_logger.addHandler(handler)
    # handler.setFormatter(formatter)

    # gunicorn_error_logger = logging.getLogger("gunicorn.error")
    # gunicorn_logger = logging.getLogger("gunicorn")
    # uvicorn_access_logger = logging.getLogger("uvicorn.access")
    # uvicorn_access_logger.handlers = gunicorn_error_logger.handlers

    # fastapi_logger.handlers = gunicorn_error_logger.handlers

    fastapi_logger.info("****************** Starting Server *****************")
    main()
