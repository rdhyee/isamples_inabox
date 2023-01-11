from typing import Optional, Any

import isamples_frictionless
from fastapi import FastAPI, HTTPException, Depends
from sqlmodel import Session
from starlette.responses import Response

from isb_lib.identifiers import datacite
import json
import logging
import requests
import starlette.config
import starlette.requests
import starlette.middleware.cors
import starlette.middleware.sessions
import starlette.types
import starlette.datastructures
import starlette_oauth2_api
import authlib.integrations.starlette_client

from isb_lib.models.namespace import Namespace
from isb_lib.utilities import url_utilities
from isb_web import config, sqlmodel_database
from isb_web.api_types import MintDataciteIdentifierParams, MintDraftIdentifierParams, ManageOrcidForNamespaceParams, \
    AddNamespaceParams, MintNoidyIdentifierParams
from isb_web.sqlmodel_database import SQLModelDAO

# The FastAPI app that mounts as a sub-app to the main FastAPI app
manage_api = FastAPI()
dao: Optional[SQLModelDAO] = None
MANAGE_PREFIX = "/manage"

logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("manage")


# use the same db connection from the main handler
def get_session():
    with dao.get_session() as session:
        yield session


allowed_orcid_ids: list = []


class AuthenticateMiddleware(starlette_oauth2_api.AuthenticateMiddleware):
    """
    Override the __call__ method of the AuthenticateMiddleware to also check
    cookies for auth information. This enables access by either a JWT or the
    authentication information stored in a cookie.
    """

    async def __call__(
        self,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
    ) -> None:
        request = starlette.requests.HTTPConnection(scope)
        last_path_component = url_utilities.last_path_component(request.url)
        if "/" + last_path_component in self._public_paths:
            return await self._app(scope, receive, send)

        token = None
        user = request.session.get("user")

        # Cookie set with auth info
        if user is not None:
            token = user.get("id_token", None)
            orcid_id = user.get("orcid", None)
            if orcid_id not in allowed_orcid_ids:
                return await self._prepare_error_response(
                    "orcid id is not authorized to manage identifiers",
                    401,
                    scope,
                    receive,
                    send,
                )

        # check for authorization header and token on it.
        elif "authorization" in request.headers and request.headers[
            "authorization"
        ].startswith("Bearer "):
            token = request.headers["authorization"][len("Bearer "):]

        elif "authorization" in request.headers:
            _L.debug('No "Bearer" in authorization header')
            return await self._prepare_error_response(
                'The "authorization" header must start with "Bearer "',
                400,
                scope,
                receive,
                send,
            )
        else:
            _L.debug("No authorization header")
            return await self._prepare_error_response(
                'The request does not contain an "authorization" header',
                400,
                scope,
                receive,
                send,
            )

        try:
            provider, claims = self.claims(token)
            scope["oauth2-claims"] = claims
            scope["oauth2-provider"] = provider
            scope["oauth2-jwt"] = token
        except starlette_oauth2_api.InvalidToken as e:
            return await self._prepare_error_response(
                e.errors, 401, scope, receive, send
            )

        return await self._app(scope, receive, send)


oauth = authlib.integrations.starlette_client.OAuth()

# https://gitlab.com/jorgecarleitao/starlette-oauth2-api#how-to-use
manage_api.add_middleware(
    AuthenticateMiddleware,
    providers={
        "orcid": {
            "issuer": config.Settings().orcid_issuer,
            "keys": config.Settings().orcid_issuer + "/oauth/jwks",
            "audience": config.Settings().orcid_client_id,
        }
    },
    public_paths={"/login", "/auth", "/logout"},
)

manage_api.add_middleware(
    starlette.middleware.sessions.SessionMiddleware,
    secret_key=config.Settings().session_middleware_key,
)

# https://www.starlette.io/middleware/#corsmiddleware
manage_api.add_middleware(
    starlette.middleware.cors.CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "POST"],
    allow_headers=["authorization"],
)


# Registration here is using openid, which is a higher level wrapper
# around the oauth end points. Take a look at the info at the
# server_metadata_url
oauth.register(
    name="orcid",
    client_id=config.Settings().orcid_client_id,
    client_secret=config.Settings().orcid_client_secret,
    server_metadata_url=config.Settings().orcid_issuer + "/.well-known/openid-configuration",
    client_kwargs={"scope": "openid"},
    api_base_url=config.Settings().orcid_issuer,
)


@manage_api.post("/mint_datacite_identifier", include_in_schema=False)
def mint_identifier(params: MintDataciteIdentifierParams):
    """Mints an identifier using the datacite API
    Args:
        request: The fastapi request
        params: Class that contains the credentials and the data to post to datacite
    Return: The minted identifier
    """
    post_data = json.dumps(params.datacite_metadata).encode("utf-8")
    result = datacite.create_doi(
        requests.session(),
        post_data,
        config.Settings().datacite_username,
        config.Settings().datacite_password,
    )
    if result is not None:
        return result
    else:
        return "Error minting identifier"


@manage_api.post("/mint_draft_datacite_identifiers", include_in_schema=False)
async def mint_draft_identifiers(params: MintDraftIdentifierParams):
    """Mints draft identifiers using the datacite API
    Args:
        params: Class that contains the credentials, data to post to datacite, and the number of drafts to create
    Return: A list of all the minted DOIs
    """
    post_data = json.dumps(params.datacite_metadata).encode("utf-8")
    dois = await datacite.async_create_draft_dois(
        params.num_drafts,
        None,
        None,
        post_data,
        False,
        config.Settings().datacite_username,
        config.Settings().datacite_password,
    )
    return dois


@manage_api.get("/login")
async def login(request: starlette.requests.Request):
    """
    Initiate OAuth2 login with ORCID
    """
    redirect_uri = request.url_for("auth")
    return await oauth.orcid.authorize_redirect(request, redirect_uri)


@manage_api.get("/auth")
async def auth(request: starlette.requests.Request):
    """
    This method is called back by ORCID oauth. It needs to be in the
    registered callbacks of the ORCID Oauth configuration.
    """
    token = await oauth.orcid.authorize_access_token(request)
    request.session["user"] = dict(token)
    redirect_url = url_utilities.joined_url(str(request.url), config.Settings().auth_response_redirect_fragment)
    return starlette.responses.RedirectResponse(url=redirect_url)


@manage_api.get("/logout")
async def logout(request: starlette.requests.Request):
    """
    Logout by removing the cookie from the user session.

    Note that this does not invalidate the JWT, which could continue
    to be used. That's a "feature" of JWTs.
    """
    request.session.pop("user", None)
    redirect_url = url_utilities.joined_url(str(request.url), config.Settings().logout_redirect_fragment)
    return starlette.responses.RedirectResponse(url=redirect_url)


@manage_api.get("/userinfo")
def userinfo(request: starlette.requests.Request):
    user: Optional[dict] = request.session.get("user")
    if user is not None:
        auth_time = ""
        user_info = user.get("userinfo")
        if user_info is not None:
            auth_time = user_info.get("auth_time")
        response_dict = {
            "name": user.get("name"),
            "orcid": user.get("orcid"),
            "id_token": user.get("id_token"),
            "expires_at": user.get("expires_at"),
            "auth_time": auth_time,
        }
    else:
        # I think the middleware should prevent this, but just in case…
        raise HTTPException(404)
    return response_dict


@manage_api.get("/add_orcid_id")
def add_orcid_id(request: starlette.requests.Request, session: Session = Depends(get_session)):
    user: Optional[dict] = request.session.get("user")
    if user is not None:
        orcid_id = request.query_params.get("orcid_id")
        if user.get("orcid") not in config.Settings().orcid_superusers:
            raise HTTPException(401, "orcid id not authorized to add users")
        person = sqlmodel_database.save_person_with_orcid_id(session, orcid_id)
        allowed_orcid_ids.append(orcid_id)
        return person.primary_key
    else:
        # I think the middleware should prevent this, but just in case…
        raise HTTPException(401, "no session")


@manage_api.post("/add_namespace")
def add_namespace(params: AddNamespaceParams, request: starlette.requests.Request, session: Session = Depends(get_session)):
    orcid_id = _orcid_id_from_session_or_scope(request)
    if orcid_id is None:
        raise HTTPException(401, "no session")
    elif orcid_id not in config.Settings().orcid_superusers:
        raise HTTPException(401, "orcid id not authorized to manage")
    else:
        existing_namespace = sqlmodel_database.namespace_with_shoulder(session, params.shoulder)
        if existing_namespace is not None:
            raise HTTPException(409, f"namespace with shoulder f{params.shoulder} alaready exists")
        else:
            namespace = Namespace()
            namespace.shoulder = params.shoulder
            namespace.allowed_people = params.orcid_ids
            namespace = sqlmodel_database.save_or_update_namespace(session, namespace)
            return namespace


@manage_api.get("/manage_orcid_id_for_namespace")
def manage_orcid_id_for_namespace(params: ManageOrcidForNamespaceParams, request: starlette.requests.Request,
                                  session: Session = Depends(get_session)):
    user: Optional[dict] = request.session.get("user")
    if user is not None:
        if user.get("orcid") not in config.Settings().orcid_superusers:
            raise HTTPException(401, "orcid id not authorized to manage users")
        namespace = sqlmodel_database.namespace_with_shoulder(session, params.shoulder)
        if namespace is None:
            raise HTTPException(404, f"unable to locate namespace with shoulder {params.shoulder}")
        if params.is_remove:
            namespace.remove_allowed_person(params.orcid_id)
        else:
            namespace.add_allowed_person(params.orcid_id)
        sqlmodel_database.save_or_update_namespace(session, namespace)
        return namespace
    else:
        # I think the middleware should prevent this, but just in case…
        raise HTTPException(401, "no session")


def _orcid_id_from_session_or_scope(request: starlette.requests.Request) -> Optional[str]:
    user = request.session.get("user")
    if user is not None:
        return user.get("orcid", None)
    else:
        oauth_claims = request.scope.get("oauth2-claims")
        if oauth_claims is not None:
            return oauth_claims.get("sub")
    return None


@manage_api.post("/mint_noidy_identifiers", include_in_schema=False)
def mint_noidy_identifiers(params: MintNoidyIdentifierParams, request: starlette.requests.Request,
                           session: Session = Depends(get_session)) -> Any:
    """Mints identifiers using the noidy API.  Requires an active session.
    Args:
        params: Class that contains the shoulder and number of identifiers to mint
    Return: A list of all the minted identifiers, or a 404 if the namespace doesn't exist.
    """
    orcid_id = _orcid_id_from_session_or_scope(request)
    if orcid_id is None:
        raise HTTPException(401, "no session")
    namespace = sqlmodel_database.namespace_with_shoulder(session, params.shoulder)
    if namespace is None:
        raise HTTPException(404, f"unable to locate namespace with shoulder {params.shoulder}")
    if namespace.allowed_people is None or orcid_id not in namespace.allowed_people:
        raise HTTPException(401, f"user doesn't have access to {params.shoulder}")
    identifiers = sqlmodel_database.mint_identifiers_in_namespace(session, namespace, params.num_identifiers)
    if params.return_filename is None:
        return identifiers
    else:
        csv_str = isamples_frictionless.insert_identifiers_into_template(identifiers)
        headers = {
            "Content-Disposition": f"inline; filename={params.return_filename}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
        return Response(bytes(csv_str, "utf-8"), headers=headers, media_type="text/csv")
