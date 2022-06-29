from fastapi import FastAPI
from pydantic import BaseModel

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
from urllib import parse
from urllib.parse import urljoin

# The FastAPI app that mounts as a sub-app to the main FastAPI app
from isb_web import config

manage_api = FastAPI()

MANAGE_PREFIX = "/manage"

logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("manage")


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
        last_path_component = request.url.path.split("/")[-1]
        if "/" + last_path_component in self._public_paths:
            return await self._app(scope, receive, send)

        token = None
        user = request.session.get("user")

        # Cookie set with auth info
        if user is not None:
            token = user.get("id_token", None)

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


class MintIdentifierParams(BaseModel):
    datacite_metadata: dict


@manage_api.post("/mint_identifier", include_in_schema=False)
def mint_identifier(params: MintIdentifierParams):
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


class MintDraftIdentifierParams(MintIdentifierParams):
    num_drafts: int


@manage_api.post("/mint_draft_identifiers", include_in_schema=False)
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
    # TODO: work this out dynamically -- need to test it out on mars to see what it looks like
    # probably need to manipulate the paths a bit more, too
    print(f"request url is {request.url}")
    url = parse.urlparse(str(request.url))
    print(f"url path is {url.path}")
    base = url.scheme + "://" + url.netloc
    joined = urljoin(base, config.Settings().auth_response_redirect_fragment)
    print(f"joined url is {joined}")
    return starlette.responses.RedirectResponse(url=joined)


@manage_api.get("/logout")
async def logout(request: starlette.requests.Request):
    """
    Logout by removing the cookie from the user session.

    Note that this does not invalidate the JWT, which could continue
    to be used. That's a "feature" of JWTs.
    """
    request.session.pop("user", None)
    return starlette.responses.RedirectResponse(url="/")
