# iSamples Authentication and Identifier Minting

iSamples identifier minting is provided using [DataCite DOIs](https://datacite.org).  Only users that have been authenticated using [orcid](https://orcid.org) are allowed to mint identifiers.  Additionally, users have to be granted access using the iSamples admin API in order to mint identifiers.

## Authentication using orcid

The first step in minting an identifier is authenticating using orcid.  Users will first need to register for an orcid id at https://orcid.org.  Once users have created their identifier, the login process is initiated by hitting the login URL at the hosted iSamples in a Box instance, e.g. on [Mars](https://mars.cyverse.org/isamples_central/manage/login).

### Python oauth implementation using authlib

Under the covers, orcid is using [openid](https://info.orcid.org/documentation/api-tutorials/api-tutorial-get-and-authenticated-orcid-id/#easy-faq-2731), which is really just an enhancement of the standard [3-legged oauth](https://info.orcid.org/documentation/api-tutorials/api-tutorial-get-and-authenticated-orcid-id/#easy-faq-2537) pattern.

The iSamples implementation of these pieces are located in [manage.py](https://github.com/isamplesorg/isamples_inabox/blob/develop/isb_web/manage.py), which contains all the necessary logic for dealing with managing identifiers.

Most of the functionality is abstracted away in the `authlib` integration, and iSamples just constructs that library and specifies the necessary parameters:

```
oauth = authlib.integrations.starlette_client.OAuth()

oauth.register(
    name="orcid",
    client_id=config.Settings().orcid_client_id,
    client_secret=config.Settings().orcid_client_secret,
    server_metadata_url=config.Settings().orcid_issuer + "/.well-known/openid-configuration",
    client_kwargs={"scope": "openid"},
    api_base_url=config.Settings().orcid_issuer,
)

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
```

The only additional step required is to sure that the `/auth` handler URL is registered as a valid redirect URL in the orcid management console.

### Python middleware using starlette_oauth2_api

The `starlette_oauth2_api.AuthenticateMiddleware` ensures that users are authenticated when they attempt to access any of the FastAPI manage handlers.  The middleware effectively intercepts all manage API requests and redirects them if there isn't a validated session, or the orcid id associated with the session isn't allowed to access the site.  The implementation is relatively straightforward, just adding the middleware to the FastAPI manage handler:

```
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
```
and it takes care of all the key verification details.

## Management of users using config and the API

Once users have their orcid ids established and the auth URLs are registered using the datacite API, the manage API can be used to manage the orcid id access list.  There are two levels of access in the iSamples management scheme:

(1) Superusers are able to add other orcid ids to the permissions list using the `/manage/add_orcid_id` API.  Superusers are controlled via the `orcid_superusers` config key.

(2) Regular users are able to mint identifiers using datacite but are not able to add other users to the list of users that are allowed to mint identifiers.  Superusers may add regular users to the iSB instance by hitting the `/manage/add_orcid_id` endpoint and passing in the `orcid_id` parameter.

## Minting identifiers using datacite

Once all of the above has been completed, you're ready to mint new identifiers with datacite.  There are two ways to do so: by using the management UI or by using a CLI that directly hits the identifier minting endpoint.

(1) Using the management UI, hit the `/dois` URL in the ISB instance e.g. on [Mars](https://mars.cyverse.org/isamples_central/ui/#/dois) and enter the required fields.  You can see the generated JSON in the right-side panel, and that generated JSON is sent to datacite.  Once the datacite response comes back, you'll be able to see the newly minted identifier.

(2) Similarly, you can also use a CLI to directly hit the iSB `/manage/mint_draft_identifiers` endpoint and mint identifiers using your scripting language of choice.  The only prerequisite here is to make sure you've obtained a valid JWT from orcid before calling the endpoint.  You'll need to include the JWT in an `authorization: Bearer` header when you call the endpoint.  A working example of a CLI is available on the [iSamples GitHub](https://github.com/isamplesorg/isamples_inabox/blob/develop/scripts/examples/mint_identifiers_cli.py)