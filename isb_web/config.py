from pydantic import BaseSettings


class Settings(BaseSettings):
    """
    Settings for the web application. Values are overridden with a
    configuration file "isb_web_config.env", normally located in
    the root folder of the web application startup.
    """

    logging_config: str = "logging.conf"

    # SQLAlchemy database URL string
    # e.g.: postgresql+psycopg2://DBUSER:DBPASS@localhost/isb_1
    database_url: str = "UNSET"

    # The Solr service URL, must end in "/"
    # e.g. http://localhost:8983/solr/isb_core_records/
    solr_url: str = "UNSET"

    thing_url_path: str = "thing"

    stac_item_url_path: str = "stac_item"

    stac_collection_url_path: str = "stac_collection"

    # The URL to the analytics API
    analytics_url: str = "UNSET"

    # The domain to record analytics events for, needs to be configured as a site in plausible.io
    analytics_domain: str = "UNSET"

    # The URL to the datacite API
    datacite_url: str = "https://api.test.datacite.org/"

    # This shouldn't be checked in.  Set by doing export DATACITE_USERNAME=foobar
    datacite_username: str = ""

    # This shouldn't be checked in.  Set by doing export DATACITE_PASSWORD=foobar
    datacite_password: str = ""

    orcid_hostname: str = "pub.sandbox.orcid.org"

    orcid_token_redirect_uri: str = "http://localhost:8000/orcid_token"

    # This shouldn't be checked in.  Set it by doing export ORCID_CLIENT_ID=foobar
    orcid_client_id: str = ""

    # This must not ever be checked in.  Set it by doing export ORCID_CLIENT_SECRET=foobar
    orcid_client_secret: str = ""

    orcid_issuer: str = "https://sandbox.orcid.org"

    session_middleware_key: str = "81987812-0cf8-459e-b27b-40dabcded856"

    auth_response_redirect_fragment: str = "/isamples_central/ui/#/dois"

    class Config:
        env_file = "isb_web_config.env"
        case_sensitive = False
