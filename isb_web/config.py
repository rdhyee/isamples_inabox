from pydantic import BaseSettings

class Settings(BaseSettings):
    logging_config: str = "logging.conf"
    database_url: str = "UNSET"
    web_root: str = "/"
    solr_url: str = "UNSET"
    client_id: str = ""
    client_secret: str = ""
    cookie_secret: str = "some-random-string"
    authorize_endpoint: str = "https://github.com/login/oauth/authorize"
    access_token_endpoint: str = "https://github.com/login/oauth/access_token"
    oauth_redirect_url: str = "https://mars.cyverse.org/githubauth"
    oauth_allowed_origins: list = [
        r"https://isamplesorg\.github\.io/",
        r"https://mars\.cyverse\.org/",
        r"https?://localhost(:\d{2,5})/",
    ]

    class Config:
        env_file = "isb_web_config.env"
