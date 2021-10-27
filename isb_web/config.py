from pydantic import BaseSettings

class Settings(BaseSettings):
    database_url: str = "UNSET"
    web_root: str = "/"
    solr_url: str = "UNSET"
    client_id: str = ""
    client_secret: str = ""
    authorize_endpoint: str = "https://github.com/login/oauth/authorize"
    access_token_endpoint: str = "https://github.com/login/oauth/access_token"

    class Config:
        env_file = "isb_web_config.env"
