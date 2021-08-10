from pydantic import BaseSettings

class Settings(BaseSettings):
    database_url: str = "UNSET"
    web_root: str = "/"
    solr_url: str = "UNSET"

    class Config:
        env_file = "isb_web_config.env"
