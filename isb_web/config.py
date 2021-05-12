from pydantic import BaseSettings

class Settings(BaseSettings):
    database_url: str = "UNSET"
    web_root: str = "/"

    class Config:
        env_file = "isb_web_config.env"
