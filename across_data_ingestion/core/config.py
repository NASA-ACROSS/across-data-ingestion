from pydantic_settings import BaseSettings, SettingsConfigDict

from .enums import Environments


class BaseConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class Config(BaseConfig):
    APP_ENV: Environments = Environments.LOCAL
    HOST: str = "localhost"
    PORT: int = 8001
    ROOT_PATH: str = "/api"
    ACROSS_SERVER_URL: str = "http://localhost:8000/api/v1/"
    ACROSS_SERVER_SERVICE_ACCOUNT: str = "local-data-ingestion-service-account"

    def is_local(self):
        return self.APP_ENV == Environments.LOCAL

    def base_url(self):
        return f"{self.HOST}:{self.PORT}{self.ROOT_PATH}"


config = Config()
