from pydantic_settings import BaseSettings, SettingsConfigDict

from .enums import Environments


class BaseConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class Config(BaseConfig):
    APP_ENV: Environments = Environments.LOCAL
    HOST: str = "localhost"
    PORT: int = 8001
    ROOT_PATH: str = "/api"

    ACROSS_SERVER_HOST: str = "http://localhost"
    ACROSS_SERVER_PORT: int = 8000
    ACROSS_SERVER_ROOT_PATH: str = "/api"
    ACROSS_SERVER_VERSION: str = "/v1"

    ACROSS_INGESTION_SERVICE_ACCOUNT_KEY: str = "local-data-ingestion-service-account"

    SPACETRACK_USER: str = "spacetrack-username"
    SPACETRACK_PWD: str = "spacetrack-pwd"

    # Logging
    LOG_LEVEL: str = "DEBUG"
    # Adjusts the output being rendered as JSON (False for dev with pretty-print).
    LOG_JSON_FORMAT: bool = False

    def is_local(self):
        return self.APP_ENV == Environments.LOCAL

    def base_url(self):
        return f"{self.HOST}:{self.PORT}{self.ROOT_PATH}"

    def across_server_url(self):
        return f"{self.ACROSS_SERVER_HOST}:{self.ACROSS_SERVER_PORT}{self.ACROSS_SERVER_ROOT_PATH}{self.ACROSS_SERVER_VERSION}"


config = Config()
