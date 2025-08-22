from pydantic_settings import BaseSettings, SettingsConfigDict

from .enums import Environments


class BaseConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class Config(BaseConfig):
    APP_ENV: str = "across-plat-lcl-local"
    RUNTIME_ENV: Environments = Environments.LOCAL
    HOST: str = "localhost"
    PORT: int = 8001
    ROOT_PATH: str = "/api"

    ACROSS_SERVER_HOST: str = "http://localhost"
    ACROSS_SERVER_PORT: int = 8000
    ACROSS_SERVER_ROOT_PATH: str = "/api"
    ACROSS_SERVER_VERSION: str = "/v1"

    ACROSS_SERVER_ID: str = "9798d4e2-fe46-4da9-8708-dd098c27ea8c"
    ACROSS_SERVER_SECRET: str = "local-service-account-key"
    ACROSS_SERVER_ID_PATH: str = "data-ingestion/core-server/client_id"
    ACROSS_SERVER_SECRET_PATH: str = "data-ingestion/core-server/client_secret"

    AWS_REGION: str = "us-east-2"
    AWS_PROFILE: str | None = None

    SPACETRACK_USER: str = "spacetrack-username"
    SPACETRACK_PWD: str = "spacetrack-pwd"

    # Logging
    LOG_LEVEL: str = "DEBUG"
    # Adjusts the output being rendered as JSON (False for dev with pretty-print).
    LOG_JSON_FORMAT: bool = False

    @property
    def ACROSS_SERVER_URL(self):
        return f"{self.ACROSS_SERVER_HOST}:{self.ACROSS_SERVER_PORT}{self.ACROSS_SERVER_ROOT_PATH}{self.ACROSS_SERVER_VERSION}"

    def is_local(self):
        return self.RUNTIME_ENV == Environments.LOCAL

    def base_url(self):
        return f"{self.HOST}:{self.PORT}{self.ROOT_PATH}"

    def across_server_url(self):
        return f"{self.ACROSS_SERVER_HOST}:{self.ACROSS_SERVER_PORT}{self.ACROSS_SERVER_ROOT_PATH}{self.ACROSS_SERVER_VERSION}"


config = Config()
