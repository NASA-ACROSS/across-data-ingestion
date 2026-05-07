import structlog

from ...core.config import BaseConfig
from ...core.config import config as core_config
from ...util.ssm import SSM

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class Config(BaseConfig):
    SPACETRACK_USER: str = "spacetrack-username"
    SPACETRACK_PWD: str = "spacetrack-pwd"
    SPACETRACK_BASE_URL: str | None = None

    def __init__(self) -> None:
        super().__init__()
        if not core_config.is_local():
            logger.debug("Getting spacetrack credentials from SSM...")

            user_param = SSM.get_parameter("spacetrack/username", core_config.APP_ENV)
            self.SPACETRACK_USER = str(user_param.get("Value"))

            logger.debug(
                "Loaded spacetrack username from SSM", username=self.SPACETRACK_USER
            )

            password_param = SSM.get_parameter(
                "spacetrack/password", core_config.APP_ENV
            )
            self.SPACETRACK_PWD = str(password_param.get("Value"))

            base_url_param = SSM.get_parameter(
                "spacetrack/base_url", core_config.APP_ENV
            )
            self.SPACETRACK_BASE_URL = str(base_url_param.get("Value"))

            logger.debug("Retrieved spacetrack credentials from SSM!")
        else:
            logger.debug("Local spacetrack credentials loaded")


spacetrack_config = Config()
