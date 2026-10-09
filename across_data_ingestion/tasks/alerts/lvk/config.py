import structlog

from ....core.config import BaseConfig
from ....core.config import config as core_config
from ....util.ssm import SSM

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class Config(BaseConfig):
    GCN_LVK_CLIENT_ID: str = "gcn-lvk-client-id"
    GCN_LVK_CLIENT_SECRET: str = "gcn-lvk-client-secret"

    def __init__(self) -> None:
        super().__init__()
        if not core_config.is_local():
            logger.debug("Getting gcn lvk credentials from SSM...")

            client_id_param = SSM.get_parameter(
                "gcn/lvk/client_id", core_config.APP_ENV
            )
            self.GCN_LVK_CLIENT_ID = str(client_id_param.get("Value"))

            logger.debug(
                "Loaded GCN LVK client ID from SSM", client_id=self.GCN_LVK_CLIENT_ID
            )

            client_secret_param = SSM.get_parameter(
                "gcn/lvk/client_secret", core_config.APP_ENV
            )
            self.GCN_LVK_CLIENT_SECRET = str(client_secret_param.get("Value"))

            logger.debug("Retrieved gcn lvk credentials from SSM!")
        else:
            logger.debug("Local gcn lvk credentials loaded")


gcn_lvk_config = Config()
