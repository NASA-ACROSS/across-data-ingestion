import threading
from typing import Literal

import structlog
from across.sdk.v1.abstract_credential_storage import CredentialStorage

from across_data_ingestion.core import config

from ..ssm import SSM

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class SSMCredentials(CredentialStorage):
    _id: str = ""
    _secret: str = ""

    def __init__(self):
        self._lock = threading.Lock()
        self._id = self._get_param("id")
        self._secret = self._get_param("secret")

    @property
    def days_before_exp(self) -> int:
        return 1

    def id(self, force: bool = False) -> str:
        return self._get_param("id", force)

    def secret(self, force: bool = False) -> str:
        return self._get_param("secret", force)

    def update_key(self, key: str) -> None:
        logger.debug(
            "updating across server secret key in SSM Parameter Store",
            method="update_key",
        )
        logger.debug("new key", masked_key=f"{key[:4]}...{key[-4:]}")
        with self._lock:
            # reset cache
            logger.debug("resetting cached secret key", method="update_key")
            logger.debug(
                "old key", masked_key=f"{self._secret[:4]}...{self._secret[-4:]}"
            )
            self._secret = key

            logger.debug("updating SSM Parameter Store value", method="update_key")
            SSM.put_parameter(
                name=config.ACROSS_SERVER_SECRET_PATH,
                value=key,
                overwrite=True,
            )
            logger.debug("SSM Parameter Store value updated", method="update_key")

    def _get_param(
        self, cred: Literal["id", "secret"], force: bool | None = False
    ) -> str:
        """
        Docstring for _get_param

        :param cred: Specific credential to retrieve ('id' or 'secret')
        :param force: Force refresh from SSM Parameter Store, default is False
        :return: The requested credential value
        """
        # check for cached value
        prop = f"_{cred}"
        logger.debug(
            "retrieving across server credential",
            credential=cred,
            prop=prop,
            method="_get_param",
        )
        cached_value = getattr(self, prop)

        if cached_value and force is False:
            logger.debug(
                "using cached credential value",
                credential=cred,
                method="_get_param",
            )
            return cached_value

        if cred == "id":
            param_name = config.ACROSS_SERVER_ID_PATH
        elif cred == "secret":
            param_name = config.ACROSS_SERVER_SECRET_PATH

        logger.debug(
            "fetching credential from SSM Parameter Store",
            credential=cred,
            method="_get_param",
        )
        param = SSM.get_parameter(param_name, config.APP_ENV)

        value = param.get("Value")

        if value is None:
            raise ValueError(
                f"No value found in the SSM Param store for the client {cred}. "
                + f"Please check the parameter store to ensure the value exists for '{param_name}'."
            )

        logger.debug(
            "fetched credential from SSM Parameter Store",
            credential=cred,
            method="_get_param",
            value=f"{value[:4]}...{value[-4:]}",
        )

        # set new value
        setattr(self, prop, value)

        return value
