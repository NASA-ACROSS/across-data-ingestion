import threading
from typing import Literal

from across.sdk.v1.abstract_credential_storage import CredentialStorage

from across_data_ingestion.core import config

from ..ssm import SSM


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
        with self._lock:
            # reset cache
            self._secret = key

            SSM.put_parameter(
                name=config.ACROSS_SERVER_SECRET_PATH,
                value=key,
                overwrite=True,
            )

    def _get_param(
        self, cred: Literal["id", "secret"], force: bool | None = False
    ) -> str:
        # check for cached value
        prop = f"_{cred}"
        value = getattr(self, prop)

        if value and force is False:
            return value

        if cred == "id":
            param_name = config.ACROSS_SERVER_ID_PATH
        elif cred == "secret":
            param_name = config.ACROSS_SERVER_SECRET_PATH

        param = SSM.get_parameter(param_name, config.APP_ENV)

        value = param.get("Value")

        if value is None:
            raise ValueError(
                f"No value found in the SSM Param store for the client {cred}. "
                + f"Please check the parameter store to ensure the value exists for '{param_name}'."
            )

        # set new value
        setattr(self, prop, value)

        return value
