import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import across.sdk.v1 as sdk
import structlog

from across_data_ingestion.core import config

from .abstract_key_manager import KeyManager

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class ApiClientWrapper(sdk.ApiClient):
    _client = None
    _key_manager: KeyManager | None

    def __init__(
        self,
        configuration: sdk.Configuration,
        key_manager: KeyManager | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(configuration, *args, **kwargs)

        self._key_manager = key_manager
        self._lock = threading.Lock()

    @classmethod
    def get_client(cls, key_manager: KeyManager | None = None):
        if cls._client is None:
            configuration = sdk.Configuration(
                host=config.ACROSS_SERVER_URL,
                username=config.ACROSS_INGESTION_SERVICE_ACCOUNT_ID,
                password=config.ACROSS_INGESTION_SERVICE_ACCOUNT_KEY,
            )

            cls._client = ApiClientWrapper(configuration, key_manager)

        return cls._client

    def call_api(self, *args, **kwargs) -> Any:
        self.refresh()

        try:
            return super().call_api(*args, **kwargs)
        except sdk.ApiException as err:
            if err.status == 401:
                logger.warning(
                    "Access token is unauthenticated or it has expired, attempting to fetch a new one..."
                )

                self.refresh_token()

                # Attempt the call again
                return super().call_api(*args, **kwargs)
            else:
                raise err

    def refresh(self) -> None:
        if not self.configuration.access_token:
            self.refresh_token()

        with self._lock:
            if self._key_manager:
                now = datetime.now(timezone.utc)
                expiration = self._key_manager.expiration()

                # treat naive datetimes as UTC (adjust to UTC if no TZ)
                if expiration.tzinfo is None:
                    expiration = expiration.replace(tzinfo=timezone.utc)

                is_expired = expiration <= now
                will_expire_soon = expiration <= now + timedelta(days=1)

                if is_expired or will_expire_soon:
                    secret_key = self._key_manager.rotate()
                    self.configuration.password = secret_key

    def refresh_token(self) -> None:
        try:
            logger.debug("Refreshing access token")

            # Instantiate with super to avoid infinite recursion through call_api
            token = sdk.AuthApi(super()).token(
                grant_type=sdk.GrantType.CLIENT_CREDENTIALS
            )
            self.configuration.access_token = token.access_token

            logger.debug("Successfully refreshed token")
        except sdk.ApiException as err:
            logger.error("Failed to authenticate", err=err)
            raise err
        except Exception as err:
            raise err


client = ApiClientWrapper.get_client()
