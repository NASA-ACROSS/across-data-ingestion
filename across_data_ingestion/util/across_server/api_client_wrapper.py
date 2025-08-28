import threading
from datetime import datetime, timedelta, timezone

import across.sdk.v1 as sdk
import structlog
from across.sdk.v1 import rest

from ...core import config
from .abstract_credential_storage import CredentialStorage as ICredStorage
from .ssm_credentials import SSMCredentials

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class ApiClientWrapper(sdk.ApiClient):
    _client = None
    _cred_store: ICredStorage | None = None
    _exp: datetime | None = None

    def __init__(
        self,
        configuration: sdk.Configuration,
        creds_store: ICredStorage | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(configuration, *args, **kwargs)

        self._cred_store = creds_store
        self._lock = threading.Lock()

    @classmethod
    def get_client(cls, creds: ICredStorage | None = None):
        if cls._client is None:
            # if local or no cred manager, expect env vars
            # else if creds manager, use it
            if creds:
                client_id = creds.id(force=True)
                client_secret = creds.secret(force=True)
            else:
                client_id = config.ACROSS_SERVER_ID
                client_secret = config.ACROSS_SERVER_SECRET

            configuration = sdk.Configuration(
                host=config.ACROSS_SERVER_URL,
                username=client_id,
                password=client_secret,
            )

            cls._client = ApiClientWrapper(configuration, creds)

        return cls._client

    def call_api(self, *args, **kwargs) -> rest.RESTResponse:
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

        if self._cred_store:
            with self._lock:
                if self._should_rotate:
                    res = sdk.InternalApi(super()).service_account_rotate_key(
                        service_account_id=self._cred_store.id()
                    )

                    self._set_exp(res.expiration)
                    self._cred_store.update_key(res.secret_key)
                    self.configuration.password = res.secret_key

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

    @property
    def _should_rotate(self) -> bool:
        now = datetime.now(timezone.utc)

        if self._expiration:
            is_expired = self._expiration <= now
            will_expire_soon = self._expiration <= now + timedelta(days=1)

            return is_expired or will_expire_soon

        return True

    @property
    def _expiration(self) -> datetime | None:
        if self._cred_store:
            if self._exp:
                return self._exp

            id = self._cred_store.id()
            res = sdk.InternalApi(super()).get_service_account(service_account_id=id)

            self._set_exp(res.expiration)

            return res.expiration
        return None

    def _set_exp(self, date: datetime):
        if date.tzinfo is None:
            dt = date.replace(tzinfo=timezone.utc)

        self._exp = dt


client = ApiClientWrapper.get_client(
    SSMCredentials() if not config.is_local() else None
)
