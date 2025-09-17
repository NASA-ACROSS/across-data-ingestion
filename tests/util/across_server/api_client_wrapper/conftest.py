from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest
from across.sdk.v1 import rest

import across_data_ingestion.util.across_server.api_client_wrapper as wrapper
from across_data_ingestion.util.across_server import sdk
from across_data_ingestion.util.across_server.abstract_credential_storage import (
    CredentialStorage,
)
from across_data_ingestion.util.across_server.api_client_wrapper import (
    ApiClientWrapper,
)


@pytest.fixture
def mock_expiration() -> MagicMock:
    mock = MagicMock()
    mock.return_value = datetime.now(timezone.utc) + timedelta(days=30)
    return mock


@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock]:
    mock = MagicMock()
    monkeypatch.setattr(wrapper, "logger", mock)
    return mock


@pytest.fixture
def mock_creds() -> MagicMock:
    # Create a MagicMock with the same interface as CredsStorage
    mock = MagicMock(spec=CredentialStorage)
    mock.id = MagicMock(return_value="service-account-id")
    mock.secret = MagicMock(return_value="service-account-secret")
    return mock


@pytest.fixture
def mock_token() -> MagicMock:
    mock = MagicMock(return_value=sdk.AccessTokenResponse(access_token="token"))
    return mock


@pytest.fixture
def mock_auth_api_cls(mock_token: MagicMock) -> MagicMock:
    mock_instance = MagicMock()
    mock_instance.token = mock_token

    mock_api_cls = MagicMock(return_value=mock_instance)
    return mock_api_cls


@pytest.fixture
def mock_get_service_account(
    fake_service_account: sdk.SystemServiceAccount,
) -> MagicMock:
    mock = MagicMock(return_value=fake_service_account)
    return mock


@pytest.fixture
def mock_service_account_rotate_key(
    fake_service_account_secret: sdk.SystemServiceAccountSecret,
) -> MagicMock:
    mock = MagicMock(return_value=fake_service_account_secret)
    return mock


@pytest.fixture
def mock_internal_api_cls(
    mock_get_service_account: MagicMock, mock_service_account_rotate_key: MagicMock
) -> MagicMock:
    mock_instance = MagicMock()
    mock_instance.get_service_account = mock_get_service_account
    mock_instance.service_account_rotate_key = mock_service_account_rotate_key

    mock_api_cls = MagicMock(return_value=mock_instance)
    return mock_api_cls


@pytest.fixture(autouse=True)
def set_sdk_mocks(
    monkeypatch: pytest.MonkeyPatch,
    mock_auth_api_cls: MagicMock,
    mock_internal_api_cls: MagicMock,
) -> None:
    monkeypatch.setattr(sdk, "AuthApi", mock_auth_api_cls)
    monkeypatch.setattr(sdk, "InternalApi", mock_internal_api_cls)


@pytest.fixture(autouse=True)
def mock_super_call(
    monkeypatch: pytest.MonkeyPatch, fake_call_res: rest.RESTResponse
) -> MagicMock:
    mock_super_call_api = MagicMock(return_value=fake_call_res)
    monkeypatch.setattr(sdk.ApiClient, "call_api", mock_super_call_api)
    return mock_super_call_api


@pytest.fixture(autouse=True, scope="function")
def test_api_client(mock_creds: MagicMock) -> ApiClientWrapper:
    config = sdk.Configuration(access_token="token", password="secret")
    client = ApiClientWrapper(configuration=config, creds_store=mock_creds)
    # force setting of expiration date to be in the future
    client._exp = datetime.now(timezone.utc) + timedelta(days=30)

    return client


@pytest.fixture
def mock_api_wrapper_instance():
    return MagicMock(id="fake-wrapper-instance")


@pytest.fixture
def mock_api_wrapper_cls(
    monkeypatch: pytest.MonkeyPatch, mock_api_wrapper_instance: MagicMock
):
    # Reset singleton
    ApiClientWrapper._client = None
    mock_cls = MagicMock(return_value=mock_api_wrapper_instance)
    monkeypatch.setattr(wrapper, "ApiClientWrapper", mock_cls)

    return mock_cls


@pytest.fixture
def fake_call_res() -> rest.RESTResponse:
    data = {
        "secret": "krabby-secret-formula",
        "expiration": (datetime.now() + timedelta(days=30)).isoformat(),
    }
    return cast(rest.RESTResponse, SimpleNamespace(status=200, data=data, reason=""))


@pytest.fixture
def fake_service_account() -> sdk.SystemServiceAccount:
    return sdk.SystemServiceAccount(
        id="fake-service-account-id",
        name="data ingestion service account",
        expiration=datetime.now() + timedelta(days=30),
        expiration_duration=30,
        roles=[],
        group_roles=[],
    )


@pytest.fixture
def fake_service_account_secret(
    fake_service_account: sdk.SystemServiceAccount,
) -> sdk.SystemServiceAccountSecret:
    return sdk.SystemServiceAccountSecret(
        **fake_service_account.model_dump(),
        secret_key="ravioli-ravioli-give-me-the-formuoli",
    )
