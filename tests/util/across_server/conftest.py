from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import across_data_ingestion.util.across_server.api_client_wrapper as wrapper
from across_data_ingestion.util.across_server import sdk
from across_data_ingestion.util.across_server.abstract_key_manager import KeyManager
from across_data_ingestion.util.across_server.api_client_wrapper import (
    ApiClientWrapper,
)


@pytest.fixture
def mock_expiration() -> MagicMock:
    mock = MagicMock()
    mock.return_value = datetime.now(timezone.utc) + timedelta(days=30)
    return mock


@pytest.fixture
def mock_token() -> MagicMock:
    mock = MagicMock(return_value=sdk.AccessTokenResponse(access_token="token"))
    return mock


@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock]:
    mock = MagicMock()
    monkeypatch.setattr(wrapper, "logger", mock)
    return mock


@pytest.fixture
def mock_key_manager(mock_expiration: MagicMock) -> MagicMock:
    _rotation_count = {"count": 0}
    # Create a MagicMock with the same interface as KeyManager
    mock = MagicMock(spec=KeyManager)

    # Force expiration to be already expired so refresh() will rotate
    mock.expiration.return_value = datetime.now(timezone.utc) - timedelta(seconds=1)

    def rotate_side_effect():
        _rotation_count["count"] += 1
        # Set expiration far in the future so subsequent calls skip rotation
        mock.expiration = mock_expiration
        return "new_secret"

    mock.rotate.side_effect = rotate_side_effect
    mock._rotation_count = _rotation_count
    return mock


@pytest.fixture
def mock_auth_api_cls(mock_token: MagicMock) -> MagicMock:
    mock_instance = MagicMock()
    mock_instance.token = mock_token

    mock_api_cls = MagicMock(return_value=mock_instance)
    return mock_api_cls


@pytest.fixture(autouse=True)
def set_sdk_mocks(
    monkeypatch: pytest.MonkeyPatch, mock_auth_api_cls: MagicMock
) -> None:
    monkeypatch.setattr(sdk, "AuthApi", mock_auth_api_cls)


@pytest.fixture(autouse=True)
def mock_super_call(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock_super_call_api = MagicMock(return_value="OK")
    monkeypatch.setattr(sdk.ApiClient, "call_api", mock_super_call_api)
    return mock_super_call_api


@pytest.fixture(autouse=True, scope="function")
def test_api_client(mock_key_manager: MagicMock) -> ApiClientWrapper:
    config = sdk.Configuration(access_token="token", password="secret")
    client = ApiClientWrapper(configuration=config, key_manager=mock_key_manager)

    return client


@pytest.fixture
def mock_wrapper():
    return MagicMock(id="fake-wrapper-instance")


@pytest.fixture
def mock_wrapper_cls(monkeypatch: pytest.MonkeyPatch, mock_wrapper: MagicMock):
    # Reset singleton
    ApiClientWrapper._client = None
    mock_cls = MagicMock(return_value=mock_wrapper)
    monkeypatch.setattr(wrapper, "ApiClientWrapper", mock_cls)

    return mock_cls
