import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from across_data_ingestion.util.across_server import sdk
from across_data_ingestion.util.across_server.api_client_wrapper import ApiClientWrapper


class TestGetClient:
    def test_should_return_a_wrapped_client_instance(self):
        client = ApiClientWrapper.get_client()

        assert isinstance(client, ApiClientWrapper)

    def test_should_return_an_existing_client_instance(
        self, mock_api_wrapper_cls: MagicMock
    ):
        ApiClientWrapper.get_client()
        second = ApiClientWrapper.get_client()

        assert second == mock_api_wrapper_cls.return_value

    def test_should_return_a_new_instance_when_it_dne(
        self, mock_api_wrapper_cls: MagicMock
    ):
        # Call get_client
        client = ApiClientWrapper.get_client()

        # Assert returned value is the mocked instance
        assert client == mock_api_wrapper_cls.return_value


class TestRefresh:
    def test_should_only_rotate_the_key_once_when_using_thread_lock(
        self,
        test_api_client: ApiClientWrapper,
        mock_service_account_rotate_key: MagicMock,
    ):
        test_api_client._exp = datetime.now(timezone.utc)
        # Test setup for faking multiple calls to "refresh"
        num_threads = 5
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            test_api_client.refresh()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(executor.map(lambda _: worker(), range(num_threads)))

        assert mock_service_account_rotate_key.call_count == 1

    def test_should_refresh_the_token_when_it_does_not_exist(
        self, test_api_client: ApiClientWrapper, mock_token: MagicMock
    ):
        # reset the token
        test_api_client.configuration.access_token = ""

        dummy_token = "new-token"
        mock_token.return_value.access_token = dummy_token

        test_api_client.refresh()

        assert test_api_client.configuration.access_token == dummy_token

    @pytest.mark.parametrize(
        "expiration",
        [
            # expired
            (datetime.now(timezone.utc) - timedelta(days=1)),
            # expires soon/today
            (datetime.now(timezone.utc)),
        ],
    )
    def test_should_rotate_based_on_expiration(
        self,
        expiration: datetime,
        test_api_client: ApiClientWrapper,
        mock_service_account_rotate_key: MagicMock,
    ):
        test_api_client._exp = expiration
        test_api_client.refresh()

        mock_service_account_rotate_key.assert_called_once()


class TestRefreshToken:
    def test_should_refresh_token_for_client_creds(
        self, test_api_client: ApiClientWrapper, mock_token: MagicMock
    ):
        test_api_client.refresh_token()

        mock_token.assert_called_once_with(grant_type=sdk.GrantType.CLIENT_CREDENTIALS)

    def test_should_instantiate_auth_api_with_super(
        self, test_api_client: ApiClientWrapper, mock_auth_api_cls: MagicMock
    ):
        test_api_client.refresh_token()

        [instantiated_client] = mock_auth_api_cls.call_args[0]

        # Should be the base class by super proxy
        assert type(instantiated_client) is super

    def test_refresh_token_raises_api_exception(
        self, test_api_client: ApiClientWrapper, mock_token: MagicMock
    ):
        mock_token.side_effect = sdk.ApiException("bad token")

        with pytest.raises(sdk.ApiException) as exc:
            test_api_client.refresh_token()

        assert "bad token" in str(exc.value)

    def test_refresh_token_raises_generic_exception(
        self, test_api_client: ApiClientWrapper, mock_token: MagicMock
    ):
        mock_token.side_effect = Exception("boom")

        with pytest.raises(Exception) as exc:
            test_api_client.refresh_token()

        assert "boom" in str(exc.value)

    def test_refresh_token_logs_error_on_api_exception(
        self,
        test_api_client: ApiClientWrapper,
        mock_token: MagicMock,
        mock_logger: MagicMock,
    ):
        mock_token.side_effect = sdk.ApiException("bad token")

        with pytest.raises(sdk.ApiException):
            test_api_client.refresh_token()

        mock_logger.error.assert_called_once()


class TestCallApi:
    def test_should_delegate_call_api_to_super(
        self, test_api_client: ApiClientWrapper, mock_super_call: MagicMock
    ):
        test_api_client._exp = (datetime.now() + timedelta(days=30)).replace(
            tzinfo=timezone.utc
        )
        test_api_client.call_api()

        mock_super_call.assert_called_once()

    def test_should_always_try_to_refresh(
        self, monkeypatch: pytest.MonkeyPatch, test_api_client: ApiClientWrapper
    ):
        # override the refresh method since we just check to see it was called.
        mock_refresh = MagicMock()
        monkeypatch.setattr(ApiClientWrapper, "refresh", mock_refresh)
        test_api_client.call_api()

        mock_refresh.assert_called_once()

    def test_should_log_warning_when_unauthorized_401_api_exception(
        self,
        test_api_client: ApiClientWrapper,
        mock_super_call: MagicMock,
        mock_logger: MagicMock,
    ):
        mock_super_call.side_effect = sdk.ApiException(status=401)

        with pytest.raises(sdk.ApiException):
            test_api_client.call_api()

        mock_logger.warning.assert_called_once()

    def test_should_retry_when_unauthorized_401_api_exception(
        self, test_api_client: ApiClientWrapper, mock_super_call: MagicMock
    ):
        mock_super_call.side_effect = sdk.ApiException(status=401)

        with pytest.raises(sdk.ApiException):
            test_api_client.call_api()

        assert mock_super_call.call_count == 2

    def test_should_refresh_token_when_unauthorized_401(
        self,
        monkeypatch: pytest.MonkeyPatch,
        test_api_client: ApiClientWrapper,
        mock_super_call: MagicMock,
    ):
        # override the refresh_token method since we just check to see it was called.
        mock_refresh_token = MagicMock()
        monkeypatch.setattr(ApiClientWrapper, "refresh_token", mock_refresh_token)
        mock_super_call.side_effect = sdk.ApiException(status=401)

        with pytest.raises(sdk.ApiException):
            test_api_client.call_api()

        mock_refresh_token.assert_called_once()

    def test_should_raise_exception_when_call_fails(
        self, test_api_client: ApiClientWrapper, mock_super_call: MagicMock
    ):
        mock_super_call.side_effect = Exception("oh no")

        with pytest.raises(Exception) as exc:
            test_api_client.call_api()

        assert "oh no" in str(exc.value)
