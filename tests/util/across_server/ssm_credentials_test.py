from unittest.mock import MagicMock

import pytest

import across_data_ingestion.util.across_server.ssm_credentials as module
from across_data_ingestion.core import config
from across_data_ingestion.util.across_server.ssm_credentials import SSMCredentials


@pytest.fixture(autouse=True)
def fake_calls():
    return [{"Value": "id"}, {"Value": "secret"}]


@pytest.fixture(autouse=True)
def mock_ssm(monkeypatch: pytest.MonkeyPatch, fake_calls: list):
    mock = MagicMock()

    mock.get_parameter = MagicMock(
        side_effect=fake_calls,
    )
    monkeypatch.setattr(module, "SSM", mock)
    return mock


class TestCreds:
    @pytest.mark.parametrize("cred", ["id", "secret"])
    def test_should_return_id(self, cred: str):
        assert getattr(SSMCredentials(), cred)() == cred

    @pytest.mark.parametrize(
        "call_idx, path",
        [
            (0, config.ACROSS_SERVER_ID_PATH),
            (1, config.ACROSS_SERVER_SECRET_PATH),
        ],
    )
    def test_should_call_get_parameter_when_instantiating(
        self, call_idx: int, path: str, mock_ssm: MagicMock
    ):
        SSMCredentials()
        call = mock_ssm.get_parameter.call_args_list[call_idx]
        assert call.args[0] == path


class TestUpdate:
    def test_should_overwrite_existing_key(self, mock_ssm: MagicMock):
        SSMCredentials().update_key("new-key")
        _, kwargs = mock_ssm.put_parameter.call_args
        assert kwargs["overwrite"] is True

    def test_should_set_new_cached_key(self, mock_ssm: MagicMock):
        creds = SSMCredentials()
        creds.update_key("new-key")
        assert creds.secret() == "new-key"


class TestCaching:
    @pytest.mark.parametrize("cred", ["id", "secret"])
    def test_should_return_cached_value_if_set(self, cred: str, mock_ssm: MagicMock):
        mock_ssm.get_parameter.side_effect = [
            {"Value": "cache-id"},
            {"Value": "cache-secret"},
        ]
        creds = SSMCredentials()

        assert getattr(creds, cred)() == f"cache-{cred}"

    @pytest.mark.parametrize("cred", ["id", "secret"])
    def test_should_force_call_ssm(self, cred: str, mock_ssm: MagicMock, fake_calls):
        fake_calls.append({"Value": "forced-call"})

        creds = SSMCredentials()

        getattr(creds, cred)(force=True)

        # third call since it is after instantiation
        assert mock_ssm.get_parameter.call_count == 3

    @pytest.mark.parametrize("cred", ["id", "secret"])
    def test_should_return_value_from_ssm_when_forced(
        self, cred: str, fake_calls: list
    ):
        fake_calls.append({"Value": f"forced-{cred}"})

        creds = SSMCredentials()

        val = getattr(creds, cred)(force=True)

        # third call since it is after instantiation
        assert val == f"forced-{cred}"

    @pytest.mark.parametrize("cred", ["id", "secret"])
    def test_should_force_call_ssm_for_new_creds(self, cred: str, mock_ssm: MagicMock):
        creds = SSMCredentials()

        mock_ssm.get_parameter.side_effect = [
            {"Value": f"new-{cred}"},
        ]
        val = getattr(creds, cred)(force=True)

        # third call since it is after instantiation
        assert val == f"new-{cred}"
