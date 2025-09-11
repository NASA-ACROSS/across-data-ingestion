from unittest.mock import MagicMock

import bs4
import httpx
import pandas as pd
import pytest

import across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned as task
from across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned import (
    ingest,
    query_ixpe_schedule,
)
from across_data_ingestion.util import across_server


class TestNicerLowFidelityScheduleIngestionTask:
    class TestIngest:
        @pytest.fixture(autouse=True)
        def patch_query_ixpe_schedule(
            self, monkeypatch: pytest.MonkeyPatch, fake_ixpe_schedule_df: pd.DataFrame
        ):
            mock = MagicMock(return_value=fake_ixpe_schedule_df)
            monkeypatch.setattr(
                task,
                "query_ixpe_schedule",
                mock,
            )

            return mock

        def test_should_create_across_schedules(self, mock_schedule_api: MagicMock):
            """Should create ACROSS schedules"""
            ingest()
            mock_schedule_api.create_schedule.assert_called()

        def test_should_call_create_schedule_with_schedule_create_instance(
            self, mock_schedule_api: MagicMock
        ):
            """Should create ACROSS schedules with ScheduleCreate"""
            ingest()
            args = mock_schedule_api.create_schedule.call_args[0]
            assert isinstance(args[0], across_server.sdk.ScheduleCreate)

        def test_should_call_get_telescopes(self, mock_telescope_api: MagicMock):
            """Should create ACROSS schedules"""
            ingest()
            mock_telescope_api.get_telescopes.assert_called()

        def test_should_log_error_when_query_ixpe_catalog_returns_none(
            self,
            mock_logger: MagicMock,
            patch_query_ixpe_schedule: MagicMock,
        ):
            """Should log an error when ixpe query returns None"""
            patch_query_ixpe_schedule.return_value = pd.DataFrame()
            ingest()
            assert (
                "Failed to read IXPE timeline file"
                in mock_logger.warning.call_args.args[0]
            )

    class TestQueryIxpeSchedule:
        def test_should_return_dataframe_when_successful(self):
            """Should return a DataFrame if querying IXPE catalog is successful"""
            data = query_ixpe_schedule()
            assert isinstance(data, pd.DataFrame)

        def test_should_return_empty_df_when_query_fails(
            self, mock_httpx_get: MagicMock
        ):
            """Should return empty df when IXPE catalog fails"""
            fake_res = httpx.Response(
                status_code=200, request=httpx.Request("GET", "https://fake.com")
            )
            mock_httpx_get.return_value = fake_res
            data = query_ixpe_schedule()
            assert not len(data)

        def test_should_raise_error_when_status_is_failure(
            self, mock_httpx_get: MagicMock
        ):
            fake_error_res = httpx.Response(
                status_code=400, request=httpx.Request("GET", "http://test.com")
            )
            mock_httpx_get.return_value = fake_error_res

            with pytest.raises(httpx.HTTPStatusError):
                query_ixpe_schedule()

        def test_should_log_error_when_parsing_fails(
            self, monkeypatch: pytest.MonkeyPatch, mock_logger: MagicMock
        ):
            mock_soup_instance = MagicMock(spec=bs4.BeautifulSoup)
            mock_soup_instance.find.side_effect = Exception("oh no")
            mock_soup_cls = MagicMock(return_value=mock_soup_instance)

            monkeypatch.setattr(bs4, "BeautifulSoup", mock_soup_cls)

            query_ixpe_schedule()

            mock_logger.error.assert_called_once()
