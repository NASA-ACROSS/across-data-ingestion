from unittest.mock import MagicMock

import pytest
from astropy.table import Table  # type: ignore[import-untyped]

import across_data_ingestion.tasks.schedules.nustar.as_flown as task
from across_data_ingestion.tasks.schedules.nustar.as_flown import (
    ingest,
    query_nustar_catalog,
)
from across_data_ingestion.util.across_server import sdk

FAKE_START_TIME = 123456  # Mock start time in MJD


class TestNustarAsFlownScheduleIngestionTask:
    class TestIngest:
        @pytest.fixture(autouse=True)
        def patch_query_catalog(
            self, monkeypatch: pytest.MonkeyPatch, fake_observation_table: Table
        ) -> MagicMock:
            mock_query_catalog = MagicMock(return_value=fake_observation_table)
            monkeypatch.setattr(task, "query_nustar_catalog", mock_query_catalog)

            return mock_query_catalog

        def test_should_create_across_schedules(self, mock_schedule_api: MagicMock):
            """Should create ACROSS schedules"""
            ingest()
            mock_schedule_api.create_schedule.assert_called_once()

        def test_should_create_across_schedule_with_schedule_create(
            self, mock_schedule_api: MagicMock
        ):
            """Should create ACROSS schedules with ScheduleCreate"""
            ingest()
            args = mock_schedule_api.create_schedule.call_args[0]
            assert isinstance(args[0], sdk.ScheduleCreate)

        def test_should_create_across_schedule_with_observation_create(
            self, mock_schedule_api: MagicMock
        ):
            """Should create ACROSS schedules with observations"""
            ingest()
            args = mock_schedule_api.create_schedule.call_args[0]
            assert isinstance(args[0].observations[0], sdk.ObservationCreate)

        def test_should_log_info_when_catalog_query_returns_empty_table(
            self, mock_logger: MagicMock, patch_query_catalog: MagicMock
        ):
            """Should log warning when the NUMASTER query returns an empty table"""
            patch_query_catalog.return_value = Table()
            ingest()
            assert "No new observations found." in mock_logger.info.call_args[0]

    class TestQueryNustarCatalog:
        def test_should_return_astropy_table_when_successful(self):
            """Should return an astropy Table when successfully querying NuSTAR catalog"""

            data = query_nustar_catalog(FAKE_START_TIME)
            assert isinstance(data, Table)

        def test_should_log_error_if_catalog_not_found(
            self, mock_heasarc_query_tap: MagicMock, mock_logger: MagicMock
        ):
            """Should log an error if the NUMASTER catalog is not available to query"""

            mock_heasarc_query_tap.side_effect = ValueError()
            query_nustar_catalog(FAKE_START_TIME)
            assert "Could not query" in mock_logger.warning.call_args.args[0]

        def test_should_raise_when_unexpected_error_raised(
            self, mock_heasarc_query_tap: MagicMock
        ):
            """Should log error when querying the NUMASTER catalog raises an unexpected error"""
            mock_heasarc_query_tap.side_effect = Exception("boom")
            with pytest.raises(Exception):
                query_nustar_catalog(FAKE_START_TIME)

        def test_should_return_empty_table_if_query_fails_from_value_error(
            self, mock_heasarc_query_tap: MagicMock
        ):
            """Should return empty table if querying NUMASTER catalog fails"""
            mock_heasarc_query_tap.side_effect = ValueError()
            data = query_nustar_catalog(FAKE_START_TIME)
            assert isinstance(data, Table)
