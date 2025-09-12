from unittest.mock import MagicMock

import pandas as pd
import pytest

import across_data_ingestion.tasks.schedules.nustar.low_fidelity_planned as task
from across_data_ingestion.tasks.schedules.nustar.low_fidelity_planned import (
    ingest,
    read_planned_schedule_table,
)
from across_data_ingestion.util.across_server import sdk


class TestNustarAsFlownScheduleIngestionTask:
    class TestIngest:
        @pytest.fixture(autouse=True)
        def patch_read_planned_schedule_table(
            self,
            monkeypatch: pytest.MonkeyPatch,
            fake_planned_schedule_dataframe: pd.DataFrame,
        ) -> MagicMock:
            mock_read_planned_schedule_table = MagicMock(
                return_value=fake_planned_schedule_dataframe
            )
            monkeypatch.setattr(
                task, "read_planned_schedule_table", mock_read_planned_schedule_table
            )

            return mock_read_planned_schedule_table

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
            self,
            mock_planned_logger: MagicMock,
            patch_read_planned_schedule_table: MagicMock,
        ):
            """Should log warning when reading planned schedule table returns an empty dataframe"""
            patch_read_planned_schedule_table.return_value = pd.DataFrame([])
            ingest()
            assert (
                "No planned schedule data found."
                in mock_planned_logger.info.call_args[0]
            )

    class TestReadPlannedScheduleTable:
        def test_should_return_pandas_dataframe_when_successful(self):
            """Should return a pandas DataFrame when successfully reading planned schedule"""

            data = read_planned_schedule_table()
            assert isinstance(data, pd.DataFrame)

        def test_should_log_warning_if_reading_schedule_raises_error(
            self, mock_pandas_read_html: MagicMock, mock_planned_logger: MagicMock
        ):
            """Should log a warning if the planned schedule table raises an error"""
            mock_pandas_read_html.side_effect = ValueError()
            read_planned_schedule_table()
            assert (
                "Could not find planned schedule table"
                in mock_planned_logger.warning.call_args.args[0]
            )

        def test_should_raise_when_unexpected_error_raised(
            self, mock_pandas_read_html: MagicMock
        ):
            """Should log error when reading planned schedule table raises an unexpected error"""
            mock_pandas_read_html.side_effect = Exception("boom")
            with pytest.raises(Exception):
                read_planned_schedule_table()

        def test_should_return_empty_dataframe_if_query_fails_from_value_error(
            self, mock_pandas_read_html: MagicMock
        ):
            """Should return empty dataframe if reading planned schedule table fails"""
            mock_pandas_read_html.side_effect = ValueError()
            data = read_planned_schedule_table()
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 0

        def test_should_log_warning_if_schedule_not_found(
            self, mock_pandas_read_html: MagicMock, mock_planned_logger: MagicMock
        ):
            """Should log an error if the planned schedule table is not found"""
            mock_pandas_read_html.return_value = []
            read_planned_schedule_table()
            assert (
                "Could not read planned schedule table"
                in mock_planned_logger.warning.call_args.args[0]
            )

        def test_should_return_empty_dataframe_if_query_returns_empty_list(
            self, mock_pandas_read_html: MagicMock
        ):
            """Should return empty dataframe if pandas read html returns empty list"""
            mock_pandas_read_html.return_value = []
            data = read_planned_schedule_table()
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 0
