from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.xmm_newton.low_fidelity_planned import (
    extract_om_exposures_from_observation_data,
    ingest,
    read_planned_schedule_table,
)
from across_data_ingestion.util.across_server import sdk

from .mocks.fake_scheduled_observation_data import fake_scheduled_observation_data
from .mocks.low_fidelity_planned_mock_schedule_output import xmm_newton_planned_schedule


class TestXMMNewtonLowFidelityPlannedScheduleIngestionTask:
    @pytest.fixture(autouse=True)
    def setup(self, fake_httpx_response: MagicMock) -> None:
        fake_httpx_response.text = fake_scheduled_observation_data

    class TestIngest:
        def test_should_call_across_generate_schedule(
            self,
            mock_schedule_api: MagicMock,
        ) -> None:
            """Should generate ACROSS schedules"""
            ingest()
            mock_schedule_api.create_schedule.assert_called_once()

        def test_should_call_across_create_schedule_with_schedule_create_instance(
            self, mock_schedule_api: MagicMock
        ) -> None:
            """Should create ACROSS schedule with ScheduleCreate schema"""
            ingest()

            args = mock_schedule_api.create_schedule.call_args[0]

            assert isinstance(args[0], sdk.ScheduleCreate)

        def test_should_call_across_create_schedule_with_observation_create_instance(
            self, mock_schedule_api: MagicMock
        ) -> None:
            """Should create ACROSS schedule with ObservationCreate schemas"""
            ingest()

            args = mock_schedule_api.create_schedule.call_args[0]

            assert isinstance(args[0].observations[0], sdk.ObservationCreate)

        @pytest.mark.parametrize(
            "instrument_id",
            [
                "epic-pn_instrument_uuid",
                "epic-mos_instrument_uuid",
                "rgs_instrument_uuid",
                "om_instrument_uuid",
            ],
        )
        @pytest.mark.parametrize("field", sdk.ObservationCreate.model_fields)
        def test_should_create_schedule_with_expected_parameters(
            self,
            mock_schedule_api: MagicMock,
            instrument_id: str,
            field: str,
        ) -> None:
            """Should create the expected schedule"""
            ingest()

            created_schedule: sdk.ScheduleCreate = (
                mock_schedule_api.create_schedule.call_args[0][0]
            )
            created_obs = [
                obs
                for obs in created_schedule.observations
                if obs.instrument_id == instrument_id
            ][0]

            expected_schedule = sdk.ScheduleCreate.model_validate(
                xmm_newton_planned_schedule
            )
            expected_obs = [
                obs
                for obs in expected_schedule.observations
                if obs.instrument_id == instrument_id
            ][0]

            assert getattr(created_obs, field) == getattr(expected_obs, field)

        def test_should_return_if_cannot_read_planned_schedule(
            self,
            mock_read_planned_schedule_table: MagicMock,
            mock_schedule_api: MagicMock,
        ) -> None:
            """Should return if cannot read planned schedule table"""
            mock_read_planned_schedule_table.return_value = pd.DataFrame([])
            ingest()
            mock_schedule_api.create_schedule.assert_not_called()

        def test_should_log_warning_if_no_om_exposures(
            self,
            fake_httpx_response: MagicMock,
            mock_logger: MagicMock,
        ) -> None:
            """Should log a warning if no OM exposures are read from the scheduled observation data"""
            fake_httpx_response.status_code = 404
            ingest()
            assert (
                "Did not find OM exposures from scheduled observations search page"
                in mock_logger.warning.call_args[0]
            )

    class TestReadPlannedScheduleTable:
        def test_should_read_planned_schedule_table_as_dataframe(
            self,
            monkeypatch: pytest.MonkeyPatch,
            mock_planned_schedule_table: pd.DataFrame,
        ) -> None:
            """Should read the planned schedule table as a DataFrame"""
            monkeypatch.setattr(
                pd,
                "read_html",
                MagicMock(return_value=[mock_planned_schedule_table]),
            )
            schedule_df = read_planned_schedule_table()
            assert isinstance(schedule_df, pd.DataFrame)

        def test_read_planned_schedule_table_should_return_empty_dataframe_if_table_empty(
            self,
            monkeypatch: pytest.MonkeyPatch,
        ) -> None:
            """Should return an empty DataFrame if the table is empty"""
            monkeypatch.setattr(pd, "read_html", MagicMock(return_value=[]))
            schedule_df = read_planned_schedule_table()
            pd.testing.assert_frame_equal(schedule_df, pd.DataFrame([]))

    class TestExtractOMExposuresFromObservationData:
        """Test extract OM exposures from observation data"""

        def test_should_return_list_of_exposures(self) -> None:
            """Should extract OM exposures from scheduled observation data and return them as a list"""
            exposures = extract_om_exposures_from_observation_data(12345)
            assert type(exposures) is list

        def test_should_return_nonempty_list_if_request_successful(self) -> None:
            """Should extract OM exposures from scheduled observation data if successful"""
            exposures = extract_om_exposures_from_observation_data(12345)
            assert len(exposures) > 0

        def test_should_return_empty_list_if_request_not_successful(
            self, fake_httpx_response: MagicMock
        ) -> None:
            """Should return empty list if httpx request returns non-200 status code"""
            fake_httpx_response.status_code = 404
            exposures = extract_om_exposures_from_observation_data(12345)
            assert len(exposures) == 0

        def test_should_log_warning_if_request_not_successful(
            self,
            fake_httpx_response: MagicMock,
            mock_logger: MagicMock,
        ) -> None:
            """Should return empty list if httpx request returns non-200 status code"""
            fake_httpx_response.status_code = 404
            extract_om_exposures_from_observation_data(12345)
            mock_logger.warning.assert_called_with(
                "Scheduled observations page returned bad status code", status_code=404
            )

        def test_should_return_empty_list_if_cannot_find_tables(
            self, fake_httpx_response: MagicMock
        ) -> None:
            """Should return empty list if BeautifulSoup cannot find tables"""
            fake_httpx_response.text = ""
            exposures = extract_om_exposures_from_observation_data(12345)
            assert len(exposures) == 0

        def test_should_fix_year_if_extracted_start_date_in_past(
            self, fake_datetime: MagicMock
        ) -> None:
            """
            Should correct the scraped start_date if it is in the past.
            This is because the provided start date in the HTML does not have
            a year, and we must specify one, avoiding edge cases if the ingestion
            runs in December but the start date is in January.
            Here we specify a "now" that is after the start date of the scheduled obs,
            to test that it is fixed to a future start date.
            """
            fake_datetime_now = datetime(2026, 12, 31)
            fake_datetime.return_value = fake_datetime_now
            exposures = extract_om_exposures_from_observation_data(12345)
            assert all(
                [
                    datetime.strptime(exposure["start_time"], "%Y-%m-%d %H:%M:%S")
                    > fake_datetime_now
                    for exposure in exposures
                ]
            )
