from unittest.mock import MagicMock

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.xmm_newton.low_fidelity_planned import (
    extract_om_exposures_from_timeline_data,
    ingest,
    read_planned_schedule_table,
    read_revolution_timeline_file,
)
from across_data_ingestion.util.across_server import sdk

from .mocks.low_fidelity_planned_mock_schedule_output import xmm_newton_planned_schedule


class TestXMMNewtonLowFidelityPlannedScheduleIngestionTask:
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

        def test_should_create_schedule_with_expected_parameters(
            self,
            mock_schedule_api: MagicMock,
        ) -> None:
            """Should create the expected schedule"""
            ingest()

            actual = mock_schedule_api.create_schedule.call_args[0][0]
            expected = sdk.ScheduleCreate.model_validate(xmm_newton_planned_schedule)

            assert actual == expected

        def test_should_return_if_cannot_planned_schedule(
            self,
            mock_read_planned_schedule_table: MagicMock,
            mock_schedule_api: MagicMock,
        ) -> None:
            """Should return if cannot read planned schedule table"""
            mock_read_planned_schedule_table.return_value = pd.DataFrame([])
            ingest()
            mock_schedule_api.create_schedule.assert_not_called()

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

    class TestReadRevolutionTimelineFile:
        def test_should_read_revolution_timeline_file_as_dataframe(
            self,
            monkeypatch: pytest.MonkeyPatch,
            mock_revolution_timeline_file: pd.DataFrame,
        ) -> None:
            """Should read the revolution timeline file as a DataFrame"""
            monkeypatch.setattr(
                pd,
                "read_html",
                MagicMock(
                    return_value=[pd.DataFrame([]), mock_revolution_timeline_file]
                ),
            )
            exposure_df = read_revolution_timeline_file(123456)
            assert isinstance(exposure_df, pd.DataFrame)

        def test_read_revolution_file_should_return_empty_dataframe_if_table_empty(
            self,
            monkeypatch: pytest.MonkeyPatch,
        ) -> None:
            """Should return an empty DataFrame if the revolution timeline file is empty"""
            monkeypatch.setattr(pd, "read_html", MagicMock(return_value=[]))
            exposure_df = read_revolution_timeline_file(123456)
            pd.testing.assert_frame_equal(exposure_df, pd.DataFrame([]))

    class TestExtractOMExposuresFromTimelineData:
        def test_extract_om_exposures_should_return_dict_of_exposures(
            self, mock_revolution_timeline_file: pd.DataFrame
        ) -> None:
            """Should extract OM exposures from timeline file and return them as a dictionary"""
            exposures = extract_om_exposures_from_timeline_data(
                mock_revolution_timeline_file
            )
            assert type(exposures) is dict
            assert len(exposures) > 0
