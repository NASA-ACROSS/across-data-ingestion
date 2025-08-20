from collections.abc import Generator
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.xmm_newton.low_fidelity_planned import (
    entrypoint,
    extract_om_exposures_from_timeline_data,
    ingest,
    read_planned_schedule_table,
    read_revolution_timeline_file,
)

from .mocks.low_fidelity_planned_mock_schedule_output import xmm_newton_planned_schedule


class TestXMMNewtonTLowFidelityPlannedScheduleIngestionTask:
    @pytest.fixture(autouse=True)
    def setup(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_schedule_post: Mock,
    ) -> Generator:
        """Setup patched methods used in all tests"""
        with patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post",
            mock_schedule_post,
        ):
            yield

    def test_should_generate_across_schedule(
        self,
        mock_schedule_post: Mock,
    ) -> None:
        """Should generate ACROSS schedules"""
        ingest()
        mock_schedule_post.assert_called_once_with(xmm_newton_planned_schedule)

    def test_ingest_should_return_if_cannot_planned_schedule(
        self,
        mock_read_planned_schedule_table: MagicMock,
        mock_schedule_post: Mock,
    ) -> None:
        """Should return if cannot read planned schedule table"""
        mock_read_planned_schedule_table.return_value = pd.DataFrame([])
        ingest()
        mock_schedule_post.assert_not_called()

    def test_should_log_when_ingestion_is_successful(
        self,
        mock_log: Mock,
    ) -> None:
        """Should log info when ingestion is successful"""
        entrypoint()  # type: ignore
        assert "ran successfully" in mock_log.info.call_args.args[0]

    def test_should_log_error_ingestion_fails(
        self,
        mock_log: Mock,
        mock_schedule_post: Mock,
    ) -> None:
        """Should log error when ingestion encounters unexpected error"""
        mock_schedule_post.side_effect = Exception()
        entrypoint()  # type: ignore
        assert "encountered an unexpected error" in mock_log.error.call_args.args[0]

    def test_should_read_planned_schedule_table_as_dataframe(
        self,
        mock_planned_schedule_table: pd.DataFrame,
    ) -> None:
        """Should read the planned schedule table as a DataFrame"""
        with patch("pandas.read_html", return_value=[mock_planned_schedule_table]):
            schedule_df = read_planned_schedule_table()
            assert isinstance(schedule_df, pd.DataFrame)

    def test_read_planned_schedule_table_should_return_empty_dataframe_if_table_empty(
        self,
    ) -> None:
        """Should return an empty DataFrame if the table is empty"""
        with patch("pandas.read_html", return_value=[]):
            schedule_df = read_planned_schedule_table()
            pd.testing.assert_frame_equal(schedule_df, pd.DataFrame([]))

    def test_should_read_revolution_timeline_file_as_dataframe(
        self,
        mock_revolution_timeline_file: pd.DataFrame,
    ) -> None:
        """Should read the revolution timeline file as a DataFrame"""
        with patch(
            "pandas.read_html",
            return_value=[pd.DataFrame([]), mock_revolution_timeline_file],
        ):
            exposure_df = read_revolution_timeline_file(123456)
            assert isinstance(exposure_df, pd.DataFrame)

    def test_read_revolution_file_should_return_empty_dataframe_if_table_empty(
        self,
    ) -> None:
        """Should return an empty DataFrame if the revolution timeline file is empty"""
        with patch("pandas.read_html", return_value=[]):
            exposure_df = read_revolution_timeline_file(123456)
            pd.testing.assert_frame_equal(exposure_df, pd.DataFrame([]))

    def test_extract_om_exposures_should_return_dict_of_exposures(
        self, mock_revolution_timeline_file: pd.DataFrame
    ) -> None:
        """Should extract OM exposures from timeline file and return them as a dictionary"""
        exposures = extract_om_exposures_from_timeline_data(
            mock_revolution_timeline_file
        )
        assert type(exposures) is dict
        assert len(exposures) > 0
