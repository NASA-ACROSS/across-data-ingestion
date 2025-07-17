from unittest.mock import AsyncMock, Mock, patch

import pytest
from astropy.table import Table  # type: ignore[import-untyped]

from across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned import (
    entrypoint,
    get_instrument_info_from_observation,
    ingest,
)

from .mocks.chandra_high_fidelity_planned_mock_schedule_output import (
    chandra_planned_schedule,
)


class TestChandraHighFidelityPlannedScheduleIngestionTask:
    @pytest.mark.asyncio
    async def test_should_generate_across_schedule(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
    ):
        mock_post = Mock(return_value=None)
        """Should generate ACROSS schedules"""
        with patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post",
            mock_post,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ):
            await ingest()
            mock_post.assert_called_once_with(chandra_planned_schedule)

    @pytest.mark.asyncio
    async def test_should_log_when_query_returns_no_response(
        self, mock_telescope_get: list[dict[str, str]]
    ):
        """Should log when querying VO service returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            AsyncMock(return_value=None),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await ingest()
            assert "No observations" in log_mock.info.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_warning_when_exposure_query_returns_no_response(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service_for_exposure_times: AsyncMock,
    ):
        """Should log a warning when querying VO service for exposure times returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service_for_exposure_times,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await ingest()
            assert "No exposure time" in log_mock.warn.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_error_when_unable_to_parse_instrument(
        mock_observation_table: Table,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
    ):
        """Should log error when unable to parse instrument from observation"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.get_instrument_info_from_observation",
            return_value=("", ""),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await ingest()
            assert "Cannot parse observations" in log_mock.error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_info_when_success(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
    ):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await entrypoint()
            assert "ingestion completed" in log_mock.info.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_error_when_schedule_ingestion_fails(
        self,
        mock_telescope_get: list[dict[str, str]],
    ):
        """Should log an error when schedule ingestion fails"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.ingest",
            return_value=None,
            side_effect=Exception(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await entrypoint()
            assert "encountered an error" in log_mock.error.call_args.args[0]

    @pytest.mark.parametrize(
        "mock_tap_row, expected_instrument_info",
        [
            (
                {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "NONE"},
                (
                    "ACIS",
                    "acis-mock-id",
                ),
            ),
            (
                {"instrument": "ACIS", "grating": "HETG", "exposure_mode": "NONE"},
                (
                    "ACIS-HETG",
                    "acis-hetg-mock-id",
                ),
            ),
            (
                {"instrument": "ACIS", "grating": "LETG", "exposure_mode": "NONE"},
                (
                    "ACIS-LETG",
                    "acis-letg-mock-id",
                ),
            ),
            (
                {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "CC"},
                (
                    "ACIS-CC",
                    "acis-cc-mock-id",
                ),
            ),
            (
                {"instrument": "HRC", "grating": "NONE", "exposure_mode": ""},
                (
                    "HRC",
                    "hrc-mock-id",
                ),
            ),
            (
                {"instrument": "HRC", "grating": "HETG", "exposure_mode": ""},
                (
                    "HRC-HETG",
                    "hrc-hetg-mock-id",
                ),
            ),
            (
                {"instrument": "HRC", "grating": "LETG", "exposure_mode": ""},
                (
                    "HRC-LETG",
                    "hrc-letg-mock-id",
                ),
            ),
            (
                {"instrument": "HRC", "grating": "NONE", "exposure_mode": "TIMING"},
                (
                    "HRC-Timing",
                    "hrc-timing-mock-id",
                ),
            ),
            (
                {"instrument": "ACIS", "grating": "BADGRATING", "exposure_mode": ""},
                (
                    "",
                    "",
                ),
            ),
            (
                {"instrument": "HRC", "grating": "BADGRATING", "exposure_mode": ""},
                (
                    "",
                    "",
                ),
            ),
            (
                {"instrument": "BADINSTRUMENT", "grating": "", "exposure_mode": ""},
                (
                    "",
                    "",
                ),
            ),
        ],
    )
    def test_get_instrument_info_from_observation(
        self,
        mock_tap_row: dict,
        expected_instrument_info: tuple,
        mock_instrument_info: list,
    ) -> None:
        """Should return correct instrument name and id from observation row"""
        assert (
            get_instrument_info_from_observation(mock_instrument_info, mock_tap_row)
            == expected_instrument_info
        )
