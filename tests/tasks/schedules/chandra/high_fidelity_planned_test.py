from unittest.mock import AsyncMock, MagicMock

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
        mock_telescope_get: AsyncMock,
        mock_schedule_post: AsyncMock,
        mock_query_vo_service: AsyncMock,
    ):
        """Should generate ACROSS schedules"""
        await ingest()
        mock_schedule_post.assert_called_once_with(chandra_planned_schedule)

    @pytest.mark.asyncio
    async def test_should_log_when_query_returns_no_response(
        self,
        mock_telescope_get: AsyncMock,
        mock_schedule_post: AsyncMock,
        mock_query_vo_service: AsyncMock,
        mock_logger: MagicMock,
    ):
        mock_query_vo_service.return_value = None
        """Should log when querying VO service returns None"""
        await ingest()
        assert "No observations" in mock_logger.info.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_warning_when_exposure_query_returns_no_response(
        self,
        mock_telescope_get: AsyncMock,
        mock_schedule_post: AsyncMock,
        mock_query_vo_service: AsyncMock,
        mock_logger: MagicMock,
        mock_observation_table: Table,
    ):
        """Should log a warning when querying VO service for exposure times returns None"""
        mock_query_vo_service.side_effect = [mock_observation_table, None]
        await ingest()
        assert "No exposure time" in mock_logger.warn.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_warning_when_unable_to_parse_instrument(
        mock_observation_table: Table,
        mock_telescope_get: AsyncMock,
        mock_schedule_post: AsyncMock,
        mock_query_vo_service: AsyncMock,
        mock_get_instrument_info_from_obs: AsyncMock,
        mock_logger: MagicMock,
    ):
        """Should log warning when unable to parse instrument from observation"""
        mock_get_instrument_info_from_obs.return_value = ("", "")
        await ingest()
        assert "Cannot parse observations" in mock_logger.warn.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_info_when_success(
        self,
        mock_telescope_get: AsyncMock,
        mock_schedule_post: AsyncMock,
        mock_query_vo_service: AsyncMock,
        mock_logger: MagicMock,
    ):
        """Should log info with ran at when success"""
        await entrypoint()
        assert "ingestion completed" in mock_logger.info.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_error_when_schedule_ingestion_fails(
        self,
        mock_logger: MagicMock,
        mock_ingest: AsyncMock,
    ):
        """Should log an error when schedule ingestion fails"""
        mock_ingest.side_effect = Exception()
        await entrypoint()
        assert "encountered an error" in mock_logger.error.call_args.args[0]

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
