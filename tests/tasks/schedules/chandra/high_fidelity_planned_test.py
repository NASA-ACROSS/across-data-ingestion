from unittest.mock import AsyncMock, patch

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
        """Should generate ACROSS schedules"""
        with patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post",
            return_value=None,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ):
            schedules = await ingest()
            assert schedules == [chandra_planned_schedule]

    @pytest.mark.asyncio
    async def test_should_generate_observations_with_schedule(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
    ):
        """Should generate list of observations with an ACROSS schedule"""
        with patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post",
            return_value=None,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ):
            schedules = await ingest()
            assert len(schedules[0]["observations"]) > 0

    @pytest.mark.asyncio
    async def test_should_log_warning_when_query_returns_no_response(
        self, mock_telescope_get: list[dict[str, str]]
    ):
        """Should log a warning when querying VO service returns None"""
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
            assert "No response returned" in log_mock.warn.call_args.args[0]

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
        "row, expected_name, expected_id",
        [
            (
                "mock_acis_observation_row",
                "ACIS",
                "acis-mock-id",
            ),
            (
                "mock_acis_hetg_observation_row",
                "ACIS-HETG",
                "acis-hetg-mock-id",
            ),
            (
                "mock_acis_letg_observation_row",
                "ACIS-LETG",
                "acis-letg-mock-id",
            ),
            (
                "mock_acis_cc_observation_row",
                "ACIS-CC",
                "acis-cc-mock-id",
            ),
            (
                "mock_hrc_observation_row",
                "HRC",
                "hrc-mock-id",
            ),
            (
                "mock_hrc_hetg_observation_row",
                "HRC-HETG",
                "hrc-hetg-mock-id",
            ),
            (
                "mock_hrc_letg_observation_row",
                "HRC-LETG",
                "hrc-letg-mock-id",
            ),
            (
                "mock_hrc_timing_observation_row",
                "HRC-Timing",
                "hrc-timing-mock-id",
            ),
            (
                "mock_bad_acis_observation_row",
                "",
                "",
            ),
            (
                "mock_bad_hrc_observation_row",
                "",
                "",
            ),
            (
                "mock_bad_instrument_observation_row",
                "",
                "",
            ),
        ],
    )
    def test_get_instrument_info_from_observation(
        self,
        row: str,
        expected_name: str,
        expected_id: str,
        request: pytest.FixtureRequest,
        mock_instrument_info: dict,
    ) -> None:
        """Should return correct instrument name and id from observation row"""
        assert get_instrument_info_from_observation(
            mock_instrument_info, request.getfixturevalue(row)
        ) == (expected_name, expected_id)
