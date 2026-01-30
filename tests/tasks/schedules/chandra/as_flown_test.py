from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from across_data_ingestion.tasks.schedules.chandra.as_flown import (
    get_observation_data_from_tap,
    ingest,
)
from across_data_ingestion.util.across_server import sdk


class TestChandraAsFlownScheduleIngestionTask:
    @pytest.mark.asyncio
    async def test_should_call_get_telescopes_with_sdk(
        self,
        mock_telescope_api: AsyncMock,
        mock_vo_service_observed_cls: AsyncMock,
    ):
        """Should get telescopes from ACROSS"""
        await ingest()
        mock_telescope_api.get_telescopes.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_create_schedule_in_across(
        self,
        mock_schedule_api: AsyncMock,
        mock_vo_service_observed_cls: AsyncMock,
    ):
        """Should create ACROSS schedules"""
        await ingest()
        mock_schedule_api.create_schedule.assert_called_once()

    @pytest.mark.parametrize(
        ["arg", "expected_input"],
        [
            ("schedule_type", "as_flown"),
            ("schedule_status", sdk.ScheduleStatus.PERFORMED),
            ("schedule_fidelity", sdk.ScheduleFidelity.HIGH),
        ],
    )
    @pytest.mark.asyncio
    async def test_should_create_schedule_with_correct_schedule_params(
        self,
        mock_schedule_api: AsyncMock,
        mock_vo_service_planned_cls: AsyncMock,
        arg: str,
        expected_input: str,
    ):
        """Should create schedule with correct params"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.as_flown.chandra_util.create_schedule"
        ) as mock_create_schedule:
            await ingest()
            call = mock_create_schedule.call_args_list[0]
            assert call.kwargs[arg] == expected_input

    class TestGetObservationsFromTap:
        @pytest.mark.asyncio
        async def test_should_initialize_vo_service(
            self, mock_vo_service_observed_cls: AsyncMock
        ):
            await get_observation_data_from_tap()

            mock_vo_service_observed_cls.assert_called_once()

        @pytest.mark.asyncio
        @pytest.mark.parametrize(
            ["expected_table", "call_idx"],
            [("cxc.observation", 0)],
        )
        async def test_should_query_tap_for_observations(
            self,
            mock_vo_service_observed_query: AsyncMock,
            expected_table: str,
            call_idx: int,
        ):
            await get_observation_data_from_tap()

            obs_call = mock_vo_service_observed_query.call_args_list[call_idx]

            assert expected_table in obs_call.args[0]

        @pytest.mark.asyncio
        @pytest.mark.parametrize(
            ["expected_col"],
            [
                ("obsid",),
                ("target_name",),
                ("start_date",),
                ("ra",),
                ("dec",),
                ("instrument",),
                ("grating",),
                ("exposure_mode",),
                ("exposure_time",),
                ("proposal_number",),
            ],
        )
        async def test_should_return_joined_table_with_expected_cols(
            self, expected_col: str
        ):
            table = await get_observation_data_from_tap()
            assert expected_col in table.colnames

        @pytest.mark.asyncio
        async def test_should_return_empty_table_when_no_observations(
            self,
            mock_vo_service_observed_query: AsyncMock,
        ):
            # need to reset query to return nothing
            mock_vo_service_observed_query.side_effect = [
                None,
            ]
            mock_vo_service_observed_query.return_value = None

            table = await get_observation_data_from_tap()

            assert len(table) == 0

    class TestWarnings:
        @pytest.mark.asyncio
        async def test_should_log_warning_when_no_observations(
            self,
            mock_as_flown_logger: MagicMock,
            mock_vo_service_observed_query: AsyncMock,
        ):
            # need to reset query to return nothing
            mock_vo_service_observed_query.side_effect = [
                None,
            ]
            mock_vo_service_observed_query.return_value = None

            await get_observation_data_from_tap()
            call = mock_as_flown_logger.warning.call_args_list[0]

            assert "No observations" in call.args[0]

        @pytest.mark.asyncio
        async def test_should_log_warning_when_telescope_has_no_instruments(
            self,
            mock_as_flown_logger: MagicMock,
            mock_telescope_api: AsyncMock,
        ):
            mock_telescope = MagicMock()
            mock_telescope.instruments = None
            mock_telescope_api.get_telescopes.return_value = [mock_telescope]

            await ingest()
            call = mock_as_flown_logger.warning.call_args_list[0]
            assert "No instruments" in call.args[0]
