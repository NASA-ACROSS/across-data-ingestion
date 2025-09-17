from unittest.mock import AsyncMock, MagicMock

import pytest
from astropy.table import Table  # type: ignore[import-untyped]

from across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned import (
    get_observation_data_from_tap,
    ingest,
    match_instrument_from_tap_observation,
)
from across_data_ingestion.util.across_server import sdk


class TestChandraHighFidelityPlannedScheduleIngestionTask:
    @pytest.mark.asyncio
    async def test_should_call_get_telescopes_with_sdk(
        self,
        mock_telescope_api: AsyncMock,
    ):
        """Should get telescopes from ACROSS"""
        await ingest()
        mock_telescope_api.get_telescopes.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_create_schedule_in_across(
        self,
        mock_schedule_api: AsyncMock,
    ):
        """Should create ACROSS schedules"""
        await ingest()
        mock_schedule_api.create_schedule.assert_called_once()

    @pytest.mark.parametrize(
        "mock_tap_row, expected_instrument_short_name",
        [
            (
                {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "NONE"},
                "ACIS",
            ),
            (
                {"instrument": "ACIS", "grating": "HETG", "exposure_mode": "NONE"},
                "ACIS-HETG",
            ),
            (
                {"instrument": "ACIS", "grating": "LETG", "exposure_mode": "NONE"},
                "ACIS-LETG",
            ),
            (
                {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "CC"},
                "ACIS-CC",
            ),
            (
                {"instrument": "HRC", "grating": "NONE", "exposure_mode": ""},
                "HRC",
            ),
            (
                {"instrument": "HRC", "grating": "HETG", "exposure_mode": ""},
                "HRC-HETG",
            ),
            (
                {"instrument": "HRC", "grating": "LETG", "exposure_mode": ""},
                "HRC-LETG",
            ),
            (
                {"instrument": "HRC", "grating": "NONE", "exposure_mode": "TIMING"},
                "HRC-Timing",
            ),
            (
                {"instrument": "ACIS", "grating": "BAD_GRATING", "exposure_mode": ""},
                None,
            ),
            (
                {"instrument": "HRC", "grating": "BAD_GRATING", "exposure_mode": ""},
                None,
            ),
            (
                {"instrument": "BAD_INSTRUMENT", "grating": "", "exposure_mode": ""},
                None,
            ),
        ],
    )
    def test_should_match_instrument_from_tap_observation(
        self,
        mock_tap_row: dict,
        expected_instrument_short_name: tuple,
        fake_instruments_by_short_name: dict[str, sdk.IDNameSchema],
    ) -> None:
        """Should return correct instrument name and id from observation row"""
        instrument = match_instrument_from_tap_observation(
            fake_instruments_by_short_name, mock_tap_row
        )

        assert instrument.short_name == expected_instrument_short_name

    class TestGetObservationsFromTap:
        @pytest.mark.asyncio
        async def test_should_initialize_vo_service(
            self, mock_vo_service_cls: AsyncMock
        ):
            await get_observation_data_from_tap()

            mock_vo_service_cls.assert_called_once()

        @pytest.mark.asyncio
        async def test_should_use_within_context_manager(
            self, mock_vo_service: AsyncMock
        ):
            await get_observation_data_from_tap()

            mock_vo_service.__aenter__.assert_called_once()

        @pytest.mark.asyncio
        @pytest.mark.parametrize(
            ["expected_table", "call_idx"],
            [("cxc.observation", 0), ("ivoa.obsplan", 1)],
        )
        async def test_should_query_tap_for_observations(
            self, mock_vo_service_query: AsyncMock, expected_table: str, call_idx: int
        ):
            await get_observation_data_from_tap()

            obs_call = mock_vo_service_query.call_args_list[call_idx]

            assert expected_table in obs_call.args[0]

        @pytest.mark.asyncio
        @pytest.mark.parametrize(
            ["expected_col"],
            [
                ("obs_id",),
                ("target_name",),
                ("t_plan_exptime",),
                ("start_date",),
                ("ra",),
                ("dec",),
                ("instrument",),
                ("grating",),
                ("exposure_mode",),
            ],
        )
        async def test_should_return_joined_table_with_expected_cols(
            self, expected_col: str
        ):
            table = await get_observation_data_from_tap()
            assert expected_col in table.colnames

        class TestWarnings:
            @pytest.mark.parametrize(
                (
                    "expected_warning",
                    "fake_observation_table",
                    "fake_exposure_times_table",
                ),
                [
                    ("No observations", None, None),
                    ("No exposure times", "fake_observation_table", None),
                ],
                indirect=[
                    "fake_observation_table",
                    "fake_exposure_times_table",
                ],
            )
            @pytest.mark.asyncio
            async def test_should_log_warning_when_no_exposure_times(
                self,
                mock_vo_service_query: AsyncMock,
                mock_logger: MagicMock,
                expected_warning: str,
                fake_observation_table: Table | None,
                fake_exposure_times_table: Table | None,
            ):
                # only observation table is returned
                mock_vo_service_query.side_effect = [
                    fake_observation_table,
                    fake_exposure_times_table,
                ]

                await get_observation_data_from_tap()

                call = mock_logger.warning.call_args_list[0]

                assert expected_warning in call.args[0]

            @pytest.mark.parametrize(
                ("fake_observation_table", "fake_exposure_times_table"),
                [
                    (None, None),
                    ("fake_observation_table", None),
                ],
                indirect=True,
            )
            @pytest.mark.asyncio
            async def test_should_return_empty_table_when_no_observations(
                self,
                fake_observation_table: Table | None,
                fake_exposure_times_table: Table | None,
                mock_vo_service_query: AsyncMock,
            ):
                # need to reset both to return nothing
                mock_vo_service_query.side_effect = [
                    fake_observation_table,
                    fake_exposure_times_table,
                ]
                mock_vo_service_query.return_value = None

                table = await get_observation_data_from_tap()

                assert len(table) == 0

            @pytest.mark.asyncio
            async def test_should_log_warning_when_observations_do_not_match_len_of_exposures(
                self,
                fake_observation_data: dict,
                fake_exposure_times_table: dict,
                mock_vo_service_query: AsyncMock,
                mock_logger: MagicMock,
            ):
                fake_observation_data2 = dict.copy(fake_observation_data)
                fake_observation_data2["obsid"] = 1

                mock_vo_service_query.side_effect = [
                    Table([fake_observation_data, fake_observation_data2]),
                    fake_exposure_times_table,
                ]

                await get_observation_data_from_tap()

                call = mock_logger.warning.call_args_list[0]

                assert "Mismatched number" in call.args[0]
