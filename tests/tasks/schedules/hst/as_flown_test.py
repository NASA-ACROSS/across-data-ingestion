from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from astropy.table import Table  # type: ignore[import-untyped]

import across_data_ingestion.tasks.schedules.hst.as_flown as task
import across_data_ingestion.tasks.schedules.hst.util as hst_util
from across_data_ingestion.tasks.schedules.hst.as_flown import (
    extract_instrument_info,
    get_observation_data_from_tap,
    ingest,
    transform_to_across_observation,
)
from across_data_ingestion.util.across_server import sdk


class TestHSTAsFlownScheduleIngestionTask:
    class TestIngest:
        @pytest.fixture(autouse=True)
        def setup(
            self,
            monkeypatch: pytest.MonkeyPatch,
            fake_instrument_info: hst_util.InstrumentInfo,
        ) -> None:
            """Monkeypatch function to extract instrument info"""
            monkeypatch.setattr(
                task,
                "extract_instrument_info",
                MagicMock(return_value=fake_instrument_info),
            )

        @pytest.mark.asyncio
        async def test_should_call_across_create_schedule(
            self,
            mock_schedule_api: MagicMock,
            mock_vo_service_cls: AsyncMock,
        ) -> None:
            """Should create ACROSS schedule"""
            await ingest()

            mock_schedule_api.create_schedule.assert_called_once()

        @pytest.mark.asyncio
        async def test_should_make_schedule_create_instance_when_calling_create_schedule(
            self,
            mock_schedule_api: MagicMock,
            mock_vo_service_cls: AsyncMock,
        ) -> None:
            """Should create ACROSS schedule with ScheduleCreate schema"""
            await ingest()

            args = mock_schedule_api.create_schedule.call_args[0]

            assert isinstance(args[0], sdk.ScheduleCreate)

        @pytest.mark.asyncio
        async def test_should_make_observation_create_instance_when_creating_schedule(
            self, mock_schedule_api: MagicMock
        ) -> None:
            """Should create ACROSS schedule with ObservationCreate schemas"""
            await ingest()

            args = mock_schedule_api.create_schedule.call_args[0]

            assert isinstance(args[0].observations[0], sdk.ObservationCreate)

        @pytest.mark.parametrize(
            ["arg", "expected_input"],
            [
                ("status", sdk.ScheduleStatus.PERFORMED),
                ("fidelity", sdk.ScheduleFidelity.HIGH),
            ],
        )
        @pytest.mark.asyncio
        async def test_should_use_as_flown_schedule_params_when_creating_schedules(
            self,
            mock_schedule_api: AsyncMock,
            arg: str,
            expected_input: str,
        ):
            """Should create schedule with correct params"""
            await ingest()
            call = mock_schedule_api.create_schedule.call_args[0]
            assert getattr(call[0], arg) == expected_input

        @pytest.mark.asyncio
        async def test_should_not_call_schedule_when_invalid_observations_are_filtered_out(
            self,
            mock_get_observation_data_from_tap: AsyncMock,
            mock_schedule_api: MagicMock,
            fake_invalid_observation_table: Table,
        ) -> None:
            """
            Should not call create schedule with invalid observations
            """
            mock_get_observation_data_from_tap.return_value = (
                fake_invalid_observation_table
            )
            await ingest()
            mock_schedule_api.create_schedule.assert_not_called()

        @pytest.mark.asyncio
        async def test_should_not_create_schedules_when_no_observations_are_found(
            self,
            mock_get_observation_data_from_tap: AsyncMock,
            mock_schedule_api: MagicMock,
        ) -> None:
            """Should not create schedule if no observations are found from TAP service"""
            mock_get_observation_data_from_tap.return_value = None

            await ingest()

            mock_schedule_api.create_schedule.assert_not_called()

    class TestGetObservationDataFromTap:
        @pytest.mark.asyncio
        async def test_should_initialize_vo_service(
            self, mock_vo_service_cls: AsyncMock
        ):
            """Should call the VOService"""
            await get_observation_data_from_tap()

            mock_vo_service_cls.assert_called_once()

        @pytest.mark.asyncio
        @pytest.mark.parametrize(
            ["expected_table", "call_idx"],
            [("dbo.hst_science_missionmast", 0)],
        )
        async def test_should_query_tap_for_observations(
            self,
            mock_vo_service_query: AsyncMock,
            expected_table: str,
            call_idx: int,
        ):
            """Should access expected table for as-flown observations"""
            await get_observation_data_from_tap()

            obs_call = mock_vo_service_query.call_args_list[call_idx]

            assert expected_table in obs_call.args[0]

        @pytest.mark.asyncio
        @pytest.mark.parametrize(
            ["expected_col"],
            [
                ("sci_pep_id",),
                ("sci_obset_id",),
                ("sci_targname",),
                ("sci_start_time",),
                ("sci_stop_time",),
                ("sci_ra",),
                ("sci_dec",),
                ("sci_instrument_config",),
                ("sci_spec_1234",),
                ("sci_pa_aper",),
                ("sci_operating_mode",),
                ("sci_actual_duration",),
            ],
        )
        async def test_should_return_table_with_expected_cols(self, expected_col: str):
            """Should return table with the expected columns from the TAP"""
            table = await get_observation_data_from_tap()
            assert table is not None
            assert expected_col in table.colnames

        @pytest.mark.asyncio
        async def test_should_return_none_when_no_observations(
            self,
            mock_vo_service_query: AsyncMock,
        ):
            """Should return None when TAP returns no observations"""
            # need to reset query to return nothing
            mock_vo_service_query.side_effect = [
                None,
            ]
            mock_vo_service_query.return_value = None

            table = await get_observation_data_from_tap()

            assert table is None

        @pytest.mark.asyncio
        async def test_should_log_warning_when_no_observations(
            self,
            mock_vo_service_query: AsyncMock,
            mock_as_flown_logger: MagicMock,
        ):
            """Should log warning when cannot find observations"""
            # need to reset query to return nothing
            mock_vo_service_query.side_effect = [
                None,
            ]
            mock_vo_service_query.return_value = None

            await get_observation_data_from_tap()

            mock_as_flown_logger.warning.assert_called_with(
                "No HST observations found from TAP",
            )

    class TestTransformToAcrossObservation:
        def test_should_return_none_when_no_instrument_info(
            self,
            monkeypatch: pytest.MonkeyPatch,
            fake_observed_observation_row: pd.Series,
        ) -> None:
            """Should return None when instrument not found for the observation"""
            monkeypatch.setattr(
                task,
                "extract_instrument_info",
                MagicMock(return_value=None),
            )

            across_observation = transform_to_across_observation(
                fake_observed_observation_row, [], {}
            )

            assert across_observation is None

    class TestExtractInstrumentInfo:
        def test_should_log_warning_when_no_instrument_short_name_match(
            self,
            fake_instrument: sdk.TelescopeInstrument,
            mock_as_flown_logger: MagicMock,
            fake_observed_observation_row: pd.Series,
        ) -> None:
            """Should log a warning when no match found for the short name"""
            fake_observed_observation_row["sci_instrument_config"] = "FAKE"

            extract_instrument_info(fake_observed_observation_row, [fake_instrument])

            mock_as_flown_logger.warning.assert_called_with(
                "Could not match data to ACROSS instrument.",
                instrument="FAKE",
            )

        def test_should_log_warning_when_no_across_instrument_found(
            self,
            fake_instrument: sdk.TelescopeInstrument,
            mock_as_flown_logger: MagicMock,
            fake_observed_observation_row: pd.Series,
            monkeypatch: pytest.MonkeyPatch,
        ) -> None:
            """Should log a warning when no ACROSS instrument found for the short name"""
            monkeypatch.setattr(
                hst_util,
                "get_instrument_short_name_from_observation",
                MagicMock(return_value="FAKE_INST"),
            )

            extract_instrument_info(fake_observed_observation_row, [fake_instrument])

            mock_as_flown_logger.warning.assert_called_with(
                "Could not match data to ACROSS instrument.",
                instruments=[fake_instrument],
                obs_instrument=fake_observed_observation_row.sci_instrument_config,
            )

        def test_should_log_warning_when_no_filter_found(
            self,
            fake_instrument: sdk.TelescopeInstrument,
            mock_as_flown_logger: MagicMock,
            fake_observed_observation_row: pd.Series,
        ) -> None:
            """Should log a warning when no filter found from obs parameters"""
            fake_observed_observation_row["sci_spec_1234"] = "FAKE"

            fake_instrument.short_name = "HST_WFC3_UVIS"
            fake_instrument.filters = []

            extract_instrument_info(fake_observed_observation_row, [fake_instrument])

            mock_as_flown_logger.warning.assert_called_with(
                "Could not find filter for instrument.",
                filter_names="FAKE",
            )

        def test_should_return_none_when_no_filter_found(
            self,
            fake_instrument: sdk.TelescopeInstrument,
            fake_observed_observation_row: pd.Series,
        ) -> None:
            """Should return None when no filter found from obs parameters"""
            fake_observed_observation_row["sci_spec_1234"] = "FAKE"

            fake_instrument.short_name = "HST_WFC3_UVIS"
            fake_instrument.filters = []

            obs = extract_instrument_info(
                fake_observed_observation_row, [fake_instrument]
            )

            assert obs is None

        def test_should_log_warning_when_multiple_filters_are_matched(
            self,
            fake_instrument: sdk.TelescopeInstrument,
            fake_observed_observation_row: pd.Series,
            mock_as_flown_logger: MagicMock,
        ):
            """Should log a warning when multiple filters are matched"""
            # Set multiple filters in observation row
            fake_observed_observation_row["sci_spec_1234"] = "F606W;F160W"

            # Set instrument short name and filter names to produce multiple matches
            fake_instrument.short_name = "HST_WFC3_UVIS"
            assert fake_instrument.filters is not None
            assert len(fake_instrument.filters) > 1
            fake_instrument.filters[0].name = "F606W"
            fake_instrument.filters[1].name = "F160W"

            extract_instrument_info(fake_observed_observation_row, [fake_instrument])

            mock_as_flown_logger.warning.assert_called_with(
                "Multiple filters matched for an element/aperture combination. Selecting the first filter...",
                matches=fake_instrument.filters,
            )
