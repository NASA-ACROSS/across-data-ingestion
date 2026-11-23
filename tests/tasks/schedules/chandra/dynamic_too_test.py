from unittest.mock import AsyncMock, MagicMock

import pytest

from across_data_ingestion.tasks.schedules.chandra.dynamic_too import (
    ingest,
)
from across_data_ingestion.util.across_server import sdk


class TestChandraDynamicToOScheduleIngestionTask:
    @pytest.mark.asyncio
    async def test_should_call_get_telescopes_with_sdk(
        self,
        mock_telescope_api: AsyncMock,
        mock_read_chandra_too_page_potential_too: AsyncMock,
    ):
        """Should get telescopes from ACROSS"""
        await ingest()
        mock_telescope_api.get_telescopes.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_create_schedule_in_across(
        self,
        mock_schedule_api: AsyncMock,
        mock_read_chandra_too_page_potential_too: AsyncMock,
    ):
        """Should create ACROSS schedules"""
        await ingest()
        mock_schedule_api.create_schedule.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_ignore_calibration_observations(
        self,
        mock_schedule_api: AsyncMock,
        mock_read_chandra_too_page_bad: AsyncMock,
    ):
        """Should not create ACROSS observations for calibration rows"""
        await ingest()
        call = mock_schedule_api.create_schedule.call_args_list[0]
        assert not any(["CAL" in obs.object_name for obs in call[0][0].observations])

    class TestMatchScheduleNameStatusFidelity:
        @pytest.mark.parametrize(
            "too_page_fixture",
            [
                "mock_read_chandra_too_page_potential_too",
                "mock_read_chandra_too_page_preliminary",
                "mock_read_chandra_too_page_final",
            ],
        )
        @pytest.mark.asyncio
        async def test_correctly_match_name_to_status(
            self,
            too_page_fixture: str,
            request: pytest.FixtureRequest,
            mock_schedule_api: AsyncMock,
        ):
            """Should correctly match schedule name to status"""
            request.getfixturevalue(too_page_fixture)
            await ingest()
            call = mock_schedule_api.create_schedule.call_args_list[0]
            assert call[0][0].status == sdk.ScheduleStatus.PLANNED

        @pytest.mark.parametrize(
            "too_page_fixture, fidelity",
            [
                ("mock_read_chandra_too_page_potential_too", sdk.ScheduleFidelity.LOW),
                ("mock_read_chandra_too_page_preliminary", sdk.ScheduleFidelity.LOW),
                ("mock_read_chandra_too_page_final", sdk.ScheduleFidelity.HIGH),
            ],
        )
        @pytest.mark.asyncio
        async def test_correctly_match_name_to_fidelity(
            self,
            too_page_fixture: str,
            fidelity: str,
            request: pytest.FixtureRequest,
            mock_schedule_api: AsyncMock,
        ):
            """Should correctly match schedule name to fidelity"""
            request.getfixturevalue(too_page_fixture)
            await ingest()
            call = mock_schedule_api.create_schedule.call_args_list[0]
            assert call[0][0].fidelity == fidelity

    class TestWarnings:
        @pytest.mark.asyncio
        async def test_should_log_warning_when_telescope_has_no_instruments(
            self,
            mock_dynamic_too_logger: MagicMock,
            mock_telescope_api: AsyncMock,
        ):
            """Should log warning when telescope has no instruments"""
            mock_telescope = MagicMock()
            mock_telescope.instruments = None
            mock_telescope_api.get_telescopes.return_value = [mock_telescope]

            await ingest()
            call = mock_dynamic_too_logger.warning.call_args_list[0]
            assert "No instruments" in call.args[0]

        @pytest.mark.asyncio
        async def test_should_log_warning_when_heading_has_bad_schedule_name(
            self,
            mock_dynamic_too_logger: MagicMock,
            mock_read_chandra_too_page_bad: AsyncMock,
        ):
            """Should log warning when the schedule status and fidelity cannot be inferred from the heading"""
            await ingest()
            call = mock_dynamic_too_logger.warning.call_args_list[0]
            assert (
                "Could not parse schedule name for status and fidelity" in call.args[0]
            )

        @pytest.mark.asyncio
        async def test_should_log_warning_when_no_heading_found(
            self,
            mock_dynamic_too_logger: MagicMock,
            mock_read_chandra_too_page_no_heading: AsyncMock,
        ):
            """Should log warning when the schedule table has no heading"""
            await ingest()
            call = mock_dynamic_too_logger.warning.call_args_list[0]
            assert "Could not read schedule name heading" in call.args[0]
