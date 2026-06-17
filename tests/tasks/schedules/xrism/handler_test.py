from unittest.mock import MagicMock

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.xrism.handler import XRISMScheduleHandler
from across_data_ingestion.util.across_server import sdk

from .mocks.mock_xrism_observations import mock_xrism_observations


class TestXRISMScheduleHandler:
    class TestCreateXRISMObservations:
        @pytest.mark.parametrize("field", sdk.ObservationCreate.model_fields)
        def test_should_create_observations_with_expected_parameters(
            self,
            fake_observation_df: pd.DataFrame,
            fake_xrism_instrument: sdk.TelescopeInstrument,
            field: str,
        ) -> None:
            """Should extract correct parameters from input DataFrame"""
            handler = XRISMScheduleHandler(
                observation_status=sdk.ObservationStatus.PLANNED,
                schedule_status=sdk.ScheduleStatus.PLANNED,
                schedule_name="low_fidelity_planned",
                schedule_fidelity=sdk.ScheduleFidelity.LOW,
            )
            created_observations = handler._create_xrism_observations(
                observation_df=fake_observation_df,
                observation_status=sdk.ObservationStatus.PLANNED,
                instrument=fake_xrism_instrument,
            )
            expected_obs = sdk.ObservationCreate.model_validate(
                mock_xrism_observations[0]
            )
            assert getattr(created_observations[0], field) == getattr(
                expected_obs, field
            )

    class TestCreateXRISMAcrossSchedule:
        def test_should_create_schedule(self) -> None:
            """Should create ACROSS schedule"""
            mock_observations = [
                sdk.ObservationCreate.model_validate(mock_xrism_observations[0])
            ]
            handler = XRISMScheduleHandler(
                observation_status=sdk.ObservationStatus.PLANNED,
                schedule_status=sdk.ScheduleStatus.PLANNED,
                schedule_name="low_fidelity_planned",
                schedule_fidelity=sdk.ScheduleFidelity.LOW,
            )
            created_schedule = handler._create_xrism_across_schedule(
                schedule_status=sdk.ScheduleStatus.PLANNED,
                schedule_fidelity=sdk.ScheduleFidelity.LOW,
                schedule_name="low_fidelity_planned",
                observations=mock_observations,
            )

            assert isinstance(created_schedule, sdk.ScheduleCreate)

    class TestRun:
        def test_should_call_create_schedule(
            self,
            mock_telescope_api: MagicMock,
            mock_schedule_api: MagicMock,
            fake_observation_df: pd.DataFrame,
            fake_xrism_telescope: sdk.Telescope,
        ) -> None:
            """Should call create schedule on run"""
            mock_telescope_api.get_telescopes.return_value = [fake_xrism_telescope]
            handler = XRISMScheduleHandler(
                observation_status=sdk.ObservationStatus.PLANNED,
                schedule_status=sdk.ScheduleStatus.PLANNED,
                schedule_name="low_fidelity_planned",
                schedule_fidelity=sdk.ScheduleFidelity.LOW,
            )
            handler.run(fake_observation_df)
            mock_schedule_api.create_schedule.assert_called_once()
