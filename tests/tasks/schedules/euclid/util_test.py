from unittest.mock import MagicMock

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.euclid.util import EuclidScheduleHandler
from across_data_ingestion.util.across_server import sdk

from .mocks.mock_nisp_observations import mock_nisp_observations
from .mocks.mock_vis_observations import mock_vis_observations


class TestCreateVisObservations:
    @pytest.mark.parametrize("field", sdk.ObservationCreate.model_fields)
    def test_should_create_observations_with_expected_parameters(
        self,
        fake_pointing_file_data: list[dict],
        fake_vis_instrument_id: str,
        field: str,
    ) -> None:
        """Should extract correct parameters from input DataFrame"""
        obs_df = pd.DataFrame(fake_pointing_file_data)
        handler = EuclidScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_name="low_fidelity_planned",
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
        )
        created_observations = handler._create_vis_observations(
            observation_df=obs_df,
            observation_status=sdk.ObservationStatus.PLANNED,
            instrument_id=fake_vis_instrument_id,
        )
        expected_obs = sdk.ObservationCreate.model_validate(mock_vis_observations[0])
        assert getattr(created_observations[0], field) == getattr(expected_obs, field)


class TestCreateNispObservations:
    @pytest.mark.parametrize("field", sdk.ObservationCreate.model_fields)
    def test_should_create_observations_with_expected_parameters(
        self,
        fake_pointing_file_data: list[dict],
        fake_nisp_instrument_id: str,
        field: str,
    ) -> None:
        """Should extract correct parameters from input DataFrame"""
        obs_df = pd.DataFrame(fake_pointing_file_data)
        handler = EuclidScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_name="low_fidelity_planned",
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
        )
        created_observations = handler._create_nisp_observations(
            observation_df=obs_df,
            observation_status=sdk.ObservationStatus.PLANNED,
            instrument_id=fake_nisp_instrument_id,
        )
        for i, obs in enumerate(created_observations):
            expected_obs = sdk.ObservationCreate.model_validate(
                mock_nisp_observations[i]
            )
            assert getattr(obs, field) == getattr(expected_obs, field)

    def test_should_log_warning_if_no_grism_found_for_obs_id(
        self, fake_pointing_file_data: list[dict], mock_util_logger: MagicMock
    ) -> None:
        """Should log a warning if no grism info found for an obs ID"""
        fake_pointing_file_data[0]["grism"] = None
        obs_df = pd.DataFrame(fake_pointing_file_data)
        handler = EuclidScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_name="low_fidelity_planned",
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
        )
        handler._create_nisp_observations(
            observation_df=obs_df,
            observation_status=sdk.ObservationStatus.PLANNED,
            instrument_id="fake_instrument_id",
        )

        assert (
            "Could not find grism information for observation ID"
            in mock_util_logger.warning.call_args[0][0]
        )


class TestCreateEuclidAcrossSchedule:
    def test_should_create_schedule(self, fake_euclid_telescope_id: str) -> None:
        """Should create ACROSS schedule"""
        mock_observations = [
            sdk.ObservationCreate.model_validate(mock_vis_observations[0])
        ]
        handler = EuclidScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_name="low_fidelity_planned",
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
        )
        created_schedule = handler._create_euclid_across_schedule(
            telescope_id=fake_euclid_telescope_id,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
            schedule_name="low_fidelity_planned",
            observations=mock_observations,
        )

        assert isinstance(created_schedule, sdk.ScheduleCreate)


class TestEuclidScheduleHandler:
    @pytest.mark.parametrize(
        ["field"],
        [
            ("observation_status",),
            ("schedule_status",),
            ("schedule_name",),
            ("schedule_fidelity",),
            ("telescope_id",),
            ("instrument_ids",),
        ],
    )
    def test_should_set_values_on_init(
        self, mock_telescope_api: MagicMock, field: str
    ) -> None:
        """Should set the expected values on initialization"""
        handler = EuclidScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_name="low_fidelity_planned",
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
        )

        assert hasattr(handler, field)

    def test_should_call_create_schedule(
        self, mock_schedule_api: MagicMock, fake_pointing_file_data: list[dict]
    ) -> None:
        """Should call create schedule on run"""
        obs_df = pd.DataFrame(fake_pointing_file_data)
        handler = EuclidScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_name="low_fidelity_planned",
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
        )
        handler.instrument_ids = {
            "Euclid VIS": "fake_vis_instrument_id",
            "Euclid NISP": "fake_nisp_instrument_id",
        }
        handler.run(obs_df)
        mock_schedule_api.create_schedule.assert_called_once()
