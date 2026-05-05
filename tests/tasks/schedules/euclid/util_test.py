from unittest.mock import MagicMock

import pandas as pd
import pytest
from httpx import HTTPError

from across_data_ingestion.tasks.schedules.euclid.util import (
    EuclidScheduleHandler,
    create_euclid_across_schedule,
    create_nisp_observations,
    create_vis_observations,
    parse_pointing_file,
    retrieve_schedule_file,
)
from across_data_ingestion.util.across_server import sdk

from .mocks.mock_nisp_observations import mock_nisp_observations
from .mocks.mock_vis_observations import mock_vis_observations


class TestRetrieveScheduleFile:
    def test_should_log_error_if_request_fails(
        self, mock_httpx_get: MagicMock, mock_util_logger: MagicMock
    ) -> None:
        """Should log an error if the httpx request to retrieve the schedule page fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        retrieve_schedule_file("")

        assert (
            "Long term planned schedule request failed"
            in mock_util_logger.error.call_args[0][0]
        )

    def test_should_return_empty_string_if_request_fails(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return an empty string if the httpx request to retrieve the schedule page fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        file_url = retrieve_schedule_file("")

        assert len(file_url) == 0

    def test_should_return_empty_string_if_bs4_cannot_find_a_tags(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return an empty string if BeautifulSoup cannot find any a tags in the schedule page"""
        mock_httpx_get.return_value.return_value = ""
        file_url = retrieve_schedule_file("")

        assert len(file_url) == 0

    def test_should_return_correct_url_from_a_tag(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return the correct url corresponding to the schedule file"""
        mock_httpx_get.return_value.text = (
            "<a href='/bad_url'>Bad URL</a><a href='/good_url.txt'>good_url.txt</a>"
        )
        file_url = retrieve_schedule_file("")

        assert file_url == "/good_url.txt"


class TestParsePointingFile:
    def test_should_log_error_if_request_fails(
        self, mock_httpx_get: MagicMock, mock_util_logger: MagicMock
    ) -> None:
        """Should log an error if the httpx request fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        parse_pointing_file("")

        assert (
            "Failed to retrieve schedule file" in mock_util_logger.error.call_args[0][0]
        )

    def test_should_return_empty_dataframe_if_request_fails(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return an empty pandas DataFrame if the httpx request fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        df = parse_pointing_file("")

        assert df.empty

    def test_should_return_dataframe_when_successful(
        self, fake_pointing_file: str, mock_httpx_get: MagicMock
    ) -> None:
        """Should return a pandas DataFrame with the correct data when the httpx request is successful"""
        mock_httpx_get.return_value.text = fake_pointing_file
        df = parse_pointing_file("")

        assert not df.empty

    def test_should_log_warning_if_schedule_file_has_incomplete_row(
        self,
        fake_pointing_file_bad_data: str,
        mock_httpx_get: MagicMock,
        mock_util_logger: MagicMock,
    ) -> None:
        """Should log a warning when ingesting a schedule file that has a row with missing values"""
        mock_httpx_get.return_value.text = fake_pointing_file_bad_data
        parse_pointing_file("")

        assert (
            "Skipping line with insufficient columns"
            in mock_util_logger.warning.call_args[0][0]
        )


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
        created_observations = create_vis_observations(
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
        created_observations = create_nisp_observations(
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
        create_nisp_observations(
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
        created_schedule = create_euclid_across_schedule(
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
