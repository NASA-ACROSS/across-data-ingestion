from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.hst.low_fidelity_planned import (
    entrypoint,
    extract_instrument_info_from_observation,
    extract_observation_pointing_coordinates,
    format_across_observation,
    get_instrument_name_from_observation_data,
    get_latest_timeline_file,
    ingest,
    read_planned_exposure_catalog,
    read_timeline_file,
)

from .mocks.low_fidelity_planned_mock_schedule_output import hst_planned_schedule


class TestHSTLowFidelityPlannedScheduleIngestionTask:
    @pytest.fixture(autouse=True)
    def setup(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_instrument_get: list[dict[str, str]],
        mock_schedule_post: Mock,
    ) -> Generator:
        """Setup patched methods used in all tests"""
        with patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.instrument.get",
            return_value=mock_instrument_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post",
            mock_schedule_post,
        ):
            yield

    def test_should_generate_across_schedule(
        self,
        mock_read_planned_exposure_catalog: AsyncMock,
        mock_read_timeline_file: AsyncMock,
        mock_get_latest_timeline_file: AsyncMock,
        mock_schedule_post: Mock,
    ) -> None:
        """Should generate ACROSS schedules"""
        ingest()
        mock_schedule_post.assert_called_once_with(hst_planned_schedule)

    def test_should_log_when_ingestion_is_successful(
        self,
        mock_read_planned_exposure_catalog: AsyncMock,
        mock_read_timeline_file: AsyncMock,
        mock_get_latest_timeline_file: AsyncMock,
        mock_log: Mock,
    ) -> None:
        """Should log info when ingestion is successful"""
        entrypoint()  # type: ignore
        assert "ran successfully" in mock_log.info.call_args.args[0]

    def test_should_log_error_ingestion_fails(
        self,
        mock_read_planned_exposure_catalog: AsyncMock,
        mock_read_timeline_file: AsyncMock,
        mock_get_latest_timeline_file: AsyncMock,
        mock_log: Mock,
        mock_schedule_post: Mock,
    ) -> None:
        """Should log error when ingestion encounters unexpected error"""
        mock_schedule_post.side_effect = Exception()
        entrypoint()  # type: ignore
        assert "encountered an unexpected error" in mock_log.error.call_args.args[0]

    def test_should_read_planned_exposure_catalog_as_dataframe(
        self, mock_pandas_read_csv: MagicMock
    ) -> None:
        """Should read the planned exposure catalog file as a DataFrame"""
        exposure_df = read_planned_exposure_catalog()
        assert isinstance(exposure_df, pd.DataFrame)

    def test_should_get_latest_timeline_filename(
        self,
        mock_soup: MagicMock,
        mock_get: MagicMock,
    ) -> None:
        """Should return the timeline filename farthest in the future"""
        mock_latest_filename = get_latest_timeline_file()
        assert mock_latest_filename == "timeline_07_28_25"

    def test_should_read_timeline_file_as_dataframe(
        self,
        mock_timeline_file_dataframe: pd.DataFrame,
        mock_soup: MagicMock,
        mock_get: MagicMock,
    ) -> None:
        """Should read the timeline file as a DataFrame"""
        mock_timeline_data = read_timeline_file("mock-timeline-filename")
        pd.testing.assert_frame_equal(mock_timeline_data, mock_timeline_file_dataframe)

    def test_extract_coords_should_return_empty_dict_if_target_not_found(
        self,
        mock_planned_exposure_catalog: pd.DataFrame,
    ) -> None:
        """Should return an empty dict if the target is not found in the planned exposure catalog"""
        mock_timeline_observation_data = {"target_name": "mock_fake_target"}
        across_observation = extract_observation_pointing_coordinates(
            mock_planned_exposure_catalog, mock_timeline_observation_data
        )
        assert across_observation == {}

    def test_format_across_observation_should_skip_acq_obs(
        self,
        mock_planned_exposure_catalog: pd.DataFrame,
    ) -> None:
        """Should return an empty dict if the observation is an acquisition image"""
        mock_timeline_observation_data = {
            "target_name": mock_planned_exposure_catalog["object_name"].values[0],
            "mode": "ACQ",
        }
        across_observation = format_across_observation(
            mock_planned_exposure_catalog, mock_timeline_observation_data, {}
        )
        assert across_observation == {}

    def test_format_across_observation_should_skip_obs_with_no_coords(
        self,
        mock_planned_exposure_catalog: pd.DataFrame,
    ) -> None:
        """Should return an empty dict if cannot find coordinates for the observation"""
        mock_timeline_observation_data = {
            "target_name": "BAD Target Name",
            "mode": "ACCUM",
            "date": "2025.209",
            "begin_time": "01:07:54",
            "end_time": "02:03:30",
        }

        across_observation = format_across_observation(
            mock_planned_exposure_catalog, mock_timeline_observation_data, {}
        )
        assert across_observation == {}

    def test_extract_instrument_info_should_log_warning_if_no_filter_found(
        self,
        mock_planned_exposure_catalog: pd.DataFrame,
        mock_log: Mock,
    ) -> None:
        """Should log a warning if no filter found from obs parameters"""
        mock_timeline_observation_data = {
            "target_name": mock_planned_exposure_catalog["object_name"].values[0],
            "element": "mock-element",
            "aperture": "mock-aperture",
        }
        mock_instrument_data = {
            "id": "mock-instrument-id",
            "short_name": "mock_inst",
            "filters": [{"name": "Mock Filter"}],
        }
        extract_instrument_info_from_observation(
            mock_timeline_observation_data, mock_instrument_data
        )

        assert "Could not find filter" in mock_log.warning.call_args.args[0]

    def test_extract_instrument_info_should_return_empty_dict_if_no_filter_found(
        self,
        mock_planned_exposure_catalog: pd.DataFrame,
    ) -> None:
        """Should return an empty dict if no filter found from obs parameters"""
        mock_timeline_observation_data = {
            "target_name": mock_planned_exposure_catalog["object_name"].values[0],
            "element": "mock-element",
            "aperture": "mock-aperture",
        }
        mock_instrument_data = {
            "id": "mock-instrument-id",
            "short_name": "mock_inst",
            "filters": [{"name": "Mock Filter"}],
        }
        obs = extract_instrument_info_from_observation(
            mock_timeline_observation_data, mock_instrument_data
        )

        assert obs == {}

    @pytest.mark.parametrize(
        "mock_observation_data, mock_instrument_data, obs_type",
        [
            (
                {"name": "MockImaging", "element": "F100W"},
                {
                    "short_name": "mock_inst",
                    "filters": [
                        {
                            "name": "F100W",
                            "min_wavelength": 1000,
                            "max_wavelength": 2000,
                        }
                    ],
                },
                "imaging",
            ),
            (
                {"name": "MockSpectroscopy", "element": "G100"},
                {
                    "short_name": "mock_inst",
                    "filters": [
                        {
                            "name": "G100",
                            "min_wavelength": 1000,
                            "max_wavelength": 2000,
                        }
                    ],
                },
                "spectroscopy",
            ),
            (
                {"name": "MockCOS", "element": "F100W"},
                {
                    "short_name": "mock_COS",
                    "filters": [
                        {
                            "name": "F100W",
                            "min_wavelength": 1000,
                            "max_wavelength": 2000,
                        }
                    ],
                },
                "spectroscopy",
            ),
        ],
    )
    def test_extract_instrument_info_should_pick_correct_obs_type(
        self,
        mock_observation_data: dict,
        mock_instrument_data: dict,
        obs_type: str,
    ) -> None:
        """Should identify correct observation type from obs parameters"""
        obs = extract_instrument_info_from_observation(
            mock_observation_data, mock_instrument_data
        )
        assert obs["type"] == obs_type

    @pytest.mark.parametrize(
        "mock_observation_data, expected_name",
        [
            (
                {"instrument": "ACS"},
                "HST_ACS",
            ),
            (
                {"instrument": "COS"},
                "HST_COS",
            ),
            (
                {"instrument": "STIS"},
                "HST_STIS",
            ),
            (
                {"instrument": "WFC3/UVIS"},
                "HST_WFC3_UVIS",
            ),
            (
                {"instrument": "WFC3/IR"},
                "HST_WFC3_IR",
            ),
            ({"instrument": "BAD Instrument"}, ""),
        ],
    )
    def test_get_instrument_from_obs_should_return_correct_name(
        self, mock_observation_data: dict, expected_name: str
    ):
        """Should get the correct instrument name given obs parameters"""
        name = get_instrument_name_from_observation_data(mock_observation_data)
        assert name == expected_name
