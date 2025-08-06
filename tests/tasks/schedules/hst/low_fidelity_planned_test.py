from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.hst.low_fidelity_planned import (
    entrypoint,
    extract_instrument_info,
    extract_observation_pointing_coordinates,
    get_latest_timeline_file,
    ingest,
    read_planned_exposure_catalog,
    read_timeline_file,
    transform_to_across_observation,
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
        mock_schedule_post: Mock,
    ) -> None:
        """Should generate ACROSS schedules"""
        ingest()
        mock_schedule_post.assert_called_once_with(hst_planned_schedule)

    def test_ingest_should_return_if_cannot_read_timeline_file(
        self,
        mock_read_timeline_file: AsyncMock,
        mock_schedule_post: Mock,
    ) -> None:
        """Should return if cannot read timeline file"""
        mock_read_timeline_file.return_value = None
        ingest()
        mock_schedule_post.assert_not_called()

    def test_should_log_when_ingestion_is_successful(
        self,
        mock_log: Mock,
    ) -> None:
        """Should log info when ingestion is successful"""
        entrypoint()  # type: ignore
        assert "ran successfully" in mock_log.info.call_args.args[0]

    def test_should_log_error_ingestion_fails(
        self,
        mock_log: Mock,
        mock_schedule_post: Mock,
    ) -> None:
        """Should log error when ingestion encounters unexpected error"""
        mock_schedule_post.side_effect = Exception()
        entrypoint()  # type: ignore
        assert "encountered an unexpected error" in mock_log.error.call_args.args[0]

    def test_should_read_planned_exposure_catalog_as_dataframe(
        self, mock_pandas: MagicMock
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
        mock_pandas: MagicMock,
    ) -> None:
        """Should read the timeline file as a DataFrame"""
        mock_timeline_data = read_timeline_file("mock-timeline-filename")
        assert mock_timeline_data is not None
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

    def test_ingest_should_skip_invalid_obs(
        self,
        mock_transform_to_across_schedule: MagicMock,
        mock_read_timeline_file: MagicMock,
        mock_schedule_post: Mock,
        mock_timeline_file_invalid_obs_dataframe: pd.DataFrame,
        mock_schedule_data: dict,
    ) -> None:
        """
        Should return an empty list of obs if the observation
        is a calibration or acquisition observation
        """
        mock_read_timeline_file.return_value = mock_timeline_file_invalid_obs_dataframe
        ingest()
        mock_schedule_post.assert_called_with(mock_schedule_data)  # No obs appended

    def test_format_across_observation_should_return_if_cannot_find_instrument_name(
        self,
        mock_planned_exposure_catalog: pd.DataFrame,
    ) -> None:
        """Should return an empty dict if cannot find instrument name for observation"""
        mock_timeline_observation_data = {
            "target_name": mock_planned_exposure_catalog["object_name"].values[0],
            "mode": "ACCUM",
            "instrument": "mock-instrument",
        }
        across_observation = transform_to_across_observation(
            mock_planned_exposure_catalog, mock_timeline_observation_data, []
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
            "instrument": "ACS",
        }

        across_observation = transform_to_across_observation(
            mock_planned_exposure_catalog, mock_timeline_observation_data, []
        )
        assert across_observation == {}

    def test_format_across_observation_should_skip_obs_with_no_instrument_info(
        self,
        mock_extract_observation_pointing_coordinate: MagicMock,
        mock_extract_instrument_info_from_observation: MagicMock,
        mock_planned_exposure_catalog: pd.DataFrame,
    ) -> None:
        """Should return an empty dict if cannot find instrument info for the observation"""
        mock_extract_instrument_info_from_observation.return_value = {}
        mock_timeline_observation_data = {
            "target_name": "Mock Target Name",
            "mode": "ACCUM",
        }

        across_observation = transform_to_across_observation(
            mock_planned_exposure_catalog,
            mock_timeline_observation_data,
            [],
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
            "instrument": "ACS",
        }
        mock_instrument_data = [
            {
                "id": "mock-instrument-id",
                "short_name": "HST_ACS",
                "filters": [],
            }
        ]
        extract_instrument_info(mock_timeline_observation_data, mock_instrument_data)

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
            "instrument": "ACS",
        }
        mock_instrument_data = [
            {
                "id": "mock-instrument-id",
                "short_name": "HST_ACS",
                "filters": [],
            }
        ]
        obs = extract_instrument_info(
            mock_timeline_observation_data, mock_instrument_data
        )

        assert obs == {}

    @pytest.mark.parametrize(
        "mock_observation_data, mock_instrument_data, obs_type",
        [
            (
                {
                    "name": "MockImaging",
                    "element": "F100W",
                    "aperture": "mock-aperture",
                    "instrument": "ACS",
                },
                {
                    "id": "mock-id",
                    "short_name": "HST_ACS",
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
                {
                    "name": "MockSpectroscopy",
                    "element": "G100",
                    "aperture": "mock-aperture",
                    "instrument": "ACS",
                },
                {
                    "id": "mock-id",
                    "short_name": "HST_ACS",
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
                {
                    "name": "MockCOS",
                    "element": "F100W",
                    "aperture": "mock-aperture",
                    "instrument": "COS",
                },
                {
                    "id": "mock-id",
                    "short_name": "HST_COS",
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
        obs = extract_instrument_info(mock_observation_data, [mock_instrument_data])
        assert obs["type"] == obs_type

    @pytest.mark.parametrize(
        "mock_observation_data, expected_name",
        [
            (
                {
                    "instrument": "ACS",
                    "element": "ACS",
                    "aperture": "",
                },
                "ACS_Filter",
            ),
            (
                {
                    "instrument": "COS",
                    "element": "COS",
                    "aperture": "",
                },
                "COS_Filter",
            ),
            (
                {
                    "instrument": "STIS",
                    "element": "STIS",
                    "aperture": "",
                },
                "STIS_Filter",
            ),
            (
                {
                    "instrument": "WFC3/UVIS",
                    "element": "UVIS",
                    "aperture": "",
                },
                "UVIS_Filter",
            ),
            (
                {
                    "instrument": "WFC3/IR",
                    "element": "IR",
                    "aperture": "",
                },
                "IR_Filter",
            ),
        ],
    )
    def test_get_instrument_info_should_return_correct_filter_name(
        self, mock_observation_data: dict, expected_name: str
    ):
        """Should get the correct filter name given obs parameters"""
        instruments = [
            {
                "id": "mock-id",
                "short_name": "HST_COS",
                "filters": [
                    {
                        "name": "COS_Filter",
                        "min_wavelength": 1000,
                        "max_wavelength": 2000,
                    }
                ],
            },
            {
                "id": "mock-id",
                "short_name": "HST_ACS",
                "filters": [
                    {
                        "name": "ACS_Filter",
                        "min_wavelength": 1000,
                        "max_wavelength": 2000,
                    }
                ],
            },
            {
                "id": "mock-id",
                "short_name": "HST_STIS",
                "filters": [
                    {
                        "name": "STIS_Filter",
                        "min_wavelength": 1000,
                        "max_wavelength": 2000,
                    }
                ],
            },
            {
                "id": "mock-id",
                "short_name": "HST_WFC3_UVIS",
                "filters": [
                    {
                        "name": "UVIS_Filter",
                        "min_wavelength": 1000,
                        "max_wavelength": 2000,
                    }
                ],
            },
            {
                "id": "mock-id",
                "short_name": "HST_WFC3_IR",
                "filters": [
                    {
                        "name": "IR_Filter",
                        "min_wavelength": 1000,
                        "max_wavelength": 2000,
                    }
                ],
            },
        ]
        instrument_info = extract_instrument_info(mock_observation_data, instruments)
        assert instrument_info["bandpass"]["filter_name"] == expected_name
