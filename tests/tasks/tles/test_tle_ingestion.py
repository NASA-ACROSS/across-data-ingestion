from unittest.mock import Mock, patch

from across_data_ingestion.tasks.tles.tle_ingestion import (
    entrypoint,
    extract_norad_info_from_tle_ephems_for_observatories,
    ingest,
    retrieve_tle_from_spacetrack,
)

from .conftest import MockTLEClass


class TestTLEIngestion:
    def test_extract_tle_parameters_for_observatories(
        self, mock_observatory_info: list[dict]
    ) -> None:
        """Should extract TLE parameters from ACROSS observatory data"""
        tle_parameters = extract_norad_info_from_tle_ephems_for_observatories(
            mock_observatory_info
        )
        assert len(tle_parameters)

    def test_extract_tle_parameters_should_extract_norad_id(
        self, mock_observatory_info: list[dict]
    ) -> None:
        """Should extract NORAD ID from ACROSS observatory data"""
        tle_parameters = extract_norad_info_from_tle_ephems_for_observatories(
            mock_observatory_info
        )
        assert (
            list(tle_parameters.values())[0]
            == mock_observatory_info[0]["ephemeris_types"][0]["parameters"]["norad_id"]
        )

    def test_retrieve_tle_from_spacetrack_should_return_tle_when_successful(
        self, mock_tle: MockTLEClass
    ) -> None:
        """Should return TLE information when fetching TLE from spacetrack is successful"""
        with patch(
            "across_data_ingestion.tasks.tles.tle_ingestion.get_tle",
            Mock(return_value=mock_tle),
        ):
            tle = retrieve_tle_from_spacetrack(123456)
            assert len(tle)

    def test_retrieve_tle_from_spacetrack_should_return_empty_dict_if_fails(
        self,
    ) -> None:
        """Should return an empty dict if unable to fetch TLE from spacetrack"""
        with patch(
            "across_data_ingestion.tasks.tles.tle_ingestion.get_tle",
            Mock(return_value=None),
        ):
            tle = retrieve_tle_from_spacetrack(123456)
            assert tle == {}

    def test_should_post_tle_to_across_server_when_successful(
        self, mock_tle: MockTLEClass, mock_observatory_info: list[dict]
    ) -> None:
        mock_tle_post = Mock(return_value=None)
        """Should post the TLE to the ACROSS server when successful"""
        with patch(
            "across_data_ingestion.tasks.tles.tle_ingestion.get_tle",
            Mock(return_value=mock_tle),
        ), patch(
            "across_data_ingestion.util.across_api.observatory.get",
            return_value=mock_observatory_info,
        ), patch("across_data_ingestion.util.across_api.tle.post", mock_tle_post):
            ingest()
            mock_tle_post.assert_called_once()

    def test_should_log_warning_when_no_tle_fetched(
        self,
        mock_observatory_info: list[dict],
    ) -> None:
        """Should log a warning when no TLE was fetched from spacetrack"""
        with patch(
            "across_data_ingestion.tasks.tles.tle_ingestion.get_tle",
            Mock(return_value=None),
        ), patch(
            "across_data_ingestion.util.across_api.observatory.get",
            return_value=mock_observatory_info,
        ), patch(
            "across_data_ingestion.util.across_api.tle.post", return_value=None
        ), patch("across_data_ingestion.tasks.tles.tle_ingestion.logger") as log_mock:
            ingest()
            assert "Could not fetch TLE" in log_mock.warn.call_args.args[0]

    def test_should_log_info_when_ran_successfully(
        self, mock_tle: MockTLEClass, mock_observatory_info: list[dict]
    ) -> None:
        """Should log info when the task runs successfully"""
        with patch(
            "across_data_ingestion.tasks.tles.tle_ingestion.get_tle",
            return_value=mock_tle,
        ), patch(
            "across_data_ingestion.util.across_api.observatory.get",
            return_value=mock_observatory_info,
        ), patch(
            "across_data_ingestion.util.across_api.tle.post", return_value=None
        ), patch("across_data_ingestion.tasks.tles.tle_ingestion.logger") as log_mock:
            entrypoint()  # type: ignore
            assert "ran at" in log_mock.info.call_args.args[0]

    def test_should_log_error_when_task_fails(self) -> None:
        """Should log error when the task fails"""
        with patch(
            "across_data_ingestion.tasks.tles.tle_ingestion.ingest",
            Mock(return_value=None, side_effect=Exception),
        ), patch("across_data_ingestion.tasks.tles.tle_ingestion.logger") as log_mock:
            entrypoint()  # type: ignore
            assert "encountered an error" in log_mock.error.call_args.args[0]
