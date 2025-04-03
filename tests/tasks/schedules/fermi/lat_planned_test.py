import json
import os
from datetime import datetime
from unittest.mock import patch
from urllib.error import HTTPError

from astropy.io import fits  # type: ignore[import-untyped]
from astropy.table import Table  # type: ignore[import-untyped]

from across_data_ingestion.tasks.schedules.fermi.lat_planned import (
    calculate_date_from_fermi_week,
    entrypoint,
    ingest,
)


class TestFermiLATPlannedScheduleIngestionTask:
    mock_lat_pointing_file_base_path = os.path.join(os.path.dirname(__file__), "mocks/")
    mock_prelim_pointing_file = "FERMI_POINTING_PRELIM_875_2025065_2025072_00.fits"
    mock_final_pointing_file = "FERMI_POINTING_FINAL_875_2025065_2025072_00.fits"

    def create_mock_output_schedule(self, filetype: str):
        mock_filepath = self.mock_lat_pointing_file_base_path
        return mock_filepath + f"mock_{filetype}_schedule_output.json"

    def test_should_calculate_date_from_fermi_week(self):
        """Should correctly calculate start date for a given Fermi week"""
        fermi_week_start_date = calculate_date_from_fermi_week(875)
        assert fermi_week_start_date == "2025065"

    def test_should_generate_across_schedules_with_prelim_pointing_file(self):
        """Should generate ACROSS schedules with prelim pointing file"""
        mock_output_schedule_file = self.create_mock_output_schedule("prelim")

        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=Table(
                fits.open(
                    self.mock_lat_pointing_file_base_path
                    + self.mock_prelim_pointing_file
                )[1].data
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "PRELIM"},
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_current_time",
            return_value=datetime(2025, 3, 28, 0, 0, 0).isoformat(),
        ):
            schedules = ingest()
            with open(mock_output_schedule_file) as expected_output_file:
                expected = json.load(expected_output_file)
                assert json.dumps(schedules) == json.dumps(expected)

    def test_should_generate_across_schedules_with_final_pointing_file(self):
        """Should generate ACROSS schedules with final pointing file"""
        mock_output_schedule_file = self.create_mock_output_schedule("final")

        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=Table(
                fits.open(
                    self.mock_lat_pointing_file_base_path
                    + self.mock_final_pointing_file
                )[1].data
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_current_time",
            return_value=datetime(2025, 3, 28, 0, 0, 0).isoformat(),
        ):
            schedules = ingest()
            with open(mock_output_schedule_file) as expected_output_file:
                expected = json.load(expected_output_file)
                assert json.dumps(schedules) == json.dumps(expected)

    def test_should_generate_observations_with_schedule(self):
        """Should generate list of observations with an ACROSS schedule"""
        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=Table(
                fits.open(
                    self.mock_lat_pointing_file_base_path
                    + self.mock_final_pointing_file
                )[1].data
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ):
            schedules = ingest()
            assert len(schedules[0]["observations"]) > 0

    def test_should_log_warning_when_retrieving_file_returns_404(self):
        """Should log warning when retrieving a file returns a 404"""
        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.logger"
        ) as log_mock, patch(
            "astropy.io.fits.open",
            return_value=None,
            side_effect=HTTPError(url="", code=404, msg="", hdrs=None, fp=None),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ):
            entrypoint()
            assert "skipping" in log_mock.warning.call_args.args[0]

    def test_should_log_error_when_retrieving_file_returns_unexpected_status_code(self):
        """Should log error when retrieving a file returns an unexpected status code"""
        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=None,
            side_effect=HTTPError(url="", code=500, msg="", hdrs=None, fp=None),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ):
            entrypoint()
            assert "unexpectedly failed" in log_mock.error.call_args.args[0]

    def test_should_log_error_when_data_processing_fails(self):
        """Should log error when processing a pointing file raises an exception"""
        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=None,
            side_effect=Exception(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ):
            entrypoint()
            assert "unexpectedly failed" in log_mock.error.call_args.args[0]

    def test_should_log_error_when_no_files_found(self):
        """Should log an error when no LAT pointing files are found for a given set of params"""
        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=None,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ):
            entrypoint()
            assert "Could not read any" in log_mock.error.call_args.args[0]

    def test_should_log_info_when_success(self):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=Table(
                fits.open(
                    self.mock_lat_pointing_file_base_path
                    + self.mock_final_pointing_file
                )[1].data
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ):
            entrypoint()
            assert "ran at" in log_mock.info.call_args.args[0]

    def test_should_log_error_when_schedule_ingestion_fails(self):
        """Should log an error when schedule ingestion fails"""

        # Ingest a pointing file with no columns
        mock_data = Table(
            fits.open(
                self.mock_lat_pointing_file_base_path + self.mock_final_pointing_file
            )[1].data
        )
        mock_data.keep_columns([])

        with patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.retrieve_lat_pointing_file",
            return_value=mock_data,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "fermi_lat_telescope_uuid",
                    "instruments": [{"id": "fermi_lat_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_FILETYPE_DICTIONARY",
            new={0: "FINAL"},
        ):
            entrypoint()
            assert "encountered an error" in log_mock.error.call_args.args[0]
