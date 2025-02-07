import json
import os
from unittest.mock import patch

from across_data_ingestion.tasks.schedules.TESS import (
    TESS_low_fidelity_schedule_ingestion_task,
)


class TestTESSLowFidelityScheduleIngestionTask:
    def create_mock_placeholder_obs_relative_filepaths(self):
        current_dir = os.path.dirname(__file__)
        # files for testing placeholder observations without orbits observations data
        mock_orbit_times_placeholder_obs_file = os.path.join(
            current_dir, "mocks", "placeholder_observations", "TESS_orbit_times.csv"
        )
        mock_pointings_placeholder_obs_file = os.path.join(
            current_dir, "mocks", "placeholder_observations", "TESS_pointings.csv"
        )
        across_schedule_placeholder_obs_output = os.path.join(
            current_dir,
            "mocks",
            "placeholder_observations",
            "ACROSS_schedule_output.json",
        )

        return (
            mock_orbit_times_placeholder_obs_file,
            mock_pointings_placeholder_obs_file,
            across_schedule_placeholder_obs_output,
        )

    def create_mock_orbit_obs_relative_filepaths(self):
        current_dir = os.path.dirname(__file__)
        # files for testing ingestion of orbits observations for each schedule
        mock_orbit_times_orbits_obs_file = os.path.join(
            current_dir, "mocks", "orbit_observations", "TESS_orbit_times.csv"
        )
        mock_pointings_orbits_obs_file = os.path.join(
            current_dir, "mocks", "orbit_observations", "TESS_pointings.csv"
        )
        across_schedule_orbits_obs_output = os.path.join(
            current_dir, "mocks", "orbit_observations", "ACROSS_schedule_output.json"
        )

        return (
            mock_orbit_times_orbits_obs_file,
            mock_pointings_orbits_obs_file,
            across_schedule_orbits_obs_output,
        )

    def test_should_generate_across_schedules_with_placeholder_observations(self):
        """Should generate ACROSS schedules with placeholder observations"""
        (
            mock_orbit_times_placeholder_obs_file,
            mock_pointings_placeholder_obs_file,
            across_schedule_placeholder_obs_output,
        ) = self.create_mock_placeholder_obs_relative_filepaths()

        with patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_POINTINGS_FILE",
            new=mock_pointings_placeholder_obs_file,
        ), patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_ORBIT_TIMES_FILE",
            new=mock_orbit_times_placeholder_obs_file,
        ):
            schedules = TESS_low_fidelity_schedule_ingestion_task()
            with open(across_schedule_placeholder_obs_output) as expected_output_file:
                expected = json.load(expected_output_file)
                assert json.dumps(schedules) == json.dumps(expected)

    def test_should_generate_across_schedules_with_orbit_observations(self):
        """Should generate ACROSS schedules with orbit observations"""
        (
            mock_orbit_times_orbits_obs_file,
            mock_pointings_orbits_obs_file,
            across_schedule_orbits_obs_output,
        ) = self.create_mock_orbit_obs_relative_filepaths()

        with patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_POINTINGS_FILE",
            new=mock_pointings_orbits_obs_file,
        ), patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_ORBIT_TIMES_FILE",
            new=mock_orbit_times_orbits_obs_file,
        ):
            schedules = TESS_low_fidelity_schedule_ingestion_task()
            with open(across_schedule_orbits_obs_output) as expected_output_file:
                expected = json.load(expected_output_file)
                assert json.dumps(schedules) == json.dumps(expected)

    def test_should_log_error_when_file_not_found(self):
        """Should log an error when file is not found"""
        mock_orbit_times_file, mock_pointings_file = ("", "")

        with patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_POINTINGS_FILE",
            new=mock_pointings_file,
        ), patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_ORBIT_TIMES_FILE",
            new=mock_orbit_times_file,
        ):
            TESS_low_fidelity_schedule_ingestion_task()
            assert "encountered an error" in log_mock.error.call_args.args[0]

    def test_should_log_info_when_success(self):
        """Should log info with ran at when success"""
        (
            mock_orbit_times_placeholder_obs_file,
            mock_pointings_placeholder_obs_file,
            across_schedule_placeholder_obs_output,
        ) = self.create_mock_placeholder_obs_relative_filepaths()

        with patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_POINTINGS_FILE",
            new=mock_pointings_placeholder_obs_file,
        ), patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_ORBIT_TIMES_FILE",
            new=mock_orbit_times_placeholder_obs_file,
        ):
            TESS_low_fidelity_schedule_ingestion_task()
            assert "ran at" in log_mock.info.call_args.args[0]
