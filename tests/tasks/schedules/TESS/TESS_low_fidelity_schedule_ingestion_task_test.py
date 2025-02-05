import json
import os
from unittest import TestCase
from unittest.mock import patch

import pytest

from across_data_ingestion.tasks.schedules.TESS import (
    TESS_low_fidelity_schedule_ingestion_task,
)


class TestTESSLowFidelityScheduleIngestionTask(TestCase):
    def create_mock_relative_filepaths(self):
        current_dir = os.path.dirname(__file__)
        mock_orbit_times_file = os.path.join(
            current_dir, "mocks", "TESS_orbit_times.csv"
        )
        mock_pointings_file = os.path.join(current_dir, "mocks", "TESS_pointings.csv")
        across_schedule_output = os.path.join(
            current_dir, "output", "ACROSS_schedule_output.json"
        )

        return mock_orbit_times_file, mock_pointings_file, across_schedule_output

    @pytest.mark.asyncio
    def test_should_generate_across_schedules(self):
        """Should generate schedules sucessfully"""
        mock_orbit_times_file, mock_pointings_file, across_schedule_output = (
            self.create_mock_relative_filepaths()
        )

        with patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_POINTINGS_FILE",
            new=mock_pointings_file,
        ), patch(
            "across_data_ingestion.tasks.schedules.TESS.TESS_low_fidelity_schedule_ingestion_task.TESS_ORBIT_TIMES_FILE",
            new=mock_orbit_times_file,
        ):
            schedules = TESS_low_fidelity_schedule_ingestion_task()
            with open(across_schedule_output) as expected_output_file:
                expected = json.load(expected_output_file)
                self.assertEqual(json.dumps(schedules), json.dumps(expected))

    @pytest.mark.asyncio
    def test_should_raise_file_not_found_when_not_found(self):
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
            assert "error" in log_mock.error.call_args.args[0]
