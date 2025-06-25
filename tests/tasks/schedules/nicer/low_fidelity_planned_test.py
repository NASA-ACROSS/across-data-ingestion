import json
import os
from unittest.mock import patch

import pandas as pd

from across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned import (
    entrypoint,
    ingest,
    nicer_schedule,
    query_nicer_catalog,
)


class TestNicerLowFidelityScheduleIngestionTask:
    mock_file_base_path = os.path.join(os.path.dirname(__file__), "mocks/")
    mock_observation_table = "nicer_first_10_rows.csv"
    mock_schedule_output = "nicer_first_10_rows_schedule.json"

    def test_should_generate_across_schedules(self):
        """Should generate ACROSS schedules"""
        mock_output_schedule_file = self.mock_file_base_path + self.mock_schedule_output

        with patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.query_nicer_catalog",
            return_value=pd.read_csv(
                self.mock_file_base_path + self.mock_observation_table
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nicer_telescope_id",
                    "instruments": [{"id": "xti_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.logger"
        ):
            schedules = ingest()
            with open(mock_output_schedule_file) as expected_output_file:
                expected = json.load(expected_output_file)
                assert schedules == expected

    def test_should_generate_observations_with_schedule(self):
        """Should generate list of observations with an ACROSS schedule"""
        with patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.query_nicer_catalog",
            return_value=pd.read_csv(
                self.mock_file_base_path + self.mock_observation_table
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nicer_telescope_id",
                    "instruments": [{"id": "xti_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.logger"
        ):
            schedules = ingest()
            assert len(schedules["observations"]) > 0

    def test_query_nicer_catalog_should_return_dataframe_when_successful(self):
        """Should return a DataFrame if querying NICER catalog is successful"""
        with patch(
            "pandas.read_csv",
            return_value=pd.read_csv(
                self.mock_file_base_path + self.mock_observation_table
            ),
        ):
            data = query_nicer_catalog()
            assert isinstance(data, pd.DataFrame)

    def test_query_nicer_catalog_should_return_none_if_query_fails(self):
        """Should return None if querying NICER catalog fails"""
        with patch(
            "pandas.read_csv",
            return_value=None,
            side_effect=ValueError(),
        ):
            data = query_nicer_catalog()
            assert data is None

    def test_create_schedule_should_return_expected(self):
        """Should return an empty dictionary when the input table is empty"""
        # Ingest a table with no rows
        mock_data = pd.read_csv(self.mock_file_base_path + self.mock_observation_table)
        schedule = nicer_schedule("nicer_telescope_id", mock_data, "planned", "low")
        expected_schedule = {
            "date_range": {
                "begin": "2025-06-12T00:06:03.000",
                "end": "2025-06-12T03:11:03.000",
            },
            "fidelity": "low",
            "name": "nicer_obs_planned_2025-06-12_2025-06-12",
            "status": "planned",
            "telescope_id": "nicer_telescope_id",
        }
        assert schedule == expected_schedule

    def test_create_schedule_should_return_empty_dict_when_given_empty_table(self):
        """Should return an empty dictionary when the input table is empty"""
        # Ingest a table with no rows
        mock_data = pd.DataFrame({})
        schedule = nicer_schedule("nicer_telescope_id", mock_data, "planned", "low")
        assert schedule == {}

    def test_should_log_error_when_query_nicer_catalog_returns_none(self):
        """Should log an error when NICER query returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.query_nicer_catalog",
            return_value=None,
        ):
            ingest()
            assert (
                "Failed to read NICER timeline file" in log_mock.warn.call_args.args[0]
            )

    def test_should_return_empty_dict_when_no_schedule_modes(self):
        """Should return an empty dictionary when no schedule modes are provided"""

        with patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.query_nicer_catalog",
            return_value=pd.read_csv(
                self.mock_file_base_path + self.mock_observation_table
            ),
        ):
            schedules = ingest(schedule_modes=["InvalidMode"])
            assert schedules == {}

    def test_should_log_info_when_success(self):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.query_nicer_catalog",
            return_value=pd.read_csv(
                self.mock_file_base_path + self.mock_observation_table
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nicer_telescope_id",
                    "instruments": [{"id": "xti_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.logger"
        ) as log_mock:
            entrypoint()
            assert "Ingestion completed" in log_mock.info.call_args.args[0]

    def test_should_log_error_when_schedule_ingestion_fails(self):
        """Should log an error when schedule ingestion fails"""
        with patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned.ingest",
            return_value=None,
            side_effect=Exception(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nicer_telescope_id",
                    "instruments": [{"id": "xti_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            entrypoint()
            assert "encountered an error" in log_mock.error.call_args.args[0]
