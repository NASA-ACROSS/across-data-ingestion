import json
import os
from unittest.mock import patch

from astropy.io import ascii  # type: ignore[import-untyped]
from astropy.table import Table  # type: ignore[import-untyped]

from across_data_ingestion.tasks.schedules.nustar.as_flown import (
    create_schedule,
    entrypoint,
    ingest,
    query_nustar_catalog,
)


class TestNustarAsFlownScheduleIngestionTask:
    mock_file_base_path = os.path.join(os.path.dirname(__file__), "mocks/")
    mock_observation_table = "NUMASTER_mock_table.ascii"
    mock_schedule_output = "nustar_as_flown_mock_schedule_output.json"
    mock_start_time = 123456  # Mock start time in MJD

    def test_should_generate_across_schedules(self):
        """Should generate ACROSS schedules"""
        mock_output_schedule_file = self.mock_file_base_path + self.mock_schedule_output

        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.query_nustar_catalog",
            return_value=Table(
                ascii.read(self.mock_file_base_path + self.mock_observation_table)
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nustar_telescope_uuid",
                    "instruments": [{"id": "nustar_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock:
            ingest()
            schedules = log_mock.info.call_args.args[0]
            with open(mock_output_schedule_file) as expected_output_file:
                expected = json.load(expected_output_file)
                assert [json.loads(schedules)] == expected

    def test_should_generate_observations_with_schedule(self):
        """Should generate list of observations with an ACROSS schedule"""
        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.query_nustar_catalog",
            return_value=Table(
                ascii.read(self.mock_file_base_path + self.mock_observation_table)
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nustar_telescope_uuid",
                    "instruments": [{"id": "nustar_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock:
            ingest()
            schedules = log_mock.info.call_args.args[0]
            assert len(json.loads(schedules)["observations"]) > 0

    def test_query_nustar_catalog_should_return_astropy_table_when_successful(self):
        """Should return an astropy Table when successfully querying NuSTAR catalog"""
        mock_data = Table(
            ascii.read(self.mock_file_base_path + self.mock_observation_table)
        )

        class MockResult:
            def __init__(self):
                self.table = mock_data

            def to_table(self):
                return self.table

        with patch("astroquery.heasarc.Heasarc.query_tap", return_value=MockResult()):
            data = query_nustar_catalog(self.mock_start_time)
            assert isinstance(data, Table)

    def test_query_nustar_catalog_should_log_error_if_catalog_not_found(
        self,
    ):
        """Should log an error if the NUMASTER catalog is not available to query"""
        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock, patch(
            "astroquery.heasarc.Heasarc.query_tap",
            return_value=None,
            side_effect=ValueError(),
        ):
            query_nustar_catalog(self.mock_start_time)
            assert "Could not query" in log_mock.warn.call_args.args[0]

    def test_query_nustar_catalog_should_log_error_when_unexpected_error_raised(self):
        """Should log error when querying the NUMASTER catalog raises an unexpected error"""
        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock, patch(
            "astroquery.heasarc.Heasarc.query_tap",
            return_value=None,
            side_effect=Exception(),
        ):
            query_nustar_catalog(self.mock_start_time)
            assert "unexpectedly failed" in log_mock.error.call_args.args[0]

    def test_query_nustar_catalog_should_return_none_if_query_fails(self):
        """Should return None if querying NUMASTER catalog fails"""
        with patch(
            "astroquery.heasarc.Heasarc.query_tap",
            return_value=None,
            side_effect=Exception(),
        ):
            data = query_nustar_catalog(self.mock_start_time)
            assert data is None

    def test_create_schedule_should_return_empty_dict_when_given_empty_table(self):
        """Should return an empty dictionary when the input table is empty"""
        # Ingest a table with no rows
        mock_data = Table(
            ascii.read(self.mock_file_base_path + self.mock_observation_table)
        )
        mock_data.keep_columns([])
        schedule = create_schedule("nustar_telescope_id", mock_data)
        assert schedule == {}

    def test_should_log_warning_when_catalog_query_returns_empty_table(self):
        """Should log warning when the NUMASTER query returns an empty table"""
        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.query_nustar_catalog",
            return_value=Table(
                ascii.read(self.mock_file_base_path + self.mock_observation_table)
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nustar_telescope_uuid",
                    "instruments": [{"id": "nustar_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.create_schedule",
            return_value={},
        ):
            ingest()
            assert "Empty table" in log_mock.warn.call_args.args[0]

    def test_should_log_error_when_query_nustar_catalog_returns_none(self):
        """Should log an error when NUMASTER query returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.query_nustar_catalog",
            return_value=None,
        ):
            ingest()
            assert "Could not query" in log_mock.error.call_args.args[0]

    def test_should_log_info_when_success(self):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.query_nustar_catalog",
            return_value=Table(
                ascii.read(self.mock_file_base_path + self.mock_observation_table)
            ),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nustar_telescope_uuid",
                    "instruments": [{"id": "nustar_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            entrypoint()
            assert "ingestion completed" in log_mock.info.call_args.args[0]

    def test_should_log_error_when_schedule_ingestion_fails(self):
        """Should log an error when schedule ingestion fails"""
        with patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.nustar.as_flown.ingest",
            return_value=None,
            side_effect=Exception(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "nustar_telescope_uuid",
                    "instruments": [{"id": "nustar_instrument_uuid"}],
                }
            ],
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            entrypoint()
            assert "encountered an error" in log_mock.error.call_args.args[0]
