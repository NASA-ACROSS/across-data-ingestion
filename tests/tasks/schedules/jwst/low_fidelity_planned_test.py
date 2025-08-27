import os
from unittest.mock import Mock, patch

import pandas as pd
from astropy.io import ascii  # type: ignore

from across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned import (
    entrypoint,
    get_most_recent_jwst_planned_url,
    ingest,
    query_jwst_planned_execution_schedule,
    read_mast_observations,
)

from .mocks.across_jwst_plan import across_jwst_plan
from .mocks.mock_jwst_plan import mock_jwst_plan_query
from .mocks.mock_mast_query_response import mock_mast_query_response
from .mocks.mock_schedule_file_response import schedule_file_response
from .mocks.mock_scienct_execution_page import science_execution_response_text


class mock_response:
    def __init__(self, text: str, raise_response: bool = False):
        self.text = text
        self.raise_response = raise_response

    def raise_for_status(self):
        if self.raise_response:
            raise Exception
        pass


def mock_telescope_get() -> list[dict]:
    return [
        {
            "id": "jwst_telescope_id",
            "instruments": [
                {"short_name": "JWST_MIRI", "id": "miri_instrument_id"},
                {"short_name": "JWST_NIRCAM", "id": "nircam_instrument_id"},
                {"short_name": "JWST_NIRISS", "id": "niriss_instrument_id"},
                {"short_name": "JWST_NIRSPEC", "id": "nirspec_instrument_id"},
            ],
        }
    ]


class TestJWSTLowFidelityScheduleIngestionTask:
    def test_should_generate_across_schedules(self, mock_schedule_post: Mock):
        """Should generate ACROSS schedules"""

        with patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.query_jwst_planned_execution_schedule",
            return_value=mock_jwst_plan_query,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get(),
        ):
            ingest()
            mock_schedule_post.assert_called_with(across_jwst_plan)

    def test_query_jwst_planned_execution_schedule_should_return_expected(self):
        """Should return correct dataframe from file result"""
        with patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.get_most_recent_jwst_planned_url",
            return_value="mock_url",
        ), patch(
            "httpx.get",
            return_value=mock_response(text=schedule_file_response),
        ), patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.read_mast_observations",
            return_value=mock_mast_query_response,
        ):
            instruments_info = {
                "JWST_MIRI": "miri_instrument_id",
                "JWST_NIRCAM": "nircam_instrument_id",
                "JWST_NIRSPEC": "nirspec_instrument_id",
                "JWST_NIRISS": "niriss_instrument_id",
            }
            jwst_planned_query = query_jwst_planned_execution_schedule(instruments_info)
            calculated = jwst_planned_query.to_dict(orient="records")
            expected = mock_jwst_plan_query.to_dict(orient="records")
            assert calculated == expected

    def test_query_jwst_planned_execution_schedule_should_return_empty_df_on_exception(
        self,
    ):
        """Should return correct dataframe from file result"""
        with patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.get_most_recent_jwst_planned_url",
            return_value="mock_url",
        ), patch(
            "httpx.get",
            return_value=mock_response(
                text=schedule_file_response, raise_response=True
            ),
        ), patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.read_mast_observations",
            return_value=mock_mast_query_response,
        ):
            instruments_info = {
                "JWST_MIRI": "miri_instrument_id",
                "JWST_NIRCAM": "nircam_instrument_id",
                "JWST_NIRSPEC": "nirspec_instrument_id",
                "JWST_NIRISS": "niriss_instrument_id",
            }
            jwst_planned_query = query_jwst_planned_execution_schedule(instruments_info)
            assert jwst_planned_query.empty

    def test_read_mast_observations_should_return_result(self):
        current_dir = os.path.dirname(__file__)
        # files for testing ingestion of orbits observations for each schedule
        mock_mast_astropy_table = os.path.join(
            current_dir, "mocks", "mock_mast_astropy_table.ecsv"
        )

        with patch(
            "astroquery.mast.Observations.query_criteria",
            return_value=ascii.read(mock_mast_astropy_table),
        ):
            calculated = read_mast_observations([])
            expected = pd.DataFrame(
                {
                    "instrument_name": {"0": "NIRISS/IMAGE"},
                    "filters": {"0": "CLEAR;GR700XD"},
                    "obs_id": {"0": "jw05924001001_xx102_00001_niriss"},
                    "target_name": {"0": "HAT-P-11"},
                    "s_ra": {"0": 297.70936375},
                    "s_dec": {"0": 48.0808611111},
                    "em_min": {"0": 600.0},
                    "em_max": {"0": 2800.0},
                }
            )
            assert calculated.to_dict(orient="records") == expected.to_dict(
                orient="records"
            )

    def test_parse_science_execution_page_should_return_result(self):
        """Should return a string url when parsing the jwst science execution page"""
        with patch(
            "httpx.get",
            return_value=mock_response(text=science_execution_response_text),
        ):
            calculated = get_most_recent_jwst_planned_url()
            expected = "/files/live/sites/www/files/home/jwst/science-execution/observing-schedules/_documents/20250804_report_20250802.txt"
            assert calculated == expected

    def test_should_log_error_when_query_swift_catalog_returns_none(self):
        """Should log an error when Swift query returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.query_jwst_planned_execution_schedule",
            return_value=pd.DataFrame({}),
        ):
            ingest()
            assert (
                "Failed to read JWST observation data"
                in log_mock.warn.call_args.args[0]
            )

    def test_should_log_info_when_success(self):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.query_jwst_planned_execution_schedule",
            return_value=mock_jwst_plan_query,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get(),
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.logger"
        ) as log_mock:
            entrypoint()
            assert "Schedule ingestion completed." in log_mock.info.call_args.args[0]

    def test_should_log_error_when_schedule_ingestion_fails(self):
        """Should log an error when schedule ingestion fails"""
        with patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.ingest",
            return_value=None,
            side_effect=Exception(),
        ):
            entrypoint()
            assert "encountered an unknown error" in log_mock.error.call_args.args[0]
