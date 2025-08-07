from unittest.mock import Mock, patch

import pandas as pd

from across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned import (
    entrypoint,
    ingest,
    ixpe_to_across_schedule,
    query_ixpe_schedule,
)

from .mocks.ixpe_across_schedule import ixpe_across_schedule
from .mocks.mock_ixpe_query import mock_ixpe_query
from .mocks.sample_respone import sample_respone


class mock_response:
    def __init__(self, read_sample_response: bool = True):
        if read_sample_response:
            self.text = sample_respone
        else:
            self.text = ""

    def raise_for_status(self):
        pass


class TestNicerLowFidelityScheduleIngestionTask:
    def test_should_generate_across_schedules(self, mock_schedule_post: Mock):
        """Should generate ACROSS schedules"""
        with patch(
            "across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned.query_ixpe_schedule",
            return_value=pd.DataFrame(mock_ixpe_query),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "ixpe_telescope_id",
                    "instruments": [{"id": "ixpe_instrument_id"}],
                }
            ],
        ):
            ingest()
            mock_schedule_post.assert_called_with(ixpe_across_schedule)

    def test_query_nicer_catalog_should_return_dataframe_when_successful(self):
        """Should return a DataFrame if querying IXPE catalog is successful"""
        with patch(
            "httpx.get",
            return_value=mock_response(),
        ):
            data = query_ixpe_schedule(url="")
            assert isinstance(data, pd.DataFrame)

    def test_query_nicer_catalog_should_return_none_if_query_fails(self):
        """Should return None if querying IXPE catalog fails"""
        with patch(
            "httpx.get",
            return_value=mock_response(read_sample_response=False),
        ):
            data = query_ixpe_schedule(url="")
            assert data is None

    def test_create_schedule_should_return_expected(self):
        """Should return an empty dictionary when the input table is empty"""
        # Ingest a table with no rows
        mock_data = pd.DataFrame(mock_ixpe_query)
        schedule = ixpe_to_across_schedule(
            "ixpe_telescope_id", mock_data, "planned", "low"
        )
        expected_schedule = {
            "date_range": {
                "begin": "2025-04-08T06:00:00.000",
                "end": "2025-09-08T06:00:00.000",
            },
            "fidelity": "low",
            "name": "ixpe_ltp_2025-04-08_2025-09-08",
            "status": "planned",
            "telescope_id": "ixpe_telescope_id",
        }
        assert schedule == expected_schedule

    def test_should_log_error_when_query_ixpe_catalog_returns_none(self):
        """Should log an error when ixpe query returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "httpx.get",
            return_value=None,
        ):
            ingest()
            assert (
                "Failed to read IXPE timeline file" in log_mock.warn.call_args.args[0]
            )

    def test_should_log_info_when_success(self):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned.ingest",
            return_value={},
        ):
            entrypoint()
            assert "Schedule ingestion completed." in log_mock.info.call_args.args[0]

    def test_should_log_error_when_schedule_ingestion_fails(self):
        """Should log an error when schedule ingestion fails"""
        with patch(
            "across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned.ingest",
            return_value=None,
            side_effect=Exception(),
        ):
            entrypoint()
            assert (
                "Schedule ingestion encountered an unknown error"
                in log_mock.error.call_args.args[0]
            )
