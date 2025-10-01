import os
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest
from astropy.io import ascii  # type: ignore
from astroquery.mast import Observations  # type: ignore

import across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned as task

from .mocks.fake_across_jwst_plan import fake_across_plan
from .mocks.fake_jwst_plan import fake_jwst_plan
from .mocks.fake_mast_query_response import fake_mast_query_response
from .mocks.fake_schedule_file_response import fake_schedule_file_response
from .mocks.fake_science_execution_page import fake_science_execution_response_text


class mock_response:
    def __init__(self, text: str, raise_response: bool = False):
        self.text = text
        self.raise_response = raise_response

    def raise_for_status(self):
        if self.raise_response:
            raise Exception
        pass


class Test:
    class TestIngest:
        def test_should_generate_across_schedules(
            self,
            mock_telescope_api: MagicMock,
            mock_schedule_api: MagicMock,
            monkeypatch: pytest.MonkeyPatch,
        ):
            """Should generate ACROSS schedules"""
            monkeypatch.setattr(
                task,
                "query_jwst_planned_execution_schedule",
                MagicMock(return_value=fake_jwst_plan),
            )

            task.ingest()
            mock_schedule_api.create_schedule.assert_called_with(fake_across_plan)

        def test_query_jwst_planned_execution_schedule_should_return_expected(
            self, monkeypatch: pytest.MonkeyPatch
        ):
            """Should return correct dataframe from file result"""
            monkeypatch.setattr(
                task,
                "get_most_recent_jwst_planned_url",
                MagicMock(return_value="mock_url"),
            )
            monkeypatch.setattr(
                httpx,
                "get",
                MagicMock(return_value=mock_response(text=fake_schedule_file_response)),
            )
            monkeypatch.setattr(
                task,
                "read_mast_observations",
                MagicMock(return_value=fake_mast_query_response),
            )

            instruments_info = {
                "JWST_MIRI": "miri_instrument_id",
                "JWST_NIRCAM": "nircam_instrument_id",
                "JWST_NIRSPEC": "nirspec_instrument_id",
                "JWST_NIRISS": "niriss_instrument_id",
            }
            jwst_planned_query = task.query_jwst_planned_execution_schedule(
                instruments_info
            )
            calculated = jwst_planned_query.to_dict(orient="records")
            expected = fake_jwst_plan.to_dict(orient="records")
            assert calculated == expected

        def test_query_jwst_planned_execution_schedule_should_return_empty_df_on_exception(
            self, monkeypatch: pytest.MonkeyPatch
        ):
            """Should return empty dataframe from get that raises response"""
            monkeypatch.setattr(
                task,
                "get_most_recent_jwst_planned_url",
                MagicMock(return_value="mock_url"),
            )
            monkeypatch.setattr(
                httpx,
                "get",
                MagicMock(return_value=mock_response(text="", raise_response=True)),
            )

            jwst_planned_query = task.query_jwst_planned_execution_schedule({})
            assert jwst_planned_query.empty

        def test_read_mast_observations_should_return_result(
            self, monkeypatch: pytest.MonkeyPatch
        ):
            """Should return correct dataframe from mast query observations"""
            current_dir = os.path.dirname(__file__)
            fake_mast_astropy_table = os.path.join(
                current_dir, "mocks", "fake_mast_astropy_table.ecsv"
            )

            monkeypatch.setattr(
                Observations,
                "query_criteria",
                MagicMock(return_value=ascii.read(fake_mast_astropy_table)),
            )

            calculated = task.read_mast_observations([])
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

        def test_parse_science_execution_page_should_return_result(
            self, monkeypatch: pytest.MonkeyPatch
        ):
            """Should return a string url when parsing the jwst science execution page"""
            monkeypatch.setattr(
                httpx,
                "get",
                MagicMock(
                    return_value=mock_response(
                        text=fake_science_execution_response_text
                    )
                ),
            )

            calculated = task.get_most_recent_jwst_planned_url()
            expected = "/files/live/sites/www/files/home/jwst/science-execution/observing-schedules/_documents/20250804_report_20250802.txt"
            assert calculated == expected

        def test_should_log_error_when_query_swift_catalog_returns_none(
            self, mock_logger, monkeypatch: pytest.MonkeyPatch
        ):
            """Should log an error when Swift query returns None"""
            monkeypatch.setattr(
                task,
                "query_jwst_planned_execution_schedule",
                MagicMock(return_value=pd.DataFrame({})),
            )

            task.ingest()
            assert (
                "Failed to read JWST observation data"
                in mock_logger.warn.call_args.args[0]
            )

        @pytest.mark.asyncio
        async def test_should_log_info_when_success(
            self,
            mock_logger,
            mock_telescope_api,
            mock_schedule_api,
            monkeypatch: pytest.MonkeyPatch,
        ):
            """Should log info with ran at when success"""
            monkeypatch.setattr(
                task,
                "query_jwst_planned_execution_schedule",
                MagicMock(return_value=fake_jwst_plan),
            )

            await task.entrypoint()  # type: ignore
            assert "Schedule ingestion completed." in mock_logger.info.call_args.args[0]

        @pytest.mark.asyncio
        async def test_should_log_error_when_schedule_ingestion_fails(
            self, mock_logger, monkeypatch: pytest.MonkeyPatch
        ):
            """Should log an error when schedule ingestion fails"""
            monkeypatch.setattr(
                task, "ingest", MagicMock(return_value=None, side_effect=Exception())
            )

            await task.entrypoint()  # type: ignore
            assert (
                "Schedule ingestion encountered an unknown error."
                in mock_logger.error.call_args.args[0]
            )
