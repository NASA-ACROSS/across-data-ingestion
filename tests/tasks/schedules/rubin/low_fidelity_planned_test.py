from unittest.mock import MagicMock

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.rubin.low_fidelity_planned import (
    get_low_fidelity_planned_schedules_data,
    ingest,
)

from .mocks.mock_rubin_obsloctap_query import result as rubin_obsloctap_result
from .mocks.rubin_across_schedule import schedule as expected_schedule


class TestRubinLowFidelityScheduleIngestionTask:
    class TestIngest:
        @pytest.fixture(autouse=True)
        def patch_query_rubin_schedule(self, monkeypatch: pytest.MonkeyPatch):
            mock = MagicMock(return_value=rubin_obsloctap_result)
            monkeypatch.setattr(
                pd,
                "read_json",
                mock,
            )

            return mock

        def test_should_create_across_schedules(self, mock_schedule_api: MagicMock):
            """Should create ACROSS schedules"""
            ingest()
            mock_schedule_api.create_schedule.assert_called_with(expected_schedule)

        def test_should_log_error_when_rubin_query_returns_none(
            self,
            mock_logger: MagicMock,
            patch_query_rubin_schedule: MagicMock,
        ):
            """Should log an error when rubin query returns None"""
            data: dict = {
                "id": [],
                "execution_status": [],
            }
            patch_query_rubin_schedule.return_value = pd.DataFrame(data=data)
            ingest()
            assert (
                "No observations found in Rubin OBSLOCTAP for low fidelity planned schedules."
                in mock_logger.warning.call_args.args[0]
            )

        def test_should_warn_when_unknown_filter(
            self,
            mock_logger: MagicMock,
            patch_query_rubin_schedule: MagicMock,
        ):
            """Should log a warning when rubin query has observations with unknown filter names"""
            data: dict = {
                "id": [1, 2],
                "execution_status": ["Scheduled", "Scheduled"],
                "em_min": [3.0e-10, 3500e-10],
                "em_max": [3.4e-10, 3900e-10],
                "t_min": [59000, 59001],
                "t_max": [59001, 59002],
            }
            patch_query_rubin_schedule.return_value = pd.DataFrame(data=data)
            get_low_fidelity_planned_schedules_data("Scheduled")
            assert (
                "Some observations have unknown filter names and will be excluded."
                in mock_logger.warning.call_args.args[0]
            )
