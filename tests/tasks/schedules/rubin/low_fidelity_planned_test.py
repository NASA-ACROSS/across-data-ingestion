from unittest.mock import MagicMock

import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.rubin.low_fidelity_planned import (
    ingest,
)

from .mocks.mock_rubin_obsloctap_query import result as rubin_obsloctap_result
from .mocks.rubin_across_schedule import schedule as expected_schedule


class TestNicerLowFidelityScheduleIngestionTask:
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
            patch_query_rubin_schedule.return_value = pd.DataFrame(
                columns=["id", "execution_status"]
            )
            ingest()
            assert (
                "No observations found in Rubin OBSLOCTAP for low fidelity planned schedules."
                in mock_logger.warning.call_args.args[0]
            )
