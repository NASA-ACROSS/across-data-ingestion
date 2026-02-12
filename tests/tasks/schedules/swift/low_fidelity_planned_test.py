from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.schedules.swift.low_fidelity_planned as task
from across_data_ingestion.util.across_server import sdk


@pytest.fixture(autouse=True)
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)

    return mock


class TestQuerySwiftPlan:
    def test_should_query_swift_for_plan(self, mock_swift_too: MagicMock):
        task.query_swift_plan()

        mock_swift_too.PlanQuery.assert_called_once()

    def test_should_return_empty_array_when_no_obs_entries(
        self, mock_swift_too: MagicMock
    ):
        mock_swift_too.PlanQuery.return_value = []

        entries = task.query_swift_plan()

        assert not entries


class TestIngest:
    def test_should_log_warning_when_no_swift_plan(
        self, monkeypatch: pytest.MonkeyPatch, mock_logger: MagicMock
    ):
        monkeypatch.setattr(task, "query_swift_plan", MagicMock(return_value=[]))

        task.ingest()

        mock_logger.warning.assert_called_once()

    def test_should_run_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_swift_schedule_handler_cls: MagicMock,
    ):
        task.ingest()

        mock_swift_schedule_handler_cls.return_value.run.assert_called_once()

    def test_should_instantiate_handler_with_expected_args(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_swift_schedule_handler_cls: MagicMock,
    ):
        task.ingest()

        mock_swift_schedule_handler_cls.assert_called_once_with(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_name="low_fidelity_planned",
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
        )
