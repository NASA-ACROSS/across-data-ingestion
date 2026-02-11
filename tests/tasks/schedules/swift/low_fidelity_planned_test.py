from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.schedules.swift.low_fidelity_planned as task
import across_data_ingestion.tasks.schedules.swift.util as util
from across_data_ingestion.util.across_server import sdk

from .mocks import swift_low_fidelity_planned_schedule as expected_schedules


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
    @pytest.fixture
    def mock_create_swift_across_schedule(self, monkeypatch: pytest.MonkeyPatch):
        mock = MagicMock(side_effect=util.create_swift_across_schedule)
        monkeypatch.setattr(util, "create_swift_across_schedule", mock)
        return mock

    def test_should_log_warning_when_no_swift_plan(
        self, monkeypatch: pytest.MonkeyPatch, mock_logger: MagicMock
    ):
        monkeypatch.setattr(task, "query_swift_plan", MagicMock(return_value=[]))

        task.ingest()

        mock_logger.warning.assert_called_once()

    @pytest.mark.parametrize(
        "telescope_name, call_idx",
        [("swift_xrt", 0), ("swift_bat", 1), ("swift_uvot", 2)],
    )
    def test_should_transform_swift_plan_to_across_schedule(
        self,
        telescope_name: str,
        call_idx: int,
        mock_create_swift_across_schedule: MagicMock,
    ):
        task.ingest()
        call = mock_create_swift_across_schedule.call_args_list[call_idx]

        assert call.kwargs["telescope_name"] == telescope_name

    @pytest.mark.parametrize(
        "obs_type, call_idx",
        [
            (sdk.ObservationType.SPECTROSCOPY, 0),
            (sdk.ObservationType.IMAGING, 1),
            (sdk.ObservationType.IMAGING, 2),
        ],
    )
    def test_should_use_expected_observation_type_for_each_telescope(
        self,
        obs_type: sdk.ObservationType,
        call_idx: int,
        mock_create_swift_across_schedule: MagicMock,
    ):
        task.ingest()
        call = mock_create_swift_across_schedule.call_args_list[call_idx]

        assert call.kwargs["observation_type"] == obs_type

    @pytest.mark.parametrize(
        "type, expected_schedule, call_idx",
        [
            ("xrt", expected_schedules.expected_xrt, 0),
            ("bat", expected_schedules.expected_bat, 1),
            ("uvot", expected_schedules.expected_uvot, 2),
        ],
    )
    def test_should_transform_to_expected_schedule_by_telescope(
        self,
        type: str,
        expected_schedule: sdk.ScheduleCreate,
        call_idx: int,
        mock_schedule_api: MagicMock,
    ):
        task.ingest()
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_sched = call.args[0]

        assert created_sched == expected_schedule

    @pytest.mark.parametrize(
        "type, expected_obs, call_idx",
        [
            ("xrt", expected_schedules.expected_xrt.observations[0], 0),
            ("bat", expected_schedules.expected_bat.observations[0], 1),
            ("uvot", expected_schedules.expected_uvot.observations[0], 2),
        ],
    )
    def test_should_transform_to_expected_observation_by_telescope(
        self,
        type: str,
        expected_obs: sdk.ObservationCreate,
        call_idx: int,
        mock_schedule_api: MagicMock,
    ):
        task.ingest()
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_obs = call.args[0].observations[0]

        assert created_obs == expected_obs
