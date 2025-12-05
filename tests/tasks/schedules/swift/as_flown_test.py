from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.schedules.swift.as_flown as task
from across_data_ingestion.util.across_server import sdk

from .mocks import swift_as_flown_schedule as expected_schedules


@pytest.fixture(autouse=True)
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)

    return mock


class TestSwiftObsQuery:
    def test_should_query_swift_for_plan(self, mock_swift_too: MagicMock):
        task.query_swift_as_flown()

        mock_swift_too.ObsQuery.assert_called_once()

    def test_should_filter_out_saa_uvot_modes(self):
        entries = task.query_swift_as_flown()

        ## one saa uvot mode exists in fake test data
        assert len(entries) == 10

    def test_should_return_empty_array_when_no_obs_entries(
        self, mock_swift_too: MagicMock
    ):
        mock_swift_too.ObsQuery.return_value = []

        entries = task.query_swift_as_flown()

        assert not entries


class TestIngest:
    def test_should_log_warning_when_no_swift_plan(
        self, monkeypatch: pytest.MonkeyPatch, mock_logger: MagicMock
    ):
        monkeypatch.setattr(task, "query_swift_as_flown", MagicMock(return_value=[]))

        task.ingest()

        mock_logger.warning.assert_called_once()

    @pytest.mark.parametrize(
        "type, expected_schedule, call_idx",
        [
            ("xrt", expected_schedules.expected_xrt, 0),
            ("bat", expected_schedules.expected_bat, 1),
            ("uvot", expected_schedules.expected_uvot, 2),
        ],
    )
    @pytest.mark.parametrize(
        "field",
        [
            field
            for field in sdk.ScheduleCreate.model_fields.keys()
            if field != "observations"
        ],
    )
    def test_should_transform_to_expected_schedule_by_telescope(
        self,
        type: str,
        expected_schedule: sdk.ScheduleCreate,
        field: str,
        call_idx: int,
        mock_schedule_api: MagicMock,
    ):
        task.ingest()
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_sched = call.args[0]

        assert getattr(created_sched, field) == getattr(expected_schedule, field)

    @pytest.mark.parametrize(
        "type, expected_obs, call_idx",
        [
            ("xrt", expected_schedules.expected_xrt.observations[0], 0),
            ("bat", expected_schedules.expected_bat.observations[0], 1),
            ("uvot", expected_schedules.expected_uvot.observations[0], 2),
        ],
    )
    @pytest.mark.parametrize("field", sdk.ObservationCreate.model_fields)
    def test_should_transform_to_expected_observation_by_telescope(
        self,
        type: str,
        expected_obs: sdk.ObservationCreate,
        field: str,
        call_idx: int,
        mock_schedule_api: MagicMock,
    ):
        task.ingest()
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_obs = call.args[0].observations[0]

        assert getattr(created_obs, field) == getattr(expected_obs, field)
