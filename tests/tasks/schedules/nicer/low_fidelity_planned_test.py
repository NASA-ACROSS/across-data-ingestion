import os
from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.schedules.nicer.low_fidelity_planned as task
from across_data_ingestion.util.across_server import sdk

from .mocks.across_nicer_schedule import expected


def get_mock_path(file: str = "") -> str:
    return os.path.join(os.path.dirname(__file__), "mocks/", file)


MOCK_NICER_CSV = "nicer_scheduled_observations.csv"


@pytest.fixture(autouse=True)
def set_mock_csv_files(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        task,
        "NICER_TIMELINE_FILE",
        get_mock_path(MOCK_NICER_CSV),
    )


@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # must be patched because it is set at runtime when the file is imported.
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)
    return mock


class TestIngest:
    def test_should_read_nicer_data(self, mock_pandas: MagicMock):
        task.ingest()

        mock_pandas.read_csv.assert_called_once()

    def test_should_log_warning_when_all_observations_are_filtered(
        self, monkeypatch: pytest.MonkeyPatch, mock_logger: MagicMock
    ) -> None:
        monkeypatch.setattr(
            task, "NICER_TIMELINE_FILE", get_mock_path("no_scheduled.csv")
        )

        task.ingest()

        mock_logger.warning.assert_called_once()

    def test_should_get_telescope(self, mock_telescope_api: MagicMock) -> None:
        task.ingest()

        mock_telescope_api.get_telescopes.assert_called_once()

    def test_should_create_schedule(self, mock_schedule_api: MagicMock) -> None:
        task.ingest()

        mock_schedule_api.create_schedule.assert_called_once()

    @pytest.mark.parametrize(
        "field",
        [
            field
            for field in sdk.ScheduleCreate.model_fields.keys()
            if field != "observations"
        ],
    )
    def test_should_create_schedule_with_expected_fields(
        self, mock_schedule_api: MagicMock, field: str
    ) -> None:
        task.ingest()

        created_schedule: sdk.ScheduleCreate = (
            mock_schedule_api.create_schedule.call_args[0][0]
        )

        assert getattr(created_schedule, field) == getattr(expected, field)

    @pytest.mark.parametrize("field", sdk.ObservationCreate.model_fields)
    def test_should_create_observation_with_expected_fields(
        self,
        mock_schedule_api: MagicMock,
        field: str,
    ) -> None:
        task.ingest()

        created_schedule: sdk.ScheduleCreate = (
            mock_schedule_api.create_schedule.call_args[0][0]
        )

        created_obs = created_schedule.observations[0]
        expected_obs = expected.observations[0]

        assert getattr(created_obs, field) == getattr(expected_obs, field)
