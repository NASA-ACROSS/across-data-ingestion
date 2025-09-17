import os
from unittest.mock import MagicMock

import pytest

import across_data_ingestion.tasks.schedules.tess.low_fidelity_planned as task
from across_data_ingestion.util.across_server import sdk

from . import mocks

MOCKS_FILE_BASE_PATH = os.path.join(os.path.dirname(__file__), "mocks")
FAKE_POINTING_FILENAME = "TESS_pointings.csv"
FAKE_ORBIT_TIMES_FILENAME = "TESS_orbit_times.csv"

type PointingFile = str
type OrbitTimesFile = str

MOCK_DATA_DIRS = ["placeholder_observations", "orbit_observations"]
SCHEDULE_FIELDS = [
    field for field in sdk.ScheduleCreate.model_fields.keys() if field != "observations"
]
OBSERVATION_FIELDS = [field for field in sdk.ObservationCreate.model_fields.keys()]


def fake_data_paths(dir: str) -> tuple[PointingFile, OrbitTimesFile]:
    return (
        f"{MOCKS_FILE_BASE_PATH}/{dir}/{FAKE_POINTING_FILENAME}",
        f"{MOCKS_FILE_BASE_PATH}/{dir}/{FAKE_ORBIT_TIMES_FILENAME}",
    )


def schedule_indices(
    schedules: list[sdk.ScheduleCreate],
) -> list[int]:
    return [idx for idx, _ in enumerate(schedules)]


def observation_indices(
    schedules: list[sdk.ScheduleCreate],
) -> list[tuple[int, int]]:
    return [
        (sched_idx, obs_idx)
        for sched_idx, schedule in enumerate(schedules)
        for obs_idx, _ in enumerate(schedule.observations)
    ]


ORBIT_SCHEDULES = schedule_indices(
    mocks.orbit_observations.ACROSS_schedule_output.expected
)
ORBIT_OBSERVATIONS = observation_indices(
    mocks.orbit_observations.ACROSS_schedule_output.expected
)
PLACEHOLDER_SCHEDULES = schedule_indices(
    mocks.placeholder_observations.ACROSS_schedule_output.expected
)
PLACEHOLDER_OBSERVATIONS = observation_indices(
    mocks.placeholder_observations.ACROSS_schedule_output.expected
)


def override_csv_paths(monkeypatch: pytest.MonkeyPatch, mock_data_dir: str):
    pointing_path, orbit_path = fake_data_paths(mock_data_dir)
    monkeypatch.setattr(task, "TESS_POINTINGS_FILE", pointing_path)
    monkeypatch.setattr(task, "TESS_ORBIT_TIMES_FILE", orbit_path)


class Test:
    @pytest.fixture(autouse=True)
    def override(self, monkeypatch: pytest.MonkeyPatch, mock_data_dir: str):
        override_csv_paths(monkeypatch, mock_data_dir)

    @pytest.mark.parametrize(("mock_data_dir"), MOCK_DATA_DIRS)
    class TestIngest:
        def test_should_read_pointings_files(
            self,
            mock_pandas: MagicMock,
            mock_data_dir: str,
        ):
            task.ingest()

            file = mock_pandas.read_csv.call_args_list[0].args[0]

            expected_file = fake_data_paths(mock_data_dir)[0]

            assert file == expected_file

        def test_should_read_orbit_times_files(
            self,
            mock_pandas: MagicMock,
            mock_data_dir: str,
        ):
            task.ingest()

            file = mock_pandas.read_csv.call_args_list[1].args[0]

            expected_file = fake_data_paths(mock_data_dir)[1]

            assert file == expected_file

        def test_should_call_get_telescopes(self, mock_telescope_api: MagicMock):
            task.ingest()

            mock_telescope_api.get_telescopes.assert_called_once()

        def test_should_create_many_schedules(self, mock_schedule_api: MagicMock):
            task.ingest()

            mock_schedule_api.create_many_schedules.assert_called_once()

        def test_should_create_expected_number_of_schedules(
            self,
            mock_schedule_api: MagicMock,
            fake_create_many_schedules: dict[str, sdk.ScheduleCreateMany],
            mock_data_dir: str,
        ):
            task.ingest()

            create_many = mock_schedule_api.create_many_schedules.call_args[0][0]

            assert len(create_many.schedules) == len(
                fake_create_many_schedules[mock_data_dir].schedules
            )

    @pytest.mark.parametrize("mock_data_dir", ["placeholder_observations"])
    class TestIngestPlaceholder:
        def test_should_use_schedule_date_range_when_creating_observation(
            self,
            mock_schedule_api: MagicMock,
        ):
            task.ingest()

            create_many: sdk.ScheduleCreateMany = (
                mock_schedule_api.create_many_schedules.call_args[0][0]
            )

            placeholder_schedule = create_many.schedules[0]
            placeholder_obs = placeholder_schedule.observations[0]

            assert placeholder_obs.date_range == placeholder_schedule.date_range


@pytest.mark.parametrize(
    "schedule_idx",
    schedule_indices(mocks.placeholder_observations.ACROSS_schedule_output.expected),
)
@pytest.mark.parametrize("field", SCHEDULE_FIELDS)
def test_should_create_schedule_with_expected_fields_for_placeholder(
    monkeypatch: pytest.MonkeyPatch,
    schedule_idx: int,
    field: str,
    mock_schedule_api: MagicMock,
    fake_create_many_schedules: dict[str, sdk.ScheduleCreateMany],
):
    """Test should ensure all schedules are created correctly with the correct data"""

    override_csv_paths(monkeypatch, "placeholder_observations")

    task.ingest()

    create_many: sdk.ScheduleCreateMany = (
        mock_schedule_api.create_many_schedules.call_args[0][0]
    )

    got_schedule = create_many.schedules[schedule_idx]

    expected: sdk.ScheduleCreateMany = fake_create_many_schedules[
        "placeholder_observations"
    ]
    expected_schedule = expected.schedules[schedule_idx]

    assert getattr(got_schedule, field) == getattr(expected_schedule, field)


@pytest.mark.parametrize("schedule_idx", ORBIT_SCHEDULES)
@pytest.mark.parametrize("field", SCHEDULE_FIELDS)
def test_should_create_schedule_with_expected_fields_for_orbit(
    monkeypatch: pytest.MonkeyPatch,
    schedule_idx: int,
    field: str,
    mock_schedule_api: MagicMock,
    fake_create_many_schedules: dict[str, sdk.ScheduleCreateMany],
):
    """Test should ensure all schedules are created correctly with the correct data"""

    override_csv_paths(monkeypatch, "orbit_observations")

    task.ingest()

    create_many: sdk.ScheduleCreateMany = (
        mock_schedule_api.create_many_schedules.call_args[0][0]
    )

    got_schedule = create_many.schedules[schedule_idx]

    expected: sdk.ScheduleCreateMany = fake_create_many_schedules["orbit_observations"]
    expected_schedule = expected.schedules[schedule_idx]

    assert getattr(got_schedule, field) == getattr(expected_schedule, field)


@pytest.mark.parametrize("sched_idx,obs_idx", PLACEHOLDER_OBSERVATIONS)
@pytest.mark.parametrize("field", OBSERVATION_FIELDS)
def test_should_create_observations_with_expected_fields_for_placeholder(
    monkeypatch: pytest.MonkeyPatch,
    sched_idx: int,
    obs_idx: int,
    field: str,
    mock_schedule_api: MagicMock,
    fake_create_many_schedules: dict[str, sdk.ScheduleCreateMany],
):
    """Test should ensure all schedules are created correctly with the correct data"""

    dir = "placeholder_observations"

    override_csv_paths(monkeypatch, dir)

    task.ingest()

    create_many: sdk.ScheduleCreateMany = (
        mock_schedule_api.create_many_schedules.call_args[0][0]
    )

    expected: sdk.ScheduleCreateMany = fake_create_many_schedules[dir]

    got_schedule = create_many.schedules[sched_idx].observations[obs_idx]
    expected_schedule = expected.schedules[sched_idx].observations[obs_idx]

    assert getattr(got_schedule, field) == getattr(expected_schedule, field)


@pytest.mark.parametrize("sched_idx,obs_idx", ORBIT_OBSERVATIONS)
@pytest.mark.parametrize("field", OBSERVATION_FIELDS)
def test_should_create_observations_with_expected_fields_for_orbit(
    monkeypatch: pytest.MonkeyPatch,
    sched_idx: int,
    obs_idx: int,
    field: str,
    mock_schedule_api: MagicMock,
    fake_create_many_schedules: dict[str, sdk.ScheduleCreateMany],
):
    """Test should ensure all schedules are created correctly with the correct data"""

    dir = "orbit_observations"

    override_csv_paths(monkeypatch, dir)

    task.ingest()

    create_many: sdk.ScheduleCreateMany = (
        mock_schedule_api.create_many_schedules.call_args[0][0]
    )

    expected: sdk.ScheduleCreateMany = fake_create_many_schedules[dir]

    got_schedule = create_many.schedules[sched_idx].observations[obs_idx]
    expected_schedule = expected.schedules[sched_idx].observations[obs_idx]

    assert getattr(got_schedule, field) == getattr(expected_schedule, field)
