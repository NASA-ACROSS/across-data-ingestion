from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.schedules.swift.low_fidelity_planned as task
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

    def test_should_filter_out_saa_uvot_modes(self):
        entries = task.query_swift_plan()

        ## one saa uvot mode exists in fake test data
        assert len(entries) == 10

    def test_should_return_empty_array_when_no_obs_entries(
        self, mock_swift_too: MagicMock
    ):
        mock_swift_too.PlanQuery.return_value = []

        entries = task.query_swift_plan()

        assert not entries


class TestBuildUVOTModeDict:
    def test_should_return_mode_dict_of_CustomUVOTModeEntry(self):
        mode = "0x30ed"
        uvot_mode_dict = task.build_uvot_mode_dict([mode])

        assert isinstance(uvot_mode_dict[mode][0], task.CustomUVOTModeEntry)

    def test_should_return_empty_dict_when_no_entries(self):
        dict = task.build_uvot_mode_dict(["dne"])

        assert len(dict) == 0


class TestCreateUVOTObservations:
    def test_should_return_list_of_across_uvot_observations(
        self, fake_swift_obs_entries: list[task.CustomSwiftObsEntry]
    ):
        observations = task.create_uvot_observations(
            instrument_id="instrument-id",
            observation_data=fake_swift_obs_entries,
        )

        assert isinstance(observations[0], sdk.ObservationCreate)

    def test_should_log_warning_when_obs_not_created_for_unmatched_filter(
        self,
        fake_swift_obs_entries: list[task.CustomSwiftObsEntry],
        fake_uvot_mode_entries: dict[str, list[dict]],
        mock_logger: MagicMock,
    ):
        # make the filters unmatchable for the first observation
        uvot = fake_swift_obs_entries[0].uvot
        filters = fake_uvot_mode_entries[uvot]
        filters[0]["filter_name"] = "unknown_filter"

        task.create_uvot_observations(
            instrument_id="instrument-id",
            observation_data=fake_swift_obs_entries,
        )

        mock_logger.warning.assert_called_once_with(
            "No observation will be created for the unmatched filter.",
            uvot=uvot,
            filter="unknown_filter",
        )

    def test_should_continue_creating_observations_for_all_matched_filters(
        self,
        fake_swift_obs_entries: list[task.CustomSwiftObsEntry],
        fake_uvot_mode_entries: dict[str, list[dict]],
    ):
        # make the filters unmatchable for the first observation
        # 0x308f uvot (only test obs with this uvot)
        uvot = fake_swift_obs_entries[0].uvot
        filters = fake_uvot_mode_entries[uvot]
        filters[0]["filter_name"] = "unknown_filter"

        obs = task.create_uvot_observations(
            instrument_id="instrument-id",
            observation_data=fake_swift_obs_entries,
        )

        expected_length = len(expected_schedules.expected_uvot.observations)

        assert len(obs) == expected_length - 1


class TestCreateAcrossSchedule:
    def test_should_call_telescope_api_when_getting_telescope_data(
        self,
        mock_telescope_api: MagicMock,
        fake_swift_obs_entries: list[task.CustomSwiftObsEntry],
    ):
        task.create_swift_across_schedule(
            telescope_name="some_telescope",
            observation_data=fake_swift_obs_entries,
            create_observations=task.create_observations,
            observation_type=sdk.ObservationType.IMAGING,
            bandpass=sdk.Bandpass(task.SWIFT_XRT_BANDPASS),
        )

        mock_telescope_api.get_telescopes.assert_called_once()

    def test_should_create_across_schedule(
        self,
        fake_swift_obs_entries: list[task.CustomSwiftObsEntry],
    ):
        schedule = task.create_swift_across_schedule(
            telescope_name="some_telescope",
            observation_data=fake_swift_obs_entries,
            create_observations=task.create_observations,
            observation_type=sdk.ObservationType.IMAGING,
            bandpass=sdk.Bandpass(task.SWIFT_XRT_BANDPASS),
        )

        assert isinstance(schedule, sdk.ScheduleCreate)

    def test_should_create_observations_with_the_strategy(
        self,
        fake_swift_obs_entries: list[task.CustomSwiftObsEntry],
    ):
        mock_create_observations = MagicMock(return_value=[])

        task.create_swift_across_schedule(
            telescope_name="some_telescope",
            observation_data=fake_swift_obs_entries,
            create_observations=mock_create_observations,
            observation_type=sdk.ObservationType.IMAGING,
            bandpass=sdk.Bandpass(task.SWIFT_XRT_BANDPASS),
        )

        mock_create_observations.assert_called_once()


class TestIngest:
    @pytest.fixture
    def mock_create_swift_across_schedule(self, monkeypatch: pytest.MonkeyPatch):
        mock = MagicMock(side_effect=task.create_swift_across_schedule)
        monkeypatch.setattr(task, "create_swift_across_schedule", mock)
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
