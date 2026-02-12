from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.schedules.swift.util as task_util
from across_data_ingestion.util.across_server import sdk

from .mocks import swift_as_flown_schedule as expected_as_flown_schedule
from .mocks import swift_low_fidelity_planned_schedule as expected_planned_schedule


@pytest.fixture(autouse=True)
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task_util, "logger", mock)

    return mock


class TestBuildUVOTModeDict:
    def test_should_return_mode_dict_of_CustomUVOTModeEntry(self):
        mode = "0x30ed"
        uvot_mode_dict = task_util.build_uvot_mode_dict([mode])

        assert isinstance(uvot_mode_dict[mode][0], task_util.CustomUVOTModeEntry)

    def test_should_return_empty_dict_when_no_entries(self):
        dict = task_util.build_uvot_mode_dict(["dne"])

        assert len(dict) == 0


class TestCreateUVOTObservations:
    def test_should_return_list_of_across_uvot_observations(
        self, fake_swift_obs_entries: list[task_util.SwiftObservationEntry]
    ):
        observations = task_util.create_uvot_observations(
            instrument_id="instrument-id",
            observation_data=fake_swift_obs_entries,
            observation_status=sdk.ObservationStatus.PLANNED,
        )

        assert isinstance(observations[0], sdk.ObservationCreate)

    def test_should_log_warning_when_obs_not_created_for_unmatched_filter(
        self,
        fake_swift_obs_entries: list[task_util.SwiftObservationEntry],
        fake_uvot_mode_entries: dict[str, list[dict]],
        mock_logger: MagicMock,
    ):
        # make the filters unmatchable for the first observation
        uvot = fake_swift_obs_entries[0].uvot
        filters = fake_uvot_mode_entries[uvot]
        filters[0]["filter_name"] = "unknown_filter"

        task_util.create_uvot_observations(
            instrument_id="instrument-id",
            observation_data=fake_swift_obs_entries,
            observation_status=sdk.ObservationStatus.PLANNED,
        )

        mock_logger.warning.assert_called_once_with(
            "No observation will be created for the unmatched filter.",
            uvot=uvot,
            filter="unknown_filter",
        )


class TestCreateAcrossSchedule:
    def test_should_return_true_for_saa_uvot_mode_when_observation_in_saa(self):
        obs_entry = task_util.SwiftObservationEntry(
            obsid="3676767",
            targname="saa-cold-test",
            ra=202.484375,
            dec=47.2305555555556,
            begin=None,
            end=None,
            uvot="0x0009",
            xrt_mode=None,
            bat_mode=None,
            target_id=3676767,
        )

        assert task_util.observation_in_saa(obs_entry)

    def test_should_call_telescope_api_when_getting_telescope_data(
        self,
        mock_telescope_api: MagicMock,
        fake_swift_obs_entries: list[task_util.SwiftObservationEntry],
    ):
        task_util.create_swift_across_schedule(
            telescope_name="some_telescope",
            observation_data=fake_swift_obs_entries,
            observation_status=sdk.ObservationStatus.PLANNED,
            create_observations=task_util.create_observations,
            observation_type=sdk.ObservationType.IMAGING,
            bandpass=sdk.Bandpass(task_util.SWIFT_XRT_BANDPASS),
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
            schedule_name="low_fidelity_planned",
        )

        mock_telescope_api.get_telescopes.assert_called_once()

    def test_should_create_across_schedule(
        self,
        fake_swift_obs_entries: list[task_util.SwiftObservationEntry],
    ):
        schedule = task_util.create_swift_across_schedule(
            telescope_name="some_telescope",
            observation_data=fake_swift_obs_entries,
            observation_status=sdk.ObservationStatus.PLANNED,
            create_observations=task_util.create_observations,
            observation_type=sdk.ObservationType.IMAGING,
            bandpass=sdk.Bandpass(task_util.SWIFT_XRT_BANDPASS),
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
            schedule_name="low_fidelity_planned",
        )

        assert isinstance(schedule, sdk.ScheduleCreate)

    @pytest.mark.parametrize(
        "type, expected_schedule, call_idx",
        [
            ("xrt", expected_planned_schedule.expected_xrt, 0),
            ("bat", expected_planned_schedule.expected_bat, 1),
            ("uvot", expected_planned_schedule.expected_uvot, 2),
        ],
    )
    def test_should_transform_to_expected_planned_schedule_by_telescope(
        self,
        type: str,
        expected_schedule: sdk.ScheduleCreate,
        call_idx: int,
        mock_schedule_api: MagicMock,
        fake_swift_obs_entries: list[task_util.SwiftObservationEntry],
    ):
        handler = task_util.SwiftScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
            schedule_name="low_fidelity_planned",
        )
        non_saa_obs_entries = [
            obs
            for obs in fake_swift_obs_entries
            if not task_util.observation_in_saa(obs)
        ]
        handler.run(observation_data=non_saa_obs_entries)
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_sched = call.args[0]

        assert created_sched == expected_schedule

    @pytest.mark.parametrize(
        "type, expected_obs, call_idx",
        [
            ("xrt", expected_planned_schedule.expected_xrt.observations[0], 0),
            ("bat", expected_planned_schedule.expected_bat.observations[0], 1),
            ("uvot", expected_planned_schedule.expected_uvot.observations[0], 2),
        ],
    )
    def test_should_transform_to_expected_planned_observation_by_telescope(
        self,
        type: str,
        expected_obs: sdk.ObservationCreate,
        call_idx: int,
        mock_schedule_api: MagicMock,
        fake_swift_obs_entries: list[task_util.SwiftObservationEntry],
    ):
        handler = task_util.SwiftScheduleHandler(
            observation_status=sdk.ObservationStatus.PLANNED,
            schedule_status=sdk.ScheduleStatus.PLANNED,
            schedule_fidelity=sdk.ScheduleFidelity.LOW,
            schedule_name="low_fidelity_planned",
        )
        non_saa_obs_entries = [
            obs
            for obs in fake_swift_obs_entries
            if not task_util.observation_in_saa(obs)
        ]
        handler.run(observation_data=non_saa_obs_entries)
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_obs = call.args[0].observations[0]

        assert created_obs == expected_obs

    @pytest.mark.parametrize(
        "type, expected_schedule, call_idx",
        [
            ("xrt", expected_as_flown_schedule.expected_xrt, 0),
            ("bat", expected_as_flown_schedule.expected_bat, 1),
            ("uvot", expected_as_flown_schedule.expected_uvot, 2),
        ],
    )
    def test_should_transform_to_expected_as_flown_schedule_by_telescope(
        self,
        type: str,
        expected_schedule: sdk.ScheduleCreate,
        call_idx: int,
        mock_schedule_api: MagicMock,
        fake_swift_obs_entries: list[task_util.SwiftObservationEntry],
    ):
        handler = task_util.SwiftScheduleHandler(
            observation_status=sdk.ObservationStatus.PERFORMED,
            schedule_status=sdk.ScheduleStatus.PERFORMED,
            schedule_fidelity=sdk.ScheduleFidelity.HIGH,
            schedule_name="as_flown",
        )
        non_saa_obs_entries = [
            obs
            for obs in fake_swift_obs_entries
            if not task_util.observation_in_saa(obs)
        ]
        handler.run(observation_data=non_saa_obs_entries)
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_sched = call.args[0]

        assert created_sched == expected_schedule

    @pytest.mark.parametrize(
        "type, expected_obs, call_idx",
        [
            ("xrt", expected_as_flown_schedule.expected_xrt.observations[0], 0),
            ("bat", expected_as_flown_schedule.expected_bat.observations[0], 1),
            ("uvot", expected_as_flown_schedule.expected_uvot.observations[0], 2),
        ],
    )
    def test_should_transform_to_expected_as_flown_observation_by_telescope(
        self,
        type: str,
        expected_obs: sdk.ObservationCreate,
        call_idx: int,
        mock_schedule_api: MagicMock,
        fake_swift_obs_entries: list[task_util.SwiftObservationEntry],
    ):
        handler = task_util.SwiftScheduleHandler(
            observation_status=sdk.ObservationStatus.PERFORMED,
            schedule_status=sdk.ScheduleStatus.PERFORMED,
            schedule_fidelity=sdk.ScheduleFidelity.HIGH,
            schedule_name="as_flown",
        )
        non_saa_obs_entries = [
            obs
            for obs in fake_swift_obs_entries
            if not task_util.observation_in_saa(obs)
        ]
        handler.run(observation_data=non_saa_obs_entries)
        call = mock_schedule_api.create_schedule.call_args_list[call_idx]
        created_obs = call.args[0].observations[0]

        assert created_obs == expected_obs
