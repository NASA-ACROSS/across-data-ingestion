from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.schedules.swift.util as task_util
from across_data_ingestion.util.across_server import sdk

from .mocks import swift_low_fidelity_planned_schedule as expected_schedules


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
        self, fake_swift_obs_entries: list[task_util.CustomSwiftObsEntry]
    ):
        observations = task_util.create_uvot_observations(
            instrument_id="instrument-id",
            observation_data=fake_swift_obs_entries,
            observation_status=sdk.ObservationStatus.PLANNED,
        )

        assert isinstance(observations[0], sdk.ObservationCreate)

    def test_should_log_warning_when_obs_not_created_for_unmatched_filter(
        self,
        fake_swift_obs_entries: list[task_util.CustomSwiftObsEntry],
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

    def test_should_continue_creating_observations_for_all_matched_filters(
        self,
        fake_swift_obs_entries: list[task_util.CustomSwiftObsEntry],
        fake_uvot_mode_entries: dict[str, list[dict]],
    ):
        # make the filters unmatchable for the first observation
        # 0x308f uvot (only test obs with this uvot)
        uvot = fake_swift_obs_entries[0].uvot
        filters = fake_uvot_mode_entries[uvot]
        filters[0]["filter_name"] = "unknown_filter"

        obs = task_util.create_uvot_observations(
            instrument_id="instrument-id",
            observation_data=fake_swift_obs_entries,
            observation_status=sdk.ObservationStatus.PLANNED,
        )

        expected_length = len(expected_schedules.expected_uvot.observations)

        assert len(obs) == expected_length - 1


class TestCreateAcrossSchedule:
    def test_should_call_telescope_api_when_getting_telescope_data(
        self,
        mock_telescope_api: MagicMock,
        fake_swift_obs_entries: list[task_util.CustomSwiftObsEntry],
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
            schedule_name_attr="low_fidelity_planned",
        )

        mock_telescope_api.get_telescopes.assert_called_once()

    def test_should_create_across_schedule(
        self,
        fake_swift_obs_entries: list[task_util.CustomSwiftObsEntry],
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
            schedule_name_attr="low_fidelity_planned",
        )

        assert isinstance(schedule, sdk.ScheduleCreate)

    # def test_should_create_observations_with_the_strategy(
    #     self,
    #     fake_swift_obs_entries: list[task_util.CustomSwiftObsEntry],
    # ):
    #     mock_create_observations = MagicMock(return_value=[])

    #     task_util.create_swift_across_schedule(
    #         telescope_name="some_telescope",
    #         observation_data=fake_swift_obs_entries,
    #         observation_status=sdk.ObservationStatus.PLANNED,
    #         create_observations=task_util.create_observations,
    #         observation_type=sdk.ObservationType.IMAGING,
    #         bandpass=sdk.Bandpass(task_util.SWIFT_XRT_BANDPASS),
    #         schedule_status=sdk.ScheduleStatus.PLANNED,
    #         schedule_fidelity=sdk.ScheduleFidelity.LOW,
    #         schedule_name_attr="low_fidelity_planned"
    #     )

    #     mock_create_observations.assert_called_once()
