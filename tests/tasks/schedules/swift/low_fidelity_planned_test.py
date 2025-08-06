from datetime import timedelta
from unittest.mock import patch

from across_data_ingestion.tasks.schedules.swift.low_fidelity_planned import (
    CustomSwiftObsEntry,
    CustomUVOTModeEntry,
    entrypoint,
    ingest,
    query_swift_plan,
    swift_to_across_schedule,
    swift_uvot_mode_dict,
)

from .mocks.swift_10_rows_of_plan import swift_10_rows_plan
from .mocks.swift_low_fidelity_planned_schedule import (
    swift_low_fidelity_planned_schedule,
)
from .mocks.uvot_mode_entries import uvot_modes_entries


class CustomSwiftUVOTMode:
    """
    Custom Swift entry for testing purposes.
    """

    entries: list[CustomUVOTModeEntry]

    def __init__(self, entries: list[CustomUVOTModeEntry]):
        self.entries = entries
        pass


def mock_swift_data(
    exposure_time_as_timedelta: bool = False,
) -> list[CustomSwiftObsEntry]:
    """
    Gets the mock swift plan
    """
    ret = []
    for entry in swift_10_rows_plan:
        if exposure_time_as_timedelta:
            entry["exposure"] = timedelta(seconds=entry["exposure"])
        ret.append(CustomSwiftObsEntry(**entry))
    return ret


def mock_swift_uvot_modes() -> dict[str, list[CustomUVOTModeEntry]]:
    """
    Gets the mock swift uvot modes
    """
    data = uvot_modes_entries

    ret = {}
    for key, value in data.items():
        ret[key] = [CustomUVOTModeEntry(**entry) for entry in value]
    return ret


class TestSwiftLowFidelityScheduleIngestionTask:
    def test_should_generate_across_schedules(self):
        """Should generate ACROSS schedules"""

        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=mock_swift_data(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "swift_telescope_id",
                    "instruments": [{"id": "swift_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.swift_uvot_mode_dict",
            return_value=mock_swift_uvot_modes(),
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            schedules = ingest()
            assert schedules == swift_low_fidelity_planned_schedule

    def test_should_generate_observations_with_schedule(self):
        """Should generate list of observations with an ACROSS schedule"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=mock_swift_data(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "swift_telescope_id",
                    "instruments": [{"id": "swift_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.swift_uvot_mode_dict",
            return_value=mock_swift_uvot_modes(),
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            schedules = ingest()
            assert len(schedules[0]["observations"]) > 0

    def test_query_swift_plan_should_return_list_entries_when_successful(self):
        """Should return a list of custom entries if querying Swift catalog is successful"""
        with patch(
            "swifttools.swift_too.PlanQuery",
            return_value=mock_swift_data(exposure_time_as_timedelta=True),
        ):
            data = query_swift_plan()
            assert isinstance(data[0], CustomSwiftObsEntry)

    def test_query_swift_catalog_should_return_none_if_query_fails(self):
        """Should return None if querying Swift catalog fails"""
        with patch(
            "swifttools.swift_too.PlanQuery",
            return_value=None,
            side_effect=Exception("HTTP Error"),
        ):
            data = query_swift_plan()
            assert data is None

    def test_query_swift_catalog_should_filter_out_saa_obs(self):
        """Should return None if querying Swift catalog fails"""
        observation = CustomSwiftObsEntry()
        observation.uvot = "0x0009"
        mock_plan = [observation]
        with patch(
            "swifttools.swift_too.PlanQuery",
            return_value=mock_plan,
        ):
            data = query_swift_plan()
            assert data is None

    def test_create_schedule_should_return_expected(self):
        """Should return an expected schedule dictionary when given valid data"""
        mock_data = mock_swift_data()
        schedule = swift_to_across_schedule(
            "swift_telescope_id", "short_name", mock_data
        )
        expected_schedule = {
            "telescope_id": "swift_telescope_id",
            "name": "short_name_low_fidelity_planned_2025-07-01_2025-07-01",
            "date_range": {
                "begin": "2025-07-01T00:05:00.000",
                "end": "2025-07-01T02:56:00.000",
            },
            "status": "planned",
            "fidelity": "low",
        }
        assert schedule == expected_schedule

    def test_should_log_error_when_query_swift_catalog_returns_none(self):
        """Should log an error when Swift query returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=None,
        ):
            ingest()
            assert (
                "Failed to query Swift planned observations"
                in log_mock.warn.call_args.args[0]
            )

    def test_should_log_info_when_success(self):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=mock_swift_data(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "swift_telescope_id",
                    "instruments": [{"id": "swift_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.swift_uvot_mode_dict",
            return_value=mock_swift_uvot_modes(),
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.logger"
        ) as log_mock:
            entrypoint()
            assert (
                "Swift low fidelity planned schedule ingestion completed"
                in log_mock.info.call_args.args[0]
            )

    def test_should_log_error_when_schedule_ingestion_fails(self):
        """Should log an error when schedule ingestion fails"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.ingest",
            return_value=None,
            side_effect=Exception(),
        ):
            entrypoint()
            assert "encountered an error" in log_mock.error.call_args.args[0]

    def test_uvot_mode_dict_should_return_expected(self):
        """Should return a dictionary with UVOT modes"""
        with patch(
            "swifttools.swift_too.UVOTMode",
            return_value=CustomSwiftUVOTMode(
                entries=[CustomUVOTModeEntry(filter_name="v", weight=100)]
            ),
        ):
            expected = {"v": [CustomUVOTModeEntry(filter_name="v", weight=100)]}
            values = swift_uvot_mode_dict(modes=["v"])
            assert values == expected

    def test_uvot_mode_dict_should_return_empty_dict(self):
        """Should return a dictionary with UVOT modes"""
        with patch(
            "swifttools.swift_too.UVOTMode",
            return_value=CustomSwiftUVOTMode(entries=None),
        ):
            expected = {}
            values = swift_uvot_mode_dict(modes=["v"])
            assert values == expected

    def test_swift_uvot_should_skip_filter_names_not_in_dict(self):
        """Should skip UVOT modes not in the dictionary"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=mock_swift_data()[:1],
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=[
                {
                    "id": "swift_telescope_id",
                    "instruments": [{"id": "swift_instrument_id"}],
                }
            ],
        ), patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.swift_uvot_mode_dict",
            return_value={
                "0x308f": [
                    CustomUVOTModeEntry(filter_name="bad_filter_name", weight=100)
                ]
            },
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ), patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.logger"
        ) as log_mock:
            ingest()
            assert (
                "Skipping observation, filter not found"
                in log_mock.warn.call_args.args[0]
            )
