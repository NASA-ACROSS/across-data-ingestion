import json
import os
from unittest.mock import patch

from across_data_ingestion.tasks.schedules.swift.low_fidelity_planned import (
    CustomSwiftEntry,
    CustomUVOTModeEntry,
    entrypoint,
    ingest,
    query_swift_plan,
    swift_schedule,
    swift_uvot_mode_dict,
)


class CustomSwiftUVOTMode:
    """
    Custom Swift entry for testing purposes.
    """

    entries: list[CustomUVOTModeEntry]

    def __init__(self, entries: list[CustomUVOTModeEntry]):
        self.entries = entries
        pass


def read_test_swift_data(file_path: str) -> list[CustomSwiftEntry]:
    """
    Helper function to read test data from a json file.
    """

    with open(file_path, "r") as file:
        data = json.load(file)

    return [CustomSwiftEntry(**entry) for entry in data]


def read_test_uvot_data(file_path: str) -> dict[str, list[CustomUVOTModeEntry]]:
    """
    Helper function to read test UVOT data from a json file.
    """
    ret = {}
    with open(file_path, "r") as file:
        data = json.load(file)

    for key, value in data.items():
        ret[key] = [CustomUVOTModeEntry(**entry) for entry in value]
    return ret


class TestSwiftLowFidelityScheduleIngestionTask:
    mock_file_base_path = os.path.join(os.path.dirname(__file__), "mocks/")
    mock_observation_table = "swift_10_rows_of_plan.json"
    mock_schedule_output = "swift_low_fidelity_planned_schedule.json"
    mock_file_uvot_output = "uvot_mode_entries.json"

    def test_should_generate_across_schedules(self):
        """Should generate ACROSS schedules"""
        mock_output_schedule_file = self.mock_file_base_path + self.mock_schedule_output

        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=read_test_swift_data(
                self.mock_file_base_path + self.mock_observation_table
            ),
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
            return_value=read_test_uvot_data(
                self.mock_file_base_path + self.mock_file_uvot_output
            ),
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            schedules = ingest()
            with open(mock_output_schedule_file) as expected_output_file:
                expected = json.load(expected_output_file)
                assert schedules == expected

    def test_should_generate_observations_with_schedule(self):
        """Should generate list of observations with an ACROSS schedule"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=read_test_swift_data(
                self.mock_file_base_path + self.mock_observation_table
            ),
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
            return_value=read_test_uvot_data(
                self.mock_file_base_path + self.mock_file_uvot_output
            ),
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            schedules = ingest()
            assert len(schedules[0]["observations"]) > 0

    def test_query_swift_plan_should_return_list_entries_when_successful(self):
        """Should return a list of custom entries if querying Swift catalog is successful"""
        with patch(
            "swifttools.swift_too.PlanQuery",
            return_value=read_test_swift_data(
                self.mock_file_base_path + self.mock_observation_table
            ),
        ):
            data = query_swift_plan()
            assert isinstance(data[0], CustomSwiftEntry)

    def test_query_swift_catalog_should_return_none_if_query_fails(self):
        """Should return None if querying Swift catalog fails"""
        with patch(
            "swifttools.swift_too.PlanQuery",
            return_value=None,
            side_effect=Exception("HTTP Error"),
        ):
            data = query_swift_plan()
            assert data is None

    def test_create_schedule_should_return_expected(self):
        """Should return an expected schedule dictionary when given valid data"""
        mock_data = read_test_swift_data(
            self.mock_file_base_path + self.mock_observation_table
        )
        schedule = swift_schedule("swift_telescope_id", "short_name", mock_data)
        expected_schedule = {
            "telescope_id": "swift_telescope_id",
            "name": "swift_short_name_low_fidelity_planned_2025-07-01_2025-07-01",
            "date_range": {
                "begin": "2025-07-01T00:05:00.000",
                "end": "2025-07-01T02:56:00.000",
            },
            "status": "planned",
            "fidelity": "low",
        }
        assert schedule == expected_schedule

    def test_create_schedule_should_return_empty_dict_when_given_empty_table(self):
        """Should return an empty dictionary when the input table is empty"""
        # Ingest a table with no rows
        mock_data = []
        schedule = swift_schedule("swift_telescope_id", "short_name", mock_data)
        assert schedule == {}

    def test_should_log_error_when_query_nicer_catalog_returns_none(self):
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
            return_value=read_test_swift_data(
                self.mock_file_base_path + self.mock_observation_table
            ),
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
            return_value=read_test_uvot_data(
                self.mock_file_base_path + self.mock_file_uvot_output
            ),
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

    def test_swift_uvot_should_skip_modes_not_in_dict(self):
        """Should skip UVOT modes not in the dictionary"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=read_test_swift_data(
                self.mock_file_base_path + self.mock_observation_table
            ),
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
                "bad_filter_name": [
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
                "not found in uvot_mode_dict, skipping"
                in log_mock.warn.call_args.args[0]
            )

    def test_swift_uvot_should_skip_filter_names_not_in_dict(self):
        """Should skip UVOT modes not in the dictionary"""
        with patch(
            "across_data_ingestion.tasks.schedules.swift.low_fidelity_planned.query_swift_plan",
            return_value=read_test_swift_data(
                self.mock_file_base_path + self.mock_observation_table
            )[:1],
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
                "not found in SWIFT_UVOT_BANDPASS_DICT, skipping"
                in log_mock.warn.call_args.args[0]
            )
