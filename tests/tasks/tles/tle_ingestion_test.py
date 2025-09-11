from typing import cast
from unittest.mock import MagicMock

import pytest

import across_data_ingestion.tasks.tles.tle_ingestion as task
from across_data_ingestion.util.across_server import sdk


@pytest.fixture
def expected_tle_parameters(fake_observatory: sdk.Observatory) -> sdk.TLEParameters:
    return cast(
        sdk.TLEParameters,
        (fake_observatory.ephemeris_types or [])[0].parameters.actual_instance,
    )


class TestExtractNoradEntries:
    def test_should_return_list_of_satellites(
        self, fake_observatory: sdk.Observatory
    ) -> None:
        """Should return list of satellites to pull TLEs"""
        entries = task.extract_norad_satellites([fake_observatory])
        assert len(entries)

    def test_should_return_instance_of_satellite_data(
        self,
        fake_observatory: sdk.Observatory,
    ) -> None:
        """Should return instance of norad satellite data object"""
        [satellite] = task.extract_norad_satellites([fake_observatory])

        assert isinstance(satellite, task.NoradSatellite)

    def test_should_extract_norad_id(
        self,
        fake_observatory: sdk.Observatory,
        expected_tle_parameters: sdk.TLEParameters,
    ) -> None:
        """Should extract NORAD ID from ACROSS observatory data"""
        [satellite] = task.extract_norad_satellites([fake_observatory])

        assert satellite.id == expected_tle_parameters.norad_id

    def test_should_extract_norad_satellite_name(
        self,
        fake_observatory: sdk.Observatory,
        expected_tle_parameters: sdk.TLEParameters,
    ) -> None:
        """Should extract NORAD satellite name from ACROSS observatory data"""
        [satellite] = task.extract_norad_satellites([fake_observatory])

        assert satellite.name == expected_tle_parameters.norad_satellite_name

    def test_should_extract_all_tle_ephem_type_satellites(
        self,
        fake_observatories: list[sdk.Observatory],
    ) -> None:
        """Should extract NORAD satellite name from ACROSS observatory data"""
        satellites = task.extract_norad_satellites(fake_observatories)

        assert len(satellites) == 2


class TestIngest:
    def test_should_get_observatories(self, mock_observatory_api: MagicMock) -> None:
        task.ingest()
        mock_observatory_api.get_observatories.assert_called_once()

    def test_should_call_get_tle_util(self, mock_get_tle: MagicMock) -> None:
        task.ingest()
        mock_get_tle.assert_called()

    def test_should_create_new_tle_record(self, mock_tle_api: MagicMock) -> None:
        """Should return TLE information when fetching TLE from spacetrack is successful"""
        task.ingest()
        first_call = mock_tle_api.create_tle.call_args_list[0]
        sent_tle: sdk.TLECreate = first_call.args[0]

        assert isinstance(sent_tle, sdk.TLECreate)

    def test_should_create_new_tle_record_with_tle_lines(
        self, mock_tle_api: MagicMock
    ) -> None:
        """Should return TLE information when fetching TLE from spacetrack is successful"""
        task.ingest()

        first_call = mock_tle_api.create_tle.call_args_list[0]
        sent_tle: sdk.TLECreate = first_call.args[0]

        assert len(sent_tle.tle1) and len(sent_tle.tle2)

    def test_should_log_warning_when_tle_cannot_be_fetched(
        self, mock_get_tle: MagicMock, mock_logger: MagicMock
    ):
        mock_get_tle.return_value = None

        task.ingest()

        mock_logger.warning.assert_called()
