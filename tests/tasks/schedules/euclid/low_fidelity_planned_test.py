from unittest.mock import MagicMock

import pandas as pd
from httpx import HTTPError

from across_data_ingestion.tasks.schedules.euclid.low_fidelity_planned import (
    ingest,
    parse_pointing_file,
    retrieve_schedule_file_path,
)


class TestRetrieveScheduleFile:
    def test_should_log_error_if_request_fails(
        self, mock_httpx_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Should log an error if the httpx request to retrieve the schedule page fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        retrieve_schedule_file_path("")

        assert (
            "Long term planned schedule request failed"
            in mock_logger.error.call_args[0][0]
        )

    def test_should_return_empty_string_if_request_fails(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return an empty string if the httpx request to retrieve the schedule page fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        file_url = retrieve_schedule_file_path("")

        assert len(file_url) == 0

    def test_should_return_empty_string_if_bs4_cannot_find_a_tags(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return an empty string if BeautifulSoup cannot find any a tags in the schedule page"""
        mock_httpx_get.return_value.return_value = ""
        file_url = retrieve_schedule_file_path("")

        assert len(file_url) == 0

    def test_should_return_correct_url_from_a_tag(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return the correct url corresponding to the schedule file"""
        mock_httpx_get.return_value.text = (
            "<a href='/bad_url'>Bad URL</a><a href='/good_url.txt'>good_url.txt</a>"
        )
        file_url = retrieve_schedule_file_path("")

        assert file_url == "/good_url.txt"


class TestParsePointingFile:
    def test_should_log_error_if_request_fails(
        self, mock_httpx_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Should log an error if the httpx request fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        parse_pointing_file("")

        assert "Failed to retrieve schedule file" in mock_logger.error.call_args[0][0]

    def test_should_return_empty_dataframe_if_request_fails(
        self, mock_httpx_get: MagicMock
    ) -> None:
        """Should return an empty pandas DataFrame if the httpx request fails"""
        mock_httpx_get.return_value.raise_for_status.side_effect = HTTPError(
            "mock error"
        )
        df = parse_pointing_file("")

        assert df.empty

    def test_should_return_dataframe_when_successful(
        self, fake_pointing_file: str, mock_httpx_get: MagicMock
    ) -> None:
        """Should return a pandas DataFrame with the correct data when the httpx request is successful"""
        mock_httpx_get.return_value.text = fake_pointing_file
        df = parse_pointing_file("")

        assert not df.empty

    def test_should_log_warning_if_schedule_file_has_incomplete_row(
        self,
        fake_pointing_file_bad_data: str,
        mock_httpx_get: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Should log a warning when ingesting a schedule file that has a row with missing values"""
        mock_httpx_get.return_value.text = fake_pointing_file_bad_data
        parse_pointing_file("")

        assert (
            "Skipping line with insufficient columns"
            in mock_logger.warning.call_args[0][0]
        )


class TestIngest:
    def test_should_call_schedule_handler(
        self,
        mock_schedule_handler: MagicMock,
        mock_retrieve_schedule_file_path: MagicMock,
        mock_parse_pointing_file: MagicMock,
    ) -> None:
        """Should call the schedule handler to handle the observations once"""
        ingest()

        mock_schedule_handler.run.assert_called_once()

    def test_should_not_call_handler_when_file_url_not_found(
        self,
        mock_schedule_handler: MagicMock,
        mock_retrieve_schedule_file_path: MagicMock,
    ) -> None:
        """Should not call the handler when retrieve_schedule_file cannot find the URL of the file"""
        mock_retrieve_schedule_file_path.return_value = ""
        ingest()

        mock_schedule_handler.run.assert_not_called()

    def test_should_not_call_handler_when_schedule_file_not_read(
        self,
        mock_schedule_handler: MagicMock,
        mock_retrieve_schedule_file_path: MagicMock,
        mock_parse_pointing_file: MagicMock,
    ) -> None:
        """Should not call the handler when parse_pointing_file cannot read the schedule file"""
        mock_parse_pointing_file.return_value = pd.DataFrame([])
        ingest()

        mock_schedule_handler.run.assert_not_called()

    def test_should_log_warning_when_no_observations_read(
        self,
        mock_retrieve_schedule_file_path: MagicMock,
        mock_parse_pointing_file: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Should log a warning when no observations are read from the pointing file"""
        mock_parse_pointing_file.return_value = pd.DataFrame([])
        ingest()

        assert (
            "No pointing observations found, nothing to ingest."
            in mock_logger.warning.call_args[0][0]
        )
