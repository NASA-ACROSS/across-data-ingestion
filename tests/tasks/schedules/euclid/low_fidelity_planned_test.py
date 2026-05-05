from unittest.mock import MagicMock

import pandas as pd

from across_data_ingestion.tasks.schedules.euclid.low_fidelity_planned import ingest


class TestIngest:
    def test_should_call_schedule_handler(
        self,
        mock_schedule_handler: MagicMock,
        mock_retrieve_schedule_file: MagicMock,
        mock_parse_pointing_file: MagicMock,
    ) -> None:
        """Should call the schedule handler to handle the observations once"""
        ingest()

        mock_schedule_handler.run.assert_called_once()

    def test_should_log_error_when_file_url_not_found(
        self,
        mock_retrieve_schedule_file: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Should log an error when retrieve_schedule_file cannot find the URL of the file"""
        mock_retrieve_schedule_file.return_value = ""
        ingest()

        assert (
            "Failed to retrieve schedule file URL from schedule page"
            in mock_logger.error.call_args[0][0]
        )

    def test_should_not_call_handler_when_file_url_not_found(
        self,
        mock_schedule_handler: MagicMock,
        mock_retrieve_schedule_file: MagicMock,
    ) -> None:
        """Should not call the handler when retrieve_schedule_file cannot find the URL of the file"""
        mock_retrieve_schedule_file.return_value = ""
        ingest()

        mock_schedule_handler.run.assert_not_called()

    def test_should_not_call_handler_when_schedule_file_not_read(
        self,
        mock_schedule_handler: MagicMock,
        mock_retrieve_schedule_file: MagicMock,
        mock_parse_pointing_file: MagicMock,
    ) -> None:
        """Should not call the handler when parse_pointing_file cannot read the schedule file"""
        mock_parse_pointing_file.return_value = pd.DataFrame([])
        ingest()

        mock_schedule_handler.run.assert_not_called()
