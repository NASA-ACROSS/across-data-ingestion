from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from across_data_ingestion.tasks.schedules.xrism.low_fidelity_planned import (
    ingest,
    parse_short_term_schedule_page,
    parse_too_table,
    query_proposal_table,
)


class TestParseShortTermSchedulePage:
    def test_should_log_warning_if_no_schedules_found(
        self,
        mock_logger: MagicMock,
    ) -> None:
        """Should log a warning if no schedule tables are found on the short term schedule page"""
        parse_short_term_schedule_page()
        assert (
            "Could not find any short term schedule tables to parse"
            in mock_logger.warning.call_args[0][0]
        )

    def test_should_return_empty_df_if_no_schedules_found(
        self,
    ) -> None:
        """Should return an empty DataFrame if no schedule tables are found on the short term schedule page"""
        df = parse_short_term_schedule_page()
        assert len(df) == 0

    def test_should_return_df_if_schedules_found(
        self,
        mock_pandas_html: MagicMock,
        fake_short_term_schedule_table: pd.DataFrame,
    ) -> None:
        """Should return a DataFrame of the first schedule table found on the short term schedule page"""
        mock_pandas_html.read_html.return_value = [fake_short_term_schedule_table]
        df = parse_short_term_schedule_page()
        assert len(df)


class TestQueryProposalTable:
    def test_should_log_warning_if_no_results_found(
        self, mock_logger: MagicMock
    ) -> None:
        """Should log a warning if no results are found from the XRISM proposal table query"""
        query_proposal_table(pd.DataFrame({"observation_id": ["12345"]}))
        assert (
            "Could not find exposure time information for short term schedule observations"
            in mock_logger.warning.call_args[0][0]
        )

    def test_should_add_exposure_times_from_query_results(
        self,
        mock_pandas_html: MagicMock,
        fake_xrism_proposal_table: pd.DataFrame,
    ) -> None:
        """Should add exposure times from results of the XRISM proposal table query"""
        fake_xrism_proposal_table["proposal_type"] = "GO"
        mock_pandas_html.read_html.return_value = [fake_xrism_proposal_table]
        df = query_proposal_table(pd.DataFrame({"observation_id": ["12345"]}))
        assert (
            df["exposure_time"].values
            == fake_xrism_proposal_table["awarded_exposure"].values
        )

    def test_should_not_add_exposure_times_for_calibration_observations(
        self,
        mock_pandas_html: MagicMock,
        fake_xrism_proposal_table: pd.DataFrame,
    ) -> None:
        """Should not add exposure times for calibration observations"""
        mock_pandas_html.read_html.return_value = [fake_xrism_proposal_table]
        df = query_proposal_table(pd.DataFrame({"observation_id": ["12345"]}))
        assert all(np.isnan(df["exposure_time"]).values)

    def test_should_return_dataframe_with_same_observation_ids(
        self,
        mock_pandas_html: MagicMock,
        fake_xrism_proposal_table: pd.DataFrame,
    ) -> None:
        """Should return a dataframe with the same observation IDs as the input"""
        mock_pandas_html.read_html.return_value = [fake_xrism_proposal_table]
        df = query_proposal_table(pd.DataFrame({"observation_id": ["12345"]}))
        assert df["observation_id"].values == ["12345"]


class TestParseToOTable:
    def test_should_log_warning_if_no_tables_found(
        self, mock_logger: MagicMock
    ) -> None:
        """Should log a warning if no tables are found on the ToO page"""
        parse_too_table(pd.DataFrame({"observation_id": ["12345"]}))
        assert "Could not read ToO table" in mock_logger.warning.call_args[0][0]

    def test_should_add_exposure_times_from_query_results(
        self,
        mock_pandas_html: MagicMock,
        fake_xrism_too_table: pd.DataFrame,
    ) -> None:
        """Should add exposure times from the ToO page"""
        mock_pandas_html.read_html.return_value = [fake_xrism_too_table]
        df = parse_too_table(pd.DataFrame({"observation_id": ["12345"]}))
        assert df["exposure_time"].values == fake_xrism_too_table["Exp. (ks)"].values

    def test_should_return_dataframe_with_same_observation_ids(
        self,
        mock_pandas_html: MagicMock,
        fake_xrism_too_table: pd.DataFrame,
    ) -> None:
        """Should return a dataframe with the same observation IDs as the input"""
        fake_xrism_too_table.loc[0] = {"Seq": "54321", "Exp. (ks)": 50000}
        mock_pandas_html.read_html.return_value = [fake_xrism_too_table]
        df = parse_too_table(pd.DataFrame({"observation_id": ["12345"]}))
        assert df["observation_id"].values == ["12345"]


class TestIngest:
    def test_should_call_schedule_handler(
        self,
        mock_schedule_handler: MagicMock,
        mock_parse_short_term_schedule_page: MagicMock,
        mock_query_proposal_table: MagicMock,
        mock_parse_too_table: MagicMock,
    ) -> None:
        """Should call the schedule handler to handle the observations once"""
        ingest()

        mock_schedule_handler.run.assert_called_once()
