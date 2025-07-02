import json
import os
from unittest.mock import AsyncMock, Mock, patch

import pytest
from astropy.table import Table  # type: ignore[import-untyped]

from across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned import (
    VOService,
    create_schedule,
    entrypoint,
    format_response_as_astropy_table,
    get_instrument_info_from_observation,
    ingest,
)


class TestVOService:
    @pytest.fixture(autouse=True)
    def setup(self):
        class MockResponse:
            url = "mock_url"
            text = "response text"

        class MockHttpxAsyncClient:
            response = MockResponse()

            async def request(self, *args, **kwargs):
                return self.response

            async def aclose(self):
                return None

        self.mock_client = MockHttpxAsyncClient

    @pytest.mark.asyncio
    async def test_initialize_should_save_job_url_as_attr(self):
        """Should save job url as model attribute when successful"""
        with patch("httpx.AsyncClient", self.mock_client):
            service = VOService()
            await service.initialize_query("mock query")
            assert hasattr(service, "job_url")

    @pytest.mark.asyncio
    async def test_run_query_should_return_true_when_successful(self):
        """Should return True when run query is successful"""
        with patch("httpx.AsyncClient", self.mock_client):
            service = VOService()
            service.job_url = "mock_job_url"
            query_ran = await service.run_query()
            assert query_ran is True

    @pytest.mark.asyncio
    async def test_run_query_should_return_false_when_unsuccessful(self):
        """Should return False when run query is unsuccessful"""
        self.mock_client.response.text = None
        with patch("httpx.AsyncClient", self.mock_client):
            service = VOService()
            service.job_url = "mock_job_url"
            query_ran = await service.run_query()
            assert query_ran is False

    def test_get_results_should_return_results(self):
        """Should return query results when running get_results"""
        with patch("httpx.AsyncClient", self.mock_client), patch(
            "httpx.get", return_value=self.mock_client.response
        ):
            service = VOService()
            service.job_url = "mock_job_url"
            results = service.get_results()
            assert len(results) > 0

    @pytest.mark.asyncio
    async def test_query_should_return_query_results(self):
        """Should return query results when running query"""
        with patch("httpx.AsyncClient", self.mock_client), patch(
            "httpx.get", return_value=self.mock_client.response
        ):
            service = VOService()
            service.job_url = "mock_job_url"
            results = await service.query("mock query")
            assert len(results) > 0


class TestChandraHighFidelityPlannedScheduleIngestionTask:
    mock_file_base_path = os.path.join(os.path.dirname(__file__), "mocks/")
    mock_schedule_output = "chandra_high_fidelity_planned_mock_schedule_output.json"
    mock_votable = "mock_votable.xml"

    @pytest.mark.asyncio
    async def test_should_generate_across_schedule(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
        mock_format_vo_table: Mock,
    ):
        """Should generate ACROSS schedules"""
        mock_output_schedule_file = self.mock_file_base_path + self.mock_schedule_output
        with patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post",
            return_value=None,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.format_response_as_astropy_table",
            mock_format_vo_table,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock:
            await ingest()
            schedules = log_mock.info.call_args.args[0]
            with open(mock_output_schedule_file) as expected_output_file:
                expected = json.load(expected_output_file)
                assert json.loads(schedules) == expected

    @pytest.mark.asyncio
    async def test_should_generate_observations_with_schedule(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
        mock_format_vo_table: Mock,
    ):
        """Should generate list of observations with an ACROSS schedule"""
        with patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post",
            return_value=None,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.format_response_as_astropy_table",
            mock_format_vo_table,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock:
            await ingest()
            schedules = log_mock.info.call_args.args[0]
            assert len(json.loads(schedules)["observations"]) > 0

    def test_format_response_as_astropy_table_should_return_table(self):
        """Should format query response as astropy table"""
        mock_votable_file = self.mock_file_base_path + self.mock_votable
        with open(mock_votable_file, "r") as f:
            table = format_response_as_astropy_table(f.read())
            assert isinstance(table, Table)

    @pytest.mark.asyncio
    async def test_should_log_warning_when_query_returns_no_response(
        self, mock_telescope_get: list[dict[str, str]]
    ):
        """Should log a warning when querying VO service returns None"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            AsyncMock(return_value=None),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await ingest()
            assert "No response returned" in log_mock.warn.call_args.args[0]

    def test_create_schedule_should_return_empty_dict_for_no_data(
        self, mock_telescope_id: str, mock_observation_table: Table
    ):
        """Should return an empty dictionary when no observations exists"""
        mock_observation_table.keep_columns([])
        schedule = create_schedule(mock_telescope_id, mock_observation_table)
        assert schedule == {}

    @pytest.mark.asyncio
    async def test_should_log_warning_when_no_observations_found(
        self,
        mock_observation_table: Table,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
        mock_format_vo_table: Mock,
    ):
        """Should log a warning when querying VO service returns None"""
        mock_observation_table.keep_columns([])
        mock_format_vo_table.return_value = mock_observation_table
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.format_response_as_astropy_table",
            mock_format_vo_table,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await ingest()
            assert "No results returned" in log_mock.warn.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_error_when_unable_to_parse_instrument(
        mock_observation_table: Table,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
        mock_format_vo_table: Mock,
    ):
        """Should log error when unable to parse instrument from observation"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.format_response_as_astropy_table",
            mock_format_vo_table,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.get_instrument_info_from_observation",
            return_value=("", ""),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await ingest()
            assert "Cannot parse observations" in log_mock.error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_info_when_success(
        self,
        mock_telescope_get: list[dict[str, str]],
        mock_query_vo_service: AsyncMock,
        mock_format_vo_table: Mock,
    ):
        """Should log info with ran at when success"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
            mock_query_vo_service,
        ), patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.format_response_as_astropy_table",
            mock_format_vo_table,
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await entrypoint()
            assert "ingestion completed" in log_mock.info.call_args.args[0]

    @pytest.mark.asyncio
    async def test_should_log_error_when_schedule_ingestion_fails(
        self,
        mock_telescope_get: list[dict[str, str]],
    ):
        """Should log an error when schedule ingestion fails"""
        with patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
        ) as log_mock, patch(
            "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.ingest",
            return_value=None,
            side_effect=Exception(),
        ), patch(
            "across_data_ingestion.util.across_api.telescope.get",
            return_value=mock_telescope_get,
        ), patch(
            "across_data_ingestion.util.across_api.schedule.post", return_value=None
        ):
            await entrypoint()
            assert "encountered an error" in log_mock.error.call_args.args[0]

    @pytest.mark.parametrize(
        "row, expected_name, expected_id",
        [
            (
                "mock_acis_observation_row",
                "ACIS",
                "acis-mock-id",
            ),
            (
                "mock_acis_hetg_observation_row",
                "ACIS-HETG",
                "acis-hetg-mock-id",
            ),
            (
                "mock_acis_letg_observation_row",
                "ACIS-LETG",
                "acis-letg-mock-id",
            ),
            (
                "mock_acis_cc_observation_row",
                "ACIS-CC",
                "acis-cc-mock-id",
            ),
            (
                "mock_hrc_observation_row",
                "HRC",
                "hrc-mock-id",
            ),
            (
                "mock_hrc_hetg_observation_row",
                "HRC-HETG",
                "hrc-hetg-mock-id",
            ),
            (
                "mock_hrc_letg_observation_row",
                "HRC-LETG",
                "hrc-letg-mock-id",
            ),
            (
                "mock_hrc_timing_observation_row",
                "HRC-Timing",
                "hrc-timing-mock-id",
            ),
            (
                "mock_bad_acis_observation_row",
                "",
                "",
            ),
            (
                "mock_bad_hrc_observation_row",
                "",
                "",
            ),
            (
                "mock_bad_instrument_observation_row",
                "",
                "",
            ),
        ],
    )
    def test_get_instrument_info_from_observation(
        self,
        row: str,
        expected_name: str,
        expected_id: str,
        request: pytest.FixtureRequest,
        mock_instrument_info: dict,
    ) -> None:
        """Should return correct instrument name and id from observation row"""
        assert get_instrument_info_from_observation(
            mock_instrument_info, request.getfixturevalue(row)
        ) == (expected_name, expected_id)
