import os
from unittest.mock import patch

import pytest

from across_data_ingestion.util.vo_service import VOService


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

        class MockXMLElement:
            text = "COMPLETED"

        class MockXMLRoot:
            def find(self, pattern):
                return MockXMLElement

        self.mock_client = MockHttpxAsyncClient
        self.url = "mock url"

        self.mock_xml_root = MockXMLRoot

    @pytest.mark.asyncio
    async def test_initialize_should_save_job_url_as_attr(self):
        """Should save job url as model attribute when successful"""
        with patch("httpx.AsyncClient", self.mock_client):
            service = VOService(self.url)
            await service._initialize_query("mock query")
            assert hasattr(service, "job_url")

    @pytest.mark.asyncio
    async def test_run_query_should_return_true_when_successful(self):
        """Should return True when run query is successful"""
        with patch("httpx.AsyncClient", self.mock_client):
            service = VOService(self.url)
            service.job_url = "mock_job_url"
            query_ran = await service._run_query()
            assert query_ran is True

    @pytest.mark.asyncio
    async def test_run_query_should_return_false_when_unsuccessful(self):
        """Should return False when run query is unsuccessful"""
        self.mock_client.response.text = None
        with patch("httpx.AsyncClient", self.mock_client):
            service = VOService(self.url)
            service.job_url = "mock_job_url"
            query_ran = await service._run_query()
            assert query_ran is False

    def test_get_results_should_return_results(self):
        """Should return query results when running get_results"""
        with patch("httpx.AsyncClient", self.mock_client), patch(
            "httpx.get", return_value=self.mock_client.response
        ), patch("xml.etree.ElementTree.fromstring", return_value=self.mock_xml_root()):
            service = VOService(self.url)
            service.job_url = "mock_job_url"
            results = service._get_results()
            assert len(results) > 0

    def test_get_results_should_return_empty_string_if_no_results_found(self):
        """Should return an empty string if no results are found"""

        class MockBadXMLElement:
            text = ""

        class MockBadXMLRoot(self.mock_xml_root):
            def find(self, pattern):
                return MockBadXMLElement

        with patch("httpx.AsyncClient", self.mock_client), patch(
            "httpx.get", return_value=self.mock_client.response
        ), patch("xml.etree.ElementTree.fromstring", return_value=MockBadXMLRoot()):
            service = VOService(self.url)
            service.job_url = "mock_job_url"
            results = service._get_results()
            assert len(results) == 0

    @pytest.mark.asyncio
    async def test_query_should_return_query_results(self):
        """Should return query results when running query"""
        mock_votable_file = os.path.join(
            os.path.dirname(__file__), "mocks/", "mock_votable.xml"
        )
        with open(mock_votable_file, "r") as f:
            table = f.read()
            self.mock_client.response.text = table
            with patch("httpx.AsyncClient", self.mock_client), patch(
                "httpx.get", return_value=self.mock_client.response
            ), patch(
                "xml.etree.ElementTree.fromstring", return_value=self.mock_xml_root()
            ):
                service = VOService(self.url)
                service.job_url = "mock_job_url"
                results = await service.query("mock query")
                assert len(results) > 0

    @pytest.mark.asyncio
    async def test_query_should_return_None_for_no_results(self):
        """Should return None when query finds no results"""
        self.mock_client.response.text = None
        with patch("httpx.AsyncClient", self.mock_client), patch(
            "httpx.get", return_value=self.mock_client.response
        ):
            service = VOService(self.url)
            service.job_url = "mock_job_url"
            results = await service.query("mock query")
            assert results is None
