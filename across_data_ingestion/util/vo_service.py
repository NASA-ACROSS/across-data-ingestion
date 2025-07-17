import xml.etree.ElementTree as ET
from io import BytesIO

import httpx
import structlog
from astropy.io import votable  # type: ignore[import-untyped]
from astropy.table import Table  # type: ignore[import-untyped]

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class VOService:
    """
    Class to handle TAP queries to a VO service.
    Currently implemented for the Chandra VO service.
    """

    url: str

    def __init__(self, url: str) -> None:
        self.url = url
        self.client = httpx.AsyncClient()

    async def query(self, query: str) -> Table | None:
        """Wrapper to initialize, run, and fetch results from query"""
        await self._initialize_query(query)
        query_ran = await self._run_query()
        if query_ran:
            results = self._get_results()
            if results:
                return self._to_astropy_table(results)
        return None

    async def _initialize_query(self, query: str) -> None:
        """Puts the query in a queue to be executed"""
        data = {
            "REQUEST": "doQuery",
            "FORMAT": "votable",
            "LANG": "ADQL",
            "QUERY": query,
        }
        response = await self.client.request(
            method="POST",
            url=self.url,
            follow_redirects=True,
            data=data,
        )
        self.job_url = str(response.url)

    async def _run_query(self) -> bool:
        """Runs the queued query"""
        response = await self.client.request(
            method="POST",
            url=self.job_url + "/phase",
            data={"PHASE": "RUN"},
            follow_redirects=True,
        )

        await self.client.aclose()

        if not response.text:
            logger.warn("Chandra TAP query never ran, exiting")
        return bool(response.text)

    def _get_results(self) -> str:
        """
        Gets the results from the ran query.
        Begins with a blocking GET request with a maximum wait time of 10s
        to wait for the query to run, and checks that it was completed
        before proceeding (from TAP documentation).
        """
        block_query_response = httpx.get(self.job_url + "?WAIT=10")
        root = ET.fromstring(block_query_response.text)
        phase = root.find("{http://www.ivoa.net/xml/UWS/v1.0}phase")
        if phase is None or phase.text != "COMPLETED":
            return ""
        response = httpx.get(self.job_url + "/results/result")
        return response.text

    def _to_astropy_table(self, response_text: str) -> Table:
        """Transforms XML response text into astropy Table"""
        tabledata = votable.parse(BytesIO(response_text.encode()))
        table = tabledata.get_first_table().to_table()
        return table
