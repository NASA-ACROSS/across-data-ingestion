import asyncio
import contextlib
import xml.etree.ElementTree as ET
from io import BytesIO

import httpx
import structlog
from astropy.io import votable  # type: ignore[import-untyped]
from astropy.table import Table  # type: ignore[import-untyped]

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class VOService(contextlib.AbstractAsyncContextManager):
    """
    Class to handle TAP queries to a VO service used within a context manager.
    Currently implemented for the Chandra VO service.

    Usage:
    ```
    with VOService() as vo_service:
        # query 1
        res1 = await vo_service.query(...)
        # another query 2
        res2 = await vo_service.query(...)
    ```
    """

    _url: str

    def __init__(self, url: str) -> None:
        self._url = url
        self._lock = asyncio.Lock()
        self._client = httpx.AsyncClient()

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
        response = await self._client.request(
            method="POST",
            url=self._url,
            follow_redirects=True,
            data=data,
        )
        self.job_url = str(response.url)

    async def _run_query(self) -> bool:
        """Runs the queued query"""
        response = await self._client.request(
            method="POST",
            url=self.job_url + "/phase",
            data={"PHASE": "RUN"},
            follow_redirects=True,
        )

        if not response.text:
            logger.warning("Chandra TAP query never ran, exiting")

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

    def _require_client(self):
        if not self._entered or self._client is None or self._client.is_closed:
            raise RuntimeError("MyHttpxService must be used inside an async with block")

    async def __aenter__(self):
        """initialize the httpx client upon entering the context manager"""
        async with self._lock:
            if self._client is None:
                self._client = httpx.AsyncClient()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        """close the httpx client upon exiting the context manager"""
        async with self._lock:
            if self._client:
                await self._client.aclose()

            self._client = None
