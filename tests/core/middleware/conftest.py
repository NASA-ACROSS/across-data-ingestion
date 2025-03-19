from collections.abc import AsyncGenerator

import httpx
import pytest
import pytest_asyncio
import structlog
from fastapi import FastAPI

from across_data_ingestion import main


@pytest.fixture(scope="module")
def app() -> FastAPI:
    return main.app


@pytest_asyncio.fixture(scope="module", autouse=True)
async def async_client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient]:
    host, port = "127.0.0.1", 9000

    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app,
            client=(host, port),
        ),
        base_url="http://test",
    )

    async with client:
        yield client


@pytest.fixture(name="log_output")
def log_output() -> structlog.testing.LogCapture:
    return structlog.testing.LogCapture()


@pytest.fixture(autouse=True)
def configure_structlog(log_output: structlog.testing.LogCapture) -> None:
    structlog.configure(processors=[log_output])
