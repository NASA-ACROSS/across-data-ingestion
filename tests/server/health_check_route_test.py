import fastapi
import pytest
import pytest_asyncio
from httpx import AsyncClient


class TestHeathCheckRoute:
    @pytest_asyncio.fixture(scope="function", autouse=True)
    async def setup(self, async_client: AsyncClient):
        self.client = async_client
        self.endpoint = "/"

    @pytest.mark.asyncio
    async def test_should_return_200_on_success(self):
        """Should return HTTP 200 when successful"""

        res = await self.client.get(self.endpoint)

        assert res.status_code == fastapi.status.HTTP_200_OK

    @pytest.mark.asyncio
    async def test_should_return_ok_on_success(self):
        """Should return ok when successful"""

        res = await self.client.get(self.endpoint)

        assert res.json() == "ok"
