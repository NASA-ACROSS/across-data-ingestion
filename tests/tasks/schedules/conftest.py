from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture
def mock_schedule_post() -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.util.across_api.schedule.post", return_value=None
    ) as mock_schedule_post:
        yield mock_schedule_post
