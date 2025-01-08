from unittest.mock import patch

import pytest


def mock_repeat_every(func):
    return func


# MUST MOCK DECORATOR BEFORE THE UNIT UNDER TEST GETS IMPORTED!
patch(
    "fastapi_utils.tasks.repeat_every", lambda *args, **kwargs: mock_repeat_every
).start()

from across_data_ingestion.tasks.example import example_task  # noqa: E402


class TestExampleTask:
    @pytest.mark.asyncio
    async def test_should_log_string(self):
        """Should log a string"""
        with patch(
            "across_data_ingestion.tasks.example.example_task.logger"
        ) as log_mock:
            await example_task()
            log_mock.info.assert_called()
