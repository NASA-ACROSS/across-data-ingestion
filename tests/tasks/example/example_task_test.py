from unittest.mock import patch

import pytest

from across_data_ingestion.tasks.example import example_task


class TestExampleTask:
    @pytest.mark.asyncio
    async def test_should_log_string(self):
        """Should log ran successfully"""
        with patch(
            "across_data_ingestion.tasks.example.example_task.logger"
        ) as log_mock:
            await example_task()
            assert "ran successfully" in log_mock.info.call_args.args[0]
