import structlog
from fastapi_utilities import repeat_at  # type: ignore[import-untyped]

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


@repeat_at(cron="* * * * *", logger=logger)
async def example_task():
    logger.info("Task ran successfully.")
