import structlog
from fastapi_utilities import repeat_at  # type: ignore

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


@repeat_at(cron="* * * * *", logger=logger)
async def example_task() -> None:
    logger.info("Task ran successfully.")
