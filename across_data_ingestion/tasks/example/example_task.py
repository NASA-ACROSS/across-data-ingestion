import logging
import time

from fastapi_utilities import repeat_at  # type: ignore[import-untyped]

logger = logging.getLogger("uvicorn.error")


@repeat_at(cron="* * * * *", logger=logger)
async def example_task():
    current_time = time.time()
    logger.info(f"{__name__} ran successfully at {current_time}")
