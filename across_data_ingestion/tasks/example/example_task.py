import time
from fastapi_utils.tasks import repeat_every
import logging

logger = logging.getLogger("uvicorn.error")


@repeat_every(seconds=1, max_repetitions=3)
async def example_task():
    current_time = time.time()
    logger.info(f"{__name__} ran successfully at {current_time}")
