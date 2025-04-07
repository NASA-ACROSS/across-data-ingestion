import os
import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, status

from .core import config, logging
from .tasks.task_loader import init_tasks

# Configure UTC system time
os.environ["TZ"] = "UTC"
time.tzset()

logging.setup(json_logs=config.LOG_JSON_FORMAT, log_level=config.LOG_LEVEL)
logger: structlog.stdlib.BoundLogger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("STARTUP EVENT: Initializing tasks")
    await init_tasks()
    yield


app = FastAPI(
    title="ACROSS Data Ingestion",
    summary="Astrophysics Cross-Observatory Science Support (ACROSS)",
    description="A service to import data related to the ACROSS system.",
    root_path=config.ROOT_PATH,
    lifespan=lifespan,
)


# Health Check Route
@app.get(
    "/",
    summary="Health Check",
    description="Container Health Check Route",
    status_code=status.HTTP_200_OK,
)
async def get() -> str:
    logger.debug("health check!")
    return "ok"
