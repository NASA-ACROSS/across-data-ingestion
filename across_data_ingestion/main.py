import os
import time
import uuid
from contextlib import asynccontextmanager

import structlog
from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI, status

from .core import config, logging
from .core.middleware import LoggingMiddleware
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


app.add_middleware(LoggingMiddleware)

# This middleware must be placed after the logging, to populate the context with the request ID
# NOTE: Why last??
# Answer: middlewares are applied in the reverse order of when they are added (you can verify this
# by debugging `app.middleware_stack` and recursively drilling down the `app` property).
app.add_middleware(
    CorrelationIdMiddleware,
    header_name=config.REQUEST_ID_HEADER,
    update_request_header=True,
    generator=lambda: uuid.uuid4().hex,
)


@app.get(
    "/",
    summary="Health Check",
    description="Container Health Check Route",
    status_code=status.HTTP_200_OK,
)
async def get() -> str:
    logger.debug("health check!")
    return "ok"
