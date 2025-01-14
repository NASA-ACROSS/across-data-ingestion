import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, status

from .core.config import config
from .tasks.task_loader import init_tasks

logger = logging.getLogger("uvicorn.error")


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
async def get():
    return "ok"
