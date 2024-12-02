from fastapi import FastAPI, status
from .core.config import config
from .tasks.example import example_task

app = FastAPI(
    title="ACROSS Data Ingestion",
    summary="Astrophysics Cross-Observatory Science Support (ACROSS)",
    description="A service to import data related to the ACROSS system.",
    root_path=config.ROOT_PATH,
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

@app.on_event("startup")
async def init_tasks():
    await example_task()
