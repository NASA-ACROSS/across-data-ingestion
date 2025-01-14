from across_data_ingestion.tasks.example import example_task


async def init_tasks():
    """
    Place tasks here to get initialized at startup.
    Each task definition contains its own configuration using a repeat_every decorator
    For more information see https://fastapiutils.github.io/fastapi-utils//user-guide/repeated-tasks/
    """
    await example_task()
