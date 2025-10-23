import structlog
from fastapi_utilities import repeat_at  # type: ignore[import-untyped]

from ...util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


@repeat_at(cron="*/5 * * * *", logger=logger)
async def check_server():
    observatories = sdk.ObservatoryApi(client).get_observatories()

    logger.info("Task ran successfully.", observatories=[o.name for o in observatories])
