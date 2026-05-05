from datetime import datetime, timedelta

import structlog
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import sdk
from .util import EuclidScheduleHandler, parse_pointing_file, retrieve_schedule_file

logger: structlog.stdlib.BoundLogger = structlog.getLogger()

SCHEDULE_PAGE_URL = "https://www.cosmos.esa.int/web/euclid/where-is-euclid-observing"
SCHEDULE_FILE_BASE_URL = "https://www.cosmos.esa.int"
DAYS_IN_FUTURE_TO_INGEST = 7


def ingest() -> None:
    schedule_file_url = retrieve_schedule_file(SCHEDULE_PAGE_URL)
    if not len(schedule_file_url):
        logger.error("Failed to retrieve schedule file URL from schedule page.")
        return
    df = parse_pointing_file(SCHEDULE_FILE_BASE_URL + schedule_file_url)
    if not len(df):
        return
    filtered_df = df[
        (df["utc"] > datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        & (
            df["utc"]
            < (datetime.now() + timedelta(days=DAYS_IN_FUTURE_TO_INGEST)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )
    ]
    handler = EuclidScheduleHandler(
        observation_status=sdk.ObservationStatus.PLANNED,
        schedule_status=sdk.ScheduleStatus.PLANNED,
        schedule_fidelity=sdk.ScheduleFidelity.LOW,
        schedule_name="low_fidelity_planned",
    )
    handler.run(filtered_df)


@repeat_at(cron="56 2 * * 2", logger=logger)
async def entrypoint():
    try:
        logger.info("Schedule ingestion started.")
        ingest()
        logger.info("Schedule ingestion completed.")
        return
    except Exception as e:
        logger.error("Encountered an unknown error", err=e, exc_info=True)
        return
