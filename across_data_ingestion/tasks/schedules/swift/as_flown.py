from datetime import datetime, timedelta, timezone

import structlog
from fastapi_utilities import repeat_at  # type: ignore
from swifttools import swift_too  # type: ignore

from ....util.across_server import sdk
from .util import CustomSwiftObsEntry, SwiftScheduleHandler

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


def query_swift_as_flown(days_in_past: int = 1) -> list[CustomSwiftObsEntry]:
    """
    Queries the Swift catalog for all Swift completed observations from the past day interval.
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days_in_past)

    query = swift_too.ObsQuery(begin=start_time, end=end_time)

    non_saa_query = [
        CustomSwiftObsEntry.from_entry(observation)
        for observation in query
        if observation.uvot not in ["0x0009"]
    ]

    return non_saa_query


def ingest(days_in_past: int = 1) -> None:
    """
    Method that POSTs Swift as flown observing schedules to the ACROSS server
    For the Swift Observatory, this includes the XRT, BAT, and UVOT Telescopes.
    It interprets a single planned observation as an observation for each instrument since it
    takes data in parallel

    Queries completed observations via the swifttools Swift TOO catalog
    This is a high fidelity complted schedule, it may not agree with what is planned.
    """

    # Get the swift telescope ids along with their instrument ids
    swift_observation_data = query_swift_as_flown(days_in_past)
    if not swift_observation_data:
        logger.warning("Query returned no as flown Swift observations.")
        return

    handler = SwiftScheduleHandler(
        observation_status=sdk.ObservationStatus.PERFORMED,
        schedule_status=sdk.ScheduleStatus.PERFORMED,
        schedule_fidelity=sdk.ScheduleFidelity.HIGH,
        schedule_name_attr="as_flown",
    )
    handler.run(observation_data=swift_observation_data)


# daily at 4am
@repeat_at(cron="0 4 * * *", logger=logger)
async def entrypoint() -> None:
    try:
        ingest()
        logger.info("Swift as_flown schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Swift as_flown schedule ingestion encountered an error", err=e)
        return
