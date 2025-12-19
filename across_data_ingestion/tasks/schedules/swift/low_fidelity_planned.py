from datetime import datetime, timedelta, timezone

import structlog
from fastapi_utilities import repeat_at  # type: ignore
from swifttools import swift_too  # type: ignore

from ....util.across_server import sdk
from .util import CustomSwiftObsEntry, SwiftScheduleHandler

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


def query_swift_plan(days_in_future: int = 4) -> list[CustomSwiftObsEntry]:
    """
    Queries the Swift catalog for all Swift planned observations from now until 4 days from now.
    """
    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(days=days_in_future)

    query = swift_too.PlanQuery(begin=start_time, end=end_time)

    non_saa_query = [
        CustomSwiftObsEntry.from_entry(observation)
        for observation in query
        if observation.uvot not in ["0x0009"]
    ]

    return non_saa_query


def ingest(days_in_future: int = 4) -> None:
    """
    Method that POSTs Swift low fidelity planned observing schedules to the ACROSS server
    For the Swift Observatory, this includes the XRT, BAT, and UVOT Telescopes.
    It interprets a single planned observation as an observation for each instrument since it
    takes data in parallel

    Queries planned observations via the swifttools Swift TOO catalog
    This is a low fidelity schedule, meaning it is not guaranteed to be accurate or complete.
    """

    # Get the swift telescope ids along with their instrument ids
    swift_observation_data = query_swift_plan(days_in_future)
    if not swift_observation_data:
        logger.warning("Query returned no planned Swift observations.")
        return

    handler = SwiftScheduleHandler(
        observation_status=sdk.ObservationStatus.PLANNED,
        schedule_status=sdk.ScheduleStatus.PLANNED,
        schedule_fidelity=sdk.ScheduleFidelity.LOW,
        schedule_name_attr="low_fidelity_planned",
    )
    handler.run(observation_data=swift_observation_data)


@repeat_at(cron="44 22 * * *", logger=logger)
async def entrypoint() -> None:
    try:
        ingest()
        logger.info("Swift low fidelity planned schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(
            "Swift low fidelity planned schedule ingestion encountered an error", err=e
        )
        return
