import pandas as pd
import structlog
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import sdk
from .util import (
    RubinSchedulerHandler,
    designate_filter_name_key,
    designate_mjd_to_datetime,
)

logger: structlog.stdlib.BoundLogger = structlog.getLogger()


def get_low_fidelity_planned_schedules_data(execution_status: str) -> pd.DataFrame:
    rubin_planned_obsloctap_url = (
        "https://usdf-rsp.slac.stanford.edu/obsloctap/schedule"
    )
    df = pd.read_json(rubin_planned_obsloctap_url)

    if df.empty:
        return df

    df = df.query(f"execution_status == '{execution_status}'").reset_index(drop=True)
    df["filter_name"] = df.apply(designate_filter_name_key, axis=1)
    df = df.query("filter_name != 'unknown'").reset_index(drop=True)

    df["date_range_begin"] = df.apply(designate_mjd_to_datetime, axis=1, which="begin")
    df["date_range_end"] = df.apply(designate_mjd_to_datetime, axis=1, which="end")

    return df


def ingest():
    """Ingests low fidelity planned Rubin schedules."""

    rubin_planned_obsloctap_df = get_low_fidelity_planned_schedules_data(
        execution_status="Scheduled"
    )
    if rubin_planned_obsloctap_df.empty:
        logger.warning(
            "No observations found in Rubin OBSLOCTAP for low fidelity planned schedules.",
        )
        return

    handler = RubinSchedulerHandler(
        observation_status=sdk.ObservationStatus.PLANNED,
        schedule_status=sdk.ScheduleStatus.PLANNED,
        schedule_fidelity=sdk.ScheduleFidelity.LOW,
        schedule_name="rubin_low_fidelity_planned",
    )
    handler.run(rubin_planned_obsloctap_df)


@repeat_at(cron="0 18 * * *", logger=logger)
async def entrypoint():
    try:
        logger.info("Schedule ingestion started.")
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        logger.error("Encountered an unknown error", err=e, exc_info=True)
        return
