from typing import NamedTuple, cast

import pandas as pd
import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

NICER_BANDPASS = sdk.Bandpass(
    sdk.EnergyBandpass(
        min=0.2,
        max=12.0,
        unit=sdk.EnergyUnit.KEV,
        filter_name="NICER XTI",
    )
)

NICER_TIMELINE_FILE = (
    "https://heasarc.gsfc.nasa.gov/docs/nicer/schedule/obs_pred_timeline_detail.csv"
)


class ObservationRow(NamedTuple):
    ObsID: int
    Target: str
    TargetID: int
    Start: str
    Stop: str
    Duration: float
    RightAscension: float
    Declination: float
    Mode: str


def transform_to_across_schedule(
    telescope_id: str,
    data: pd.DataFrame,
    status: sdk.ScheduleStatus,
    fidelity: sdk.ScheduleFidelity,
) -> sdk.ScheduleCreate:
    """
    Creates a NICER schedule from the provided data.
    """

    begin = Time(f"{min(data["Start"])}", format="isot").isot
    end = Time(f"{max(data["Stop"])}", format="isot").isot

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"nicer_{status.value}_{fidelity.value}_{begin.split('T')[0]}_{end.split('T')[0]}",
        date_range=sdk.DateRange(
            begin=begin,
            end=end,
        ),
        status=status,
        fidelity=fidelity,
        observations=[],
    )


def transform_to_across_observation(
    instrument_id: str, row: ObservationRow
) -> sdk.ObservationCreate:
    """
    Creates a NICER observation from the provided row of data.
    """
    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=row.Target,
        pointing_position=sdk.Coordinate(
            ra=row.RightAscension,
            dec=row.Declination,
        ),
        object_position=sdk.Coordinate(
            ra=row.RightAscension,
            dec=row.Declination,
        ),
        date_range=sdk.DateRange(
            begin=Time(row.Start, format="isot").isot,
            end=Time(row.Stop, format="isot").isot,
        ),
        external_observation_id=str(row.ObsID),
        type=sdk.ObservationType.IMAGING,
        status=sdk.ObservationStatus.PLANNED,
        exposure_time=float(row.Duration),
        bandpass=NICER_BANDPASS,
        pointing_angle=0.0,
    )


def ingest(schedule_modes: list[str] = ["Scheduled"]) -> None:
    """
    Method that posts NICER low fidelity observing schedules via the known webfile:
        https://heasarc.gsfc.nasa.gov/docs/nicer/schedule/obs_pred_timeline_detail.csv

    If the file is not available, it will pass and return an empty schedule. If the file is available,
    it will grab all observations with the Mode "Scheduled", and create a schedule with the planned
    observations. Then it will then POST the schedule to the ACROSS server.
    """
    nicer_df = pd.read_csv(NICER_TIMELINE_FILE)

    # Only get planned observations
    nicer_planned_df = nicer_df.loc[nicer_df["Mode"].isin(schedule_modes)]
    if nicer_planned_df.empty:
        logger.warning(
            "No observations found in NICER timeline file.",
            schedule_modes=schedule_modes,
        )
        return

    # GET Telescope by name
    telescope = sdk.TelescopeApi(client).get_telescopes(name="nicer")[0]
    telescope_id = telescope.id
    if telescope.instruments:
        instrument_id = telescope.instruments[0].id

    # Initialize schedule
    schedule = transform_to_across_schedule(
        telescope_id=telescope_id,
        data=nicer_planned_df,
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=sdk.ScheduleFidelity.LOW,
    )

    nicer_obs = list(nicer_df.itertuples())

    # Transform observations
    schedule.observations = [
        transform_to_across_observation(
            instrument_id,
            cast(ObservationRow, obs),
        )
        for obs in nicer_obs
    ]

    # Post schedule
    sdk.ScheduleApi(client).create_schedule(schedule)


@repeat_at(cron="18 23 * * *", logger=logger)
async def entrypoint():
    try:
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Schedule ingestion encountered an unknown error.", err=e)
