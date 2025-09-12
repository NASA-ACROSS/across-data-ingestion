import pandas as pd
import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore[import-untyped]

from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

PLANNED_SCHEDULE_TABLE_URL = (
    "https://nustarsoc.caltech.edu/NuSTAR_Public/NuSTAROperationSite/Schedule.php"
)

NUSTAR_BANDPASS = sdk.EnergyBandpass.model_validate(
    {
        "filter_name": "NuSTAR",
        "min": 3.0,
        "max": 78.4,
        "type": "ENERGY",
        "unit": sdk.EnergyUnit.KEV,
    }
)


def read_planned_schedule_table() -> pd.DataFrame:
    """Read the planned schedule table as a pandas DataFrame"""
    try:
        dfs: list[pd.DataFrame] = pd.read_html(
            PLANNED_SCHEDULE_TABLE_URL, flavor="bs4", header=0
        )
    except ValueError as err:
        logger.warning(
            "Could not find planned schedule table.",
            err=err,
        )
        return pd.DataFrame([])
    if len(dfs) > 0:
        schedule_df = dfs[0]
        return schedule_df
    logger.warning("Could not read planned schedule table")
    return pd.DataFrame([])


def create_schedule(telescope_id: str, data: pd.DataFrame) -> sdk.ScheduleCreate:
    """Create schedule metadata from schedule DataFrame"""
    begin = Time(f"{min(data['obs_start'])}", format="yday").isot
    end = Time(f"{max(data['obs_end'])}", format="yday").isot

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"nustar_low_fidelity_planned_{begin.split('T')[0]}_{end.split('T')[0]}",
        date_range=sdk.DateRange(begin=begin, end=end),
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=sdk.ScheduleFidelity.LOW,
        observations=[],
    )


def transform_to_observation(
    instrument_id: str, row: pd.Series
) -> sdk.ObservationCreate:
    """Create ACROSS observation for given instrument ID and observation row"""
    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=f"{row['Name']}",
        pointing_position=sdk.Coordinate.model_validate(
            {
                "ra": float(row["J2000 RA"]),
                "dec": float(row["J2000 Dec"]),
            }
        ),
        date_range=sdk.DateRange.model_validate(
            {
                "begin": Time(f"{row['obs_start']}", format="yday").isot,
                "end": Time(f"{row['obs_end']}", format="yday").isot,
            }
        ),
        external_observation_id=f"{row['sequenceID']}",
        type=sdk.ObservationType.TIMING,
        status=sdk.ObservationStatus.PLANNED,
        pointing_angle=0.0,  # Assume no roll angle
        exposure_time=float(row["Exp"]) * 1000,  # Given in ks
        bandpass=sdk.Bandpass(NUSTAR_BANDPASS),
    )


def ingest() -> None:
    """Ingest planned NuSTAR observations by reading the planned schedule table"""
    nustar_observation_data = read_planned_schedule_table()
    if len(nustar_observation_data) == 0:
        logger.info("No planned schedule data found.")
        return

    logger.debug("Found observations...", observations=len(nustar_observation_data))

    # Get telescope and instrument IDs
    (telescope,) = sdk.TelescopeApi(client).get_telescopes(name="NuSTAR")
    (instrument,) = sdk.InstrumentApi(client).get_instruments(name="FPM A/B")

    schedule = create_schedule(telescope.id, nustar_observation_data)

    for _, row in nustar_observation_data.iterrows():
        across_observation = transform_to_observation(instrument.id, row)
        schedule.observations.append(across_observation)

    try:
        sdk.ScheduleApi(client).create_schedule(schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.warning("Schedule already exists.", err=err.__dict__)
        else:
            raise err


@repeat_at(cron="7 1 * * *", logger=logger)
async def entrypoint() -> None:
    try:
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Schedule ingestion encountered an error", err=e)
