from datetime import datetime, timedelta

import structlog
from astropy.table import Table  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from astroquery.heasarc import Heasarc  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_DAY, SECONDS_IN_A_WEEK
from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

# Bandpass information found here: https://www.nustar.caltech.edu/page/optics
NUSTAR_BANDPASS = sdk.EnergyBandpass.model_validate(
    {
        "filter_name": "NuSTAR",
        "min": 3.0,
        "max": 78.4,
        "type": "ENERGY",
        "unit": sdk.EnergyUnit.KEV,
    }
)


def query_nustar_catalog(start_time: int) -> Table:
    """
    Queries the NuMASTER HEASARC catalog for all NuSTAR observations
    beginning after the input `start_time`
    """
    try:
        result = Heasarc.query_tap(f"SELECT * FROM numaster WHERE time > {start_time}")
    except ValueError as err:
        logger.warning(
            "Could not query for NuMASTER catalog on HEASARC",
            start_time=start_time,
            err=err,
        )
        return Table()

    table = result.to_table()

    return table


def create_schedule(telescope_id: str, data: Table) -> sdk.ScheduleCreate:
    begin = Time(f"{min(data['time'])}", format="mjd").isot
    end = Time(f"{max(data['end_time'])}", format="mjd").isot

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"nustar_as_flown_{begin.split('T')[0]}_{end.split('T')[0]}",
        date_range=sdk.DateRange(begin=begin, end=end),
        status=sdk.ScheduleStatus.PERFORMED,
        fidelity=sdk.ScheduleFidelity.HIGH,
        observations=[],
    )


def transform_to_observation(
    instrument_id: str, row: Table.Row
) -> sdk.ObservationCreate:
    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=f"{row['name']}",
        pointing_position=sdk.Coordinate.model_validate(
            {
                "ra": float(row["ra"]),
                "dec": float(row["dec"]),
            }
        ),
        date_range=sdk.DateRange.model_validate(
            {
                "begin": Time(f"{row['time']}", format="mjd").isot,
                "end": Time(f"{row['end_time']}", format="mjd").isot,
            }
        ),
        external_observation_id=f"{row['obsid']}",
        type=sdk.ObservationType.TIMING,
        status=sdk.ObservationStatus.PERFORMED,
        pointing_angle=float(f"{row['roll_angle']}"),
        exposure_time=float(row["end_time"] - row["time"]) * SECONDS_IN_A_DAY,
        bandpass=sdk.Bandpass(NUSTAR_BANDPASS),
    )


def ingest() -> None:
    """
    Method that POSTs NuSTAR as-flown observing schedules to the ACROSS server
    Queries completed observations via the HEASARC `NUMASTER` catalog
    """
    last_week = datetime.now() - timedelta(days=7)
    last_week_mjd = Time(last_week).mjd

    nustar_observation_data = query_nustar_catalog(last_week_mjd)
    if len(nustar_observation_data) == 0:
        logger.info(
            "No new observations found.",
            last_week=last_week.isoformat(),
        )

        return

    logger.debug("Found observations...", observations=len(nustar_observation_data))

    # Get telescope and instrument IDs
    (telescope,) = sdk.TelescopeApi(client).get_telescopes(name="NuSTAR")
    (instrument,) = sdk.InstrumentApi(client).get_instruments(name="FPM A/B")

    schedule = create_schedule(telescope.id, nustar_observation_data)

    for row in nustar_observation_data:
        if row["observation_mode"] == "SCIENCE":
            across_observation = transform_to_observation(instrument.id, row)
            schedule.observations.append(across_observation)

    try:
        sdk.ScheduleApi(client).create_schedule(schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.warning("Schedule already exists.", err=err.__dict__)
        else:
            raise err


@repeat_every(seconds=SECONDS_IN_A_WEEK)
async def entrypoint() -> None:
    try:
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Schedule ingestion encountered an error", err=e)
