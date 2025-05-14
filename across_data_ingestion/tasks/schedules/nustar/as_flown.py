import logging
from datetime import datetime, timedelta

from astropy.table import Table  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from astroquery.heasarc import Heasarc  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

# from ....util import across_api # TODO: Uncomment when integrating with server

logger = logging.getLogger("uvicorn.error")

SECONDS_IN_A_DAY = 60 * 60 * 24
SECONDS_IN_A_WEEK = 60 * 60 * 24 * 7
# Bandpass information found here: https://www.nustar.caltech.edu/page/optics
NUSTAR_BANDPASS = {
    "filter_name": "NuSTAR",
    "min": 3.0,
    "max": 78.4,
    "type": "ENERGY",
    "unit": "keV",
}


def query_nustar_catalog(start_time: int) -> Table | None:
    """
    Queries the NuMASTER HEASARC catalog for all NuSTAR observations
    beginning after the input `start_time`
    """
    try:
        result = Heasarc.query_tap(f"SELECT * from numaster where time > {start_time}")
    except ValueError:
        logger.error(f"{__name__}: Could not query for NUMASTER catalog on HEASARC")
        return None
    except Exception:
        # We got an unexpected error
        logger.error(
            f"{__name__}: Reading NuSTAR HEASARC catalog unexpectedly failed",
            exc_info=True,
        )
        return None

    table = result.to_table()

    return table


def create_schedule(
    telescope_id: str, data: Table
) -> dict[str, str | dict[str, str] | list]:
    if len(data) == 0:
        # Empty schedule, return
        return {}

    begin = Time(f"{min(data["time"])}", format="mjd").isot
    end = Time(f"{max(data["end_time"])}", format="mjd").isot

    return {
        "telescope_id": telescope_id,
        "name": f"nustar_as_flown_{begin.split('T')[0]}_{end.split('T')[0]}",
        "date_range": {
            "begin": begin,
            "end": end,
        },
        "status": "performed",
        "fidelity": "high",
    }


def create_observation(instrument_id: str, row: Table.Row):
    obs = {
        "instrument_id": instrument_id,
        "object_name": f"{row["name"]}",
        "pointing_position": {
            "ra": f"{row["ra"]}",
            "dec": f"{row["dec"]}",
        },
        "date_range": {
            "begin": Time(f"{row['time']}", format="mjd").isot,
            "end": Time(f"{row["end_time"]}", format="mjd").isot,
        },
        "external_observation_id": f"{row["obsid"]}",
        "type": "timing",
        "status": "performed",
        "pointing_angle": float(f"{row["roll_angle"]}"),
        "exposure_time": float(row["end_time"] - row["time"]) * SECONDS_IN_A_DAY,
        "bandpass": NUSTAR_BANDPASS,
    }
    return obs


def ingest() -> list | None:
    """
    Method that POSTs NuSTAR as-flown observing schedules to the ACROSS server
    Queries completed observations via the HEASARC `NUMASTER` catalog
    """
    last_week = datetime.now() - timedelta(days=7)
    last_week_mjd = Time(last_week).mjd

    observation_table = query_nustar_catalog(last_week_mjd)
    if observation_table is None:
        logger.error(f"{__name__}: Could not query NuSTAR observations from HEASARC")
        return None

    # Get telescope and instrument IDs
    telescope_id = "nustar_telescope_uuid"
    instrument_id = "nustar_instrument_uuid"

    # Initialize list of schedules to append
    schedules: list[dict] = []

    schedule = create_schedule(telescope_id, observation_table)
    if not schedule:
        # No observations found for the queried time range, return empty list
        logger.warning(
            f"{__name__}: Empty table returned from HEASARC NUMASTER catalog"
        )
        return schedules
    observations: list[dict] = []
    for row in observation_table:
        if row["observation_mode"] == "SCIENCE":
            obs = create_observation(instrument_id, row)
            observations.append(obs)

    schedule["observations"] = observations
    logger.info(schedule)  # TODO: POST to the API endpoint
    schedules.append(schedule)

    return schedules


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
def entrypoint() -> None:
    current_time = Time.now()

    try:
        ingest()
        logger.info(f"{__name__} ran at {current_time}")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(f"{__name__} encountered an error {e} at {current_time}")
        return
