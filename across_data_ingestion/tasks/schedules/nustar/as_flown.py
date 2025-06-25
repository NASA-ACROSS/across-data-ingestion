import json
from datetime import datetime, timedelta

import structlog
from astropy.table import Table  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from astroquery.heasarc import Heasarc  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_DAY, SECONDS_IN_A_WEEK
from ..types import AcrossObservation, AcrossSchedule

# from ....util import across_api # TODO: Uncomment when integrating with server

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

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
    except ValueError as err:
        logger.warn(
            "Could not query for NuMASTER catalog on HEASARC",
            start_time=start_time,
            err=err,
        )
        return None
    except Exception as err:
        logger.error("Reading NuSTAR HEASARC catalog unexpectedly failed", err=err)
        return None

    table = result.to_table()

    return table


def create_schedule(telescope_id: str, data: Table) -> AcrossSchedule | dict:
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


def transform_to_observation(instrument_id: str, row: Table.Row) -> AcrossObservation:
    return {
        "instrument_id": instrument_id,
        "object_name": f"{row["name"]}",
        "pointing_position": {
            "ra": f"{row["ra"]}",
            "dec": f"{row["dec"]}",
        },
        "object_position": {
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


def ingest() -> None:
    """
    Method that POSTs NuSTAR as-flown observing schedules to the ACROSS server
    Queries completed observations via the HEASARC `NUMASTER` catalog
    """
    last_week = datetime.now() - timedelta(days=7)
    last_week_mjd = Time(last_week).mjd

    nustar_observation_data = query_nustar_catalog(last_week_mjd)
    if nustar_observation_data is None:
        logger.error("Could not query NuSTAR observations from HEASARC")
        return None

    # Get telescope and instrument IDs
    telescope_id = "nustar_telescope_uuid"
    instrument_id = "nustar_instrument_uuid"

    # Initialize list of schedules to append
    schedules: list[AcrossSchedule | dict] = []

    schedule = create_schedule(telescope_id, nustar_observation_data)

    if not schedule:
        # No observations found for the queried time range, return empty list
        logger.warn("Empty table returned from HEASARC NUMASTER catalog")
        return None
    observations: list[AcrossObservation] = []

    for row in nustar_observation_data:
        if row["observation_mode"] == "SCIENCE":
            across_observation = transform_to_observation(instrument_id, row)
            observations.append(across_observation)

    schedule["observations"] = observations
    logger.info(json.dumps(schedule))  # TODO: POST to the API endpoint
    schedules.append(schedule)

    return None


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
def entrypoint() -> None:
    try:
        ingest()
        logger.info("NuSTAR as-flown schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("NuSTAR as-flown schedule ingestion encountered an error", err=e)
        return
