from datetime import datetime

import structlog
from across.tools.tle import get_tle
from fastapi_utils.tasks import repeat_every

from ...core import config  # type: ignore[import-untyped]
from ...core.constants import SECONDS_IN_A_DAY
from ...util import across_api

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


def extract_norad_info_from_tle_ephems_for_observatories(
    observatories: list[dict],
) -> list[dict]:
    """
    Extracts NORAD satellite name: NORAD ID pairs from the
    observatory data returned by ACROSS server
    """
    norad_entries = []

    for observatory in observatories:
        tle_ephems = [
            ephem
            for ephem in observatory["ephemeris_types"]
            if ephem["ephemeris_type"] == "tle"
        ]

        # observatories will only ever have a single tle ephem.
        tle_parameters = tle_ephems[0]["parameters"]

        norad_entries.append(
            {
                "satellite_name": tle_parameters["norad_satellite_name"],
                "id": tle_parameters["norad_id"],
            }
        )

    return norad_entries


def retrieve_tle_from_spacetrack(norad_id: int) -> dict:
    """
    Uses across-tools get_tle function to retrieve most up-to-date TLE f
    rom Spacetrack using a satellite NORAD ID and Spacetrack user credentials.
    """
    tle = get_tle(
        norad_id=norad_id,
        epoch=datetime.now(),
        spacetrack_user=config.SPACETRACK_USER,
        spacetrack_pwd=config.SPACETRACK_PWD,
    )
    return tle.model_dump() if tle else {}


def ingest() -> None:
    """
    Method that queries ACROSS server for all observatories with TLE ephemerides,
    extracts the NORAD IDs and names for those observtories from the returned data package,
    fetches up-to-date TLEs from Spacetrack, and uploads them to ACROSS server.
    """
    observatories = across_api.observatory.get({"ephemeris_type": "tle"})
    norad_entries = extract_norad_info_from_tle_ephems_for_observatories(observatories)

    for entry in norad_entries:
        tle = retrieve_tle_from_spacetrack(norad_id=entry["id"])

        if len(tle):
            tle["satellite_name"] = entry["satellite_name"]
            # Remove the epoch from the returned data package before POSTing to the server
            tle.pop("epoch") if "epoch" in tle.keys() else None
            across_api.tle.post(tle)

            logger.info("Fetched TLE", norad_name=entry["satellite_name"])
        else:
            logger.warn("Could not fetch TLE", norad_name=entry["satellite_name"])


@repeat_every(seconds=SECONDS_IN_A_DAY)
def entrypoint() -> None:
    try:
        ingest()
        logger.info("Completed TLE ingestion.")
        return

    except Exception as e:
        logger.error("TLE ingestion encountered an unknown error: ", err=e)
        return
