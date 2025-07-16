from datetime import datetime

import structlog
from across.tools.tle import get_tle
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ...core import config  # type: ignore[import-untyped]
from ...core.constants import SECONDS_IN_A_DAY
from ...util import across_api

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


def extract_norad_info_from_tle_ephems_for_observatories(
    observatory_info: list[dict],
) -> dict:
    """
    Extracts NORAD satellite name: NORAD ID pairs from the
    observatory data returned by ACROSS server
    """
    norad_info = {}

    for observatory in observatory_info:
        tle_ephem_parameters = [
            ephem
            for ephem in observatory["ephemeris_types"]
            if ephem["ephemeris_type"] == "tle"
        ][0]["parameters"]
        norad_info[tle_ephem_parameters["norad_satellite_name"]] = tle_ephem_parameters[
            "norad_id"
        ]

    return norad_info


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
    observatory_info = across_api.observatory.get({"ephemeris_type": "tle"})
    tle_parameters = extract_norad_info_from_tle_ephems_for_observatories(
        observatory_info
    )

    for norad_name, norad_id in tle_parameters.items():
        tle = retrieve_tle_from_spacetrack(norad_id=norad_id)
        if len(tle):
            tle["satellite_name"] = norad_name
            # Remove the epoch from the returned data package before POSTing to the server
            tle.pop("epoch") if "epoch" in tle.keys() else None
            across_api.tle.post(tle)

            logger.info(f"Successfully posted TLE for {norad_name} to ACROSS server")
        else:
            logger.warn(f"Could not fetch TLE for {norad_name}")


@repeat_every(seconds=SECONDS_IN_A_DAY)
def entrypoint() -> None:
    current_time = Time.now()

    try:
        ingest()
        logger.info(f"{__name__} ran at {current_time}")
        return

    except Exception as e:
        logger.error(f"{__name__} encountered an error {e} at {current_time}")
        return
