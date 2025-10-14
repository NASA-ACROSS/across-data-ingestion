from datetime import datetime

import pydantic
import structlog
from across.tools import tle as tle_tool
from fastapi_utilities import repeat_at  # type: ignore

from ...core import config
from ...util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


class NoradSatellite(pydantic.BaseModel):
    id: int
    name: str


def extract_norad_satellites(
    observatories: list[sdk.Observatory],
) -> list[NoradSatellite]:
    """
    Extracts NORAD satellite name: NORAD ID pairs from the
    observatory data returned by ACROSS server
    """
    satellites: list[NoradSatellite] = []

    for observatory in observatories:
        if observatory.ephemeris_types:
            tle_ephems = [
                ephem
                for ephem in observatory.ephemeris_types
                if ephem.ephemeris_type == sdk.EphemerisType.TLE
            ]

            # observatories will only ever have a single tle ephem.
            tle_parameters = tle_ephems[0].parameters.actual_instance

            assert isinstance(tle_parameters, sdk.TLEParameters)

            satellites.append(
                NoradSatellite(
                    id=tle_parameters.norad_id,
                    name=tle_parameters.norad_satellite_name,
                )
            )

    return satellites


def ingest() -> None:
    """
    Method that queries ACROSS server for all observatories with TLE ephemerides,
    extracts the NORAD IDs and names for those observatories from the returned data package,
    fetches up-to-date TLEs from Spacetrack, and uploads them to ACROSS server.
    """
    observatories = sdk.ObservatoryApi(client).get_observatories(
        ephemeris_type=[sdk.EphemerisType.TLE]
    )
    satellites = extract_norad_satellites(observatories)

    for satellite in satellites:
        tle = tle_tool.get_tle(
            norad_id=satellite.id,
            epoch=datetime.now(),
            spacetrack_user=config.SPACETRACK_USER,
            spacetrack_pwd=config.SPACETRACK_PWD,
        )

        if tle:
            across_tle = sdk.TLECreate(
                norad_id=satellite.id,
                satellite_name=satellite.name,
                tle1=tle.tle1,
                tle2=tle.tle2,
            )

            try:
                sdk.TLEApi(client).create_tle(across_tle)
                logger.info("Created new TLE", satellite=satellite.model_dump())
            except sdk.ApiException as err:
                if err.status == 409:
                    logger.warning("TLE Already Exists", name=across_tle.satellite_name, norad_id=across_tle.norad_id, epoch=tle.epoch)
                else:
                    raise err
            
        else:
            logger.warning("Could not fetch TLE", satellite=satellite.model_dump())


@repeat_at(cron="34 4 * * *", logger=logger)
async def entrypoint() -> None:
    try:
        ingest()
        logger.info("Completed TLE ingestion.")
        return

    except Exception as e:
        logger.error(
            "TLE ingestion encountered an unknown error: ",
            err=e,
            exc_info=True,
        )
        return
