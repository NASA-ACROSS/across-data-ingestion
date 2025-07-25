from datetime import datetime, timedelta, timezone
from typing import Literal

import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_DAY
from ....util import across_api
from ....util.vo_service import VOService
from ..types import AcrossObservation, AcrossSchedule

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


# Chandra has multiple instruments with different bandpasses
CHANDRA_ACIS_BANDPASS: dict[str, str | float] = {
    "filter_name": "Chandra ACIS",
    "min": 0.1,
    "max": 10.0,
    "type": "ENERGY",
    "unit": "keV",
}

CHANDRA_HETG_BANDPASS: dict[str, str | float] = {
    "filter_name": "Chandra HETG",
    "min": 0.6,
    "max": 10.0,
    "type": "ENERGY",
    "unit": "keV",
}

CHANDRA_LETG_BANDPASS: dict[str, str | float] = {
    "filter_name": "Chandra LETG",
    "min": 0.1,
    "max": 6.0,
    "type": "ENERGY",
    "unit": "keV",
}

CHANDRA_HRC_BANDPASS: dict[str, str | float] = {
    "filter_name": "Chandra HRC",
    "min": 0.1,
    "max": 10.0,
    "type": "ENERGY",
    "unit": "keV",
}

CHANDRA_BANDPASSES: dict[str, dict[str, str | float]] = {
    "ACIS": CHANDRA_ACIS_BANDPASS,
    "ACIS-HETG": CHANDRA_HETG_BANDPASS,
    "ACIS-LETG": CHANDRA_LETG_BANDPASS,
    "ACIS-CC": CHANDRA_ACIS_BANDPASS,
    "HRC": CHANDRA_HRC_BANDPASS,
    "HRC-HETG": CHANDRA_HETG_BANDPASS,
    "HRC-LETG": CHANDRA_LETG_BANDPASS,
    "HRC-Timing": CHANDRA_HRC_BANDPASS,
}


# Each Chandra instrument has a different observation type
CHANDRA_OBSERVATION_TYPES: dict[str, Literal["imaging", "spectroscopy", "timing"]] = {
    "ACIS": "imaging",
    "ACIS-HETG": "spectroscopy",
    "ACIS-LETG": "spectroscopy",
    "ACIS-CC": "timing",
    "HRC": "imaging",
    "HRC-HETG": "spectroscopy",
    "HRC-LETG": "spectroscopy",
    "HRC-Timing": "timing",
}

CHANDRA_TAP_URL = "https://cda.cfa.harvard.edu/cxctap/async"


def get_instrument_info_from_observation(
    instruments: list[dict], tap_row: dict
) -> tuple[str, str]:
    """
    Constructs the instrument name from the observation parameters and
    returns both the name and the instrument id in across-server
    """
    instrument_dict = {
        instrument["short_name"]: instrument["id"] for instrument in instruments
    }
    if "ACIS" in tap_row["instrument"]:
        if tap_row["grating"] == "NONE" and tap_row["exposure_mode"] != "CC":
            instrument_name = "ACIS"
        elif tap_row["grating"] in ["HETG", "LETG"]:
            instrument_name = f"ACIS-{tap_row["grating"]}"
        elif tap_row["exposure_mode"] == "CC":
            instrument_name = "ACIS-CC"
        else:
            logger.error(
                "Could not parse observation parameters for correct instrument"
            )
            return "", ""
    elif "HRC" in tap_row["instrument"]:
        if tap_row["exposure_mode"] != "":
            instrument_name = "HRC-Timing"
        elif tap_row["grating"] == "NONE":
            instrument_name = "HRC"
        elif tap_row["grating"] in ["HETG", "LETG"]:
            instrument_name = f"HRC-{tap_row["grating"]}"
        else:
            logger.error(
                "Could not parse observation parameters for correct instrument"
            )
            return "", ""
    else:
        logger.error("Could not parse observation parameters for correct instrument")
        return "", ""

    instrument_id = instrument_dict[instrument_name]
    return instrument_name, instrument_id


def create_schedule(telescope_id: str, observations: dict) -> AcrossSchedule | dict:
    begin = f"{min([data["start_date"] for data in observations.values()])}"
    end = f"{max([data["start_date"] for data in observations.values()])}"
    return {
        "telescope_id": telescope_id,
        "name": f"chandra_high_fidelity_planned_{begin.split('T')[0]}_{end.split('T')[0]}",
        "date_range": {
            "begin": begin,
            "end": end,
        },
        "status": "scheduled",
        "fidelity": "high",
    }


async def get_observation_parameters_from_tap(instruments: list[dict]) -> dict:
    """Query Chandra TAP service to get most of the observation parameters"""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    query = "select o.obsid, o.start_date, o.ra, o.dec, o.instrument, "
    query += f"o.grating, o.exposure_mode from cxc.observation o where o.start_date > '{now}' "
    query += "and o.status='scheduled' order by o.start_date desc"

    voservice = VOService(CHANDRA_TAP_URL)
    observations_table = await voservice.query(query)
    if not observations_table:
        logger.info("No observations found")
        return {}

    observations = {}
    for row in observations_table:
        # Get correct instrument id from the observation parameters
        row_dict = dict(row)
        instrument_name, instrument_id = get_instrument_info_from_observation(
            instruments, row_dict
        )
        if not instrument_name:
            logger.error("Cannot parse observations with unknown instrument")
            return {}
        observation_data = dict(row)
        observation_data["instrument_id"] = instrument_id
        observation_data["instrument_name"] = instrument_name

        # Add observation_id: observation_data pair to dict for lookup later
        observation_id = str(observation_data.pop("obsid"))
        observations[observation_id] = observation_data

    return observations


async def update_exposure_time_from_tap(observations: dict) -> dict:
    """Query TAP again to get planned exposure time for observation IDs"""
    exposure_time_query = "select obs_id, target_name, t_plan_exptime from ivoa.obsplan"
    exposure_time_query += f" where obs_id in ('{"', '".join(str(obsid) for obsid in observations.keys())}')"
    voservice = VOService(CHANDRA_TAP_URL)
    exposure_time_table = await voservice.query(exposure_time_query)
    if not exposure_time_table:
        logger.warn("No exposure time for observations found")
        return {}

    for row in exposure_time_table:
        exposure_time_data = dict(row)
        observation_id = str(exposure_time_data.pop("obs_id"))
        # Add new observation parameters to existing observation_data dict
        observations[observation_id].update(exposure_time_data)
    return observations


def transform_to_observation(
    observation_id: str, observation_data: dict
) -> AcrossObservation:
    instrument_id = observation_data["instrument_id"]
    instrument_name = observation_data["instrument_name"]
    begin = observation_data["start_date"]
    end = (
        Time(begin, format="isot")
        + timedelta(seconds=observation_data["t_plan_exptime"])
    ).isot

    return {
        "instrument_id": instrument_id,
        "object_name": observation_data["target_name"],
        "pointing_position": {
            "ra": f"{observation_data["ra"]}",
            "dec": f"{observation_data["dec"]}",
        },
        "object_position": {
            "ra": f"{observation_data["ra"]}",
            "dec": f"{observation_data["dec"]}",
        },
        "date_range": {
            "begin": f"{begin}",
            "end": f"{end}",
        },
        "external_observation_id": observation_id,
        "type": CHANDRA_OBSERVATION_TYPES[instrument_name],
        "status": "scheduled",
        "pointing_angle": 0.0,
        "exposure_time": float(observation_data["t_plan_exptime"]),
        "bandpass": CHANDRA_BANDPASSES[instrument_name],
    }


async def ingest() -> None:
    """
    Ingests all scheduled Chandra observations by submitting a TAP query using
    the Chandra VO service.

    Performs queries of two different tables to retrieve all required parameters.
    Transforms the data into the AcrossSchedule and AcrossObservation interfaces,
    matches the correct Chandra instrument given the observation parameters,
    and pushes the schedule to the across-server endpoint.
    """
    # GET Telescope by name
    chandra_telescope_info = across_api.telescope.get({"name": "chandra"})[0]
    telescope_id = chandra_telescope_info["id"]

    schedules: list[AcrossSchedule | dict] = []

    logger.info(chandra_telescope_info["instruments"])

    chandra_observation_data = await get_observation_parameters_from_tap(
        chandra_telescope_info["instruments"]
    )
    if not len(chandra_observation_data):
        return

    schedule = create_schedule(telescope_id, chandra_observation_data)
    chandra_observations_with_exptimes = await update_exposure_time_from_tap(
        chandra_observation_data
    )
    if not len(chandra_observations_with_exptimes):
        return

    across_observations: list[AcrossObservation] = [
        transform_to_observation(observation_id, observation)
        for observation_id, observation in chandra_observations_with_exptimes.items()
    ]

    schedule["observations"] = across_observations
    schedules.append(schedule)

    across_api.schedule.post(schedule)

    return


@repeat_every(seconds=SECONDS_IN_A_DAY)  # Daily
async def entrypoint() -> None:
    try:
        await ingest()
        logger.info("Chandra high-fidelity planned schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(
            "Chandra high-fidelity planned schedule ingestion encountered an error",
            err=e,
        )
        return
