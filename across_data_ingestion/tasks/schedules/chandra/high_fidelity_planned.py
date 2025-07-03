import json
from datetime import datetime, timedelta, timezone
from typing import Literal

import structlog
from astropy.table import Table  # type: ignore[import-untyped]
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
    instrument_info: dict, row: Table.Row
) -> tuple[str, str]:
    """
    Constructs the instrument name from the observation parameters and
    returns both the name and the instrument id in across-server
    """
    if "ACIS" in row["instrument"]:
        instrument_full_name = "Advanced CCD Imaging Spectrometer"
        if row["grating"] == "NONE" and row["exposure_mode"] != "CC":
            instrument_name = "ACIS"
        elif row["grating"] == "HETG":
            instrument_name = "ACIS-HETG"
            instrument_full_name += " - High Energy Transmission Grating"
        elif row["grating"] == "LETG":
            instrument_name = "ACIS-LETG"
            instrument_full_name += " - Low Energy Transmission Grating"
        elif row["exposure_mode"] == "CC":
            instrument_name = "ACIS-CC"
            instrument_full_name += " - Continuous Clocking Mode"
        else:
            logger.error(
                "Could not parse observation parameters for correct instrument"
            )
            return "", ""
    elif "HRC" in row["instrument"]:
        instrument_full_name = "High Resolution Camera"
        if row["exposure_mode"] != "":
            instrument_name = "HRC-Timing"
            instrument_full_name += " - Timing Mode"
        elif row["grating"] == "NONE":
            instrument_name = "HRC"
        elif row["grating"] == "HETG":
            instrument_name = "HRC-HETG"
            instrument_full_name += " - High Energy Transmission Grating"
        elif row["grating"] == "LETG":
            instrument_name = "HRC-LETG"
            instrument_full_name += " - Low Energy Transmission Grating"
        else:
            logger.error(
                "Could not parse observation parameters for correct instrument"
            )
            return "", ""
    else:
        logger.error("Could not parse observation parameters for correct instrument")
        return "", ""

    instrument_id = instrument_info[instrument_full_name]
    return instrument_name, instrument_id


def create_schedule(telescope_id: str, data: Table) -> AcrossSchedule | dict:
    begin = f"{min(data["start_date"])}"
    end = f"{max(data["start_date"])}"
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
    # Build dict of instrument name: id pairs for lookup later
    instrument_info_dict = {
        instrument["name"]: instrument["id"]
        for instrument in chandra_telescope_info["instruments"]
    }

    schedules: list[AcrossSchedule | dict] = []

    # Query Chandra TAP service to get most of the observation parameters
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    query = "select o.obsid, o.start_date, o.ra, o.dec, o.instrument, "
    query += f"o.grating, o.exposure_mode from cxc.observation o where o.start_date > '{now}' "
    query += "and o.status='scheduled' order by o.start_date desc"

    voservice = VOService(CHANDRA_TAP_URL)
    observations_table = await voservice.query(query)
    if not observations_table:
        logger.warn("No response returned for async request")
        return None

    schedule = create_schedule(telescope_id, observations_table)

    observations = {}
    for row in observations_table:
        # Get correct instrument id from the observation parameters
        instrument_name, instrument_id = get_instrument_info_from_observation(
            instrument_info_dict, row
        )
        if not instrument_name:
            logger.error("Cannot parse observations with unknown instrument")
            return None
        observation_data = dict(row)
        observation_data["instrument_id"] = instrument_id
        observation_data["instrument_name"] = instrument_name

        # Add observation_id: observation_data pair to dict for lookup later
        observation_id = str(observation_data.pop("obsid"))
        observations[observation_id] = observation_data

    # Query TAP again to get planned exposure time for observation IDs
    exposure_time_query = "select obs_id, target_name, t_plan_exptime from ivoa.obsplan"
    exposure_time_query += f" where obs_id in ('{"', '".join(str(obsid) for obsid in observations.keys())}')"
    voservice = VOService(CHANDRA_TAP_URL)
    exposure_time_table = await voservice.query(exposure_time_query)
    if not exposure_time_table:
        logger.warn("No exposure time for observations found")
        return None

    for row in exposure_time_table:
        exposure_time_data = dict(row)
        observation_id = str(exposure_time_data.pop("obs_id"))
        # Add new observation parameters to existing observation_data dict
        observations[observation_id].update(exposure_time_data)

    across_observations: list[AcrossObservation] = [
        transform_to_observation(observation_id, observation)
        for observation_id, observation in observations.items()
    ]

    schedule["observations"] = across_observations
    logger.info(json.dumps(schedule, indent=4))  # TODO: Remove
    schedules.append(schedule)

    across_api.schedule.post(schedule)

    return None


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
