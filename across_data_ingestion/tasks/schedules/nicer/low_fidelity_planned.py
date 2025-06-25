import logging

import pandas as pd
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_WEEK
from ....util import across_api
from ..types import AcrossObservation, AcrossSchedule

logger = logging.getLogger("uvicorn.error")

NICER_BANDPASS = {
    "min": 0.2,
    "max": 12.0,
    "unit": "keV",
    "filter_name": "NICER XTI",
}

NICER_TIMELINE_FILE = (
    "https://heasarc.gsfc.nasa.gov/docs/nicer/schedule/obs_pred_timeline_detail.csv"
)


def query_nicer_catalog() -> pd.DataFrame | None:
    """
    Queries the NICER HEASARC catalog for all NICER observations
    """
    try:
        df = pd.read_csv(NICER_TIMELINE_FILE)
    except Exception:
        return None

    return df


def nicer_schedule(
    telescope_id: str, data: pd.DataFrame, status: str, fidelity: str
) -> AcrossSchedule | dict:
    """
    Creates a NICER schedule from the provided data.
    If the data is empty, it returns an empty dictionary."""
    if len(data) == 0:
        # Empty schedule, return
        return {}

    begin = Time(f"{min(data["Start"])}", format="isot").isot
    end = Time(f"{max(data["Stop"])}", format="isot").isot

    return {
        "telescope_id": telescope_id,
        "name": f"nicer_obs_planned_{begin.split('T')[0]}_{end.split('T')[0]}",
        "date_range": {
            "begin": begin,
            "end": end,
        },
        "status": status,
        "fidelity": fidelity,
    }


def nicer_observation(instrument_id: str, row: dict) -> AcrossObservation:
    """
    Creates a NICER observation from the provided row of data.
    """
    return {
        "instrument_id": instrument_id,
        "object_name": f"{row["Target"]}",
        "pointing_position": {
            "ra": f"{row["RightAscension"]}",
            "dec": f"{row["Declination"]}",
        },
        "object_position": {
            "ra": f"{row["RightAscension"]}",
            "dec": f"{row["Declination"]}",
        },
        "date_range": {
            "begin": Time(f"{row["Start"]}", format="isot").isot,
            "end": Time(f"{row["Stop"]}", format="isot").isot,
        },
        "external_observation_id": f"{row["ObsID"]}",
        "type": "imaging",
        "status": "planned",
        "exposure_time": float(row["Duration"]),
        "bandpass": NICER_BANDPASS,
        "pointing_angle": 0.0,
    }


def ingest(schedule_modes: list[str] = ["Scheduled"]) -> AcrossSchedule | dict:
    """
    Method that posts NICER low fidelity observing schedules via the known webfile:
        https://heasarc.gsfc.nasa.gov/docs/nicer/schedule/obs_pred_timeline_detail.csv

    If the file is not available, it will pass and return an empty schedule. If the file is available,
    it will grab all observations with the Mode "Scheduled", and create a schedule with the planned
    observations. Then it will then POST the schedule to the ACROSS server.
    """
    nicer_df = query_nicer_catalog()
    if nicer_df is None:
        logger.error("Failed to read NICER timeline file")
        return {}

    # Only get planned observations
    nicer_planned = nicer_df.loc[nicer_df["Mode"].isin(schedule_modes)]
    if nicer_planned.empty:
        logger.info(f"No {schedule_modes} observations found in NICER timeline file.")
        return {}

    # GET Telescope by name
    nicer_telescope_info = across_api.telescope.get({"name": "nicer"})[0]
    telescope_id = nicer_telescope_info["id"]
    instrument_id = nicer_telescope_info["instruments"][0]["id"]

    # Initialize schedule
    schedule = nicer_schedule(
        telescope_id=telescope_id,
        data=nicer_planned,
        status="planned",
        fidelity="low",
    )

    # Transform dataframe to list of dictionaries
    schedule_observations = nicer_planned.to_dict(orient="records")

    # Transform observations
    schedule["observations"] = [
        nicer_observation(instrument_id, row) for row in schedule_observations
    ]

    # Post schedule
    across_api.schedule.post(dict(schedule))

    return schedule


@repeat_every(seconds=2 * SECONDS_IN_A_WEEK)  # BiWeekly
def entrypoint():
    current_time = Time.now()

    try:
        schedule = ingest()
        logger.info(f"Ingestion completed: {__name__} ran at {current_time}")
        return schedule
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(f"{__name__} encountered an error {e} at {current_time}")
        return
