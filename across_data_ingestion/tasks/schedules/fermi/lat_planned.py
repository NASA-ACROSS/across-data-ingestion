import logging
import re
from datetime import datetime
from typing import Literal
from urllib.error import HTTPError

import astropy.units as u  # type: ignore[import-untyped]
import httpx
import numpy as np
from astropy.io import fits  # type: ignore[import-untyped]
from astropy.table import Row, Table  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

# from ....util import across_api # TODO: Uncomment when integrating with server

logger = logging.getLogger("uvicorn.error")

SECONDS_IN_A_WEEK = 60 * 60 * 24 * 7
FERMI_LAT_POINTING_FILE_BASE_PATH = (
    "https://fermi.gsfc.nasa.gov/ssc/observations/timeline/ft2/files/"
)
FERMI_JD_WEEK_23 = (
    Time("2008-01-01 00:00:00").jd + 310
)  # Fermi schedules start at Fermi week 23 on the 311th day of 2008
FERMI_TIME_START_EPOCH = Time("2001-01-01")  # Start of Fermi time system
FERMI_LAT_MIN_ENERGY = 0.02  # GeV
FERMI_LAT_MAX_ENERGY = 300  # GeV
FERMI_FILETYPE_DICTIONARY: dict[int, Literal["PRELIM", "FINAL"]] = {
    3: "PRELIM",
    1: "FINAL",
    0: "FINAL",
}  # Dictionary of filetypes and number of weeks ahead to ingest
# Assume Fermi LAT footprint is a circle, so no pointing angle needed
FERMI_LAT_POINTING_ANGLE = 0


def get_current_time():
    """Wrapper around datetime.now to enable better testing"""
    return datetime.now().isoformat()


def retrieve_lat_pointing_file(
    filetype: Literal["PRELIM", "FINAL"],
    week: int,
    start_date: str,
    end_date: str,
) -> Table | None:
    """
    Retrieve either a preliminary or final LAT pointing file given a Fermi week,
    start date, end date, and version, and return its contents as an astropy Table
    Scrapes the page for links to FT2 files, selecting those that match the inputs,
    and reads the data from the most recent file
    """
    html_content = httpx.get(FERMI_LAT_POINTING_FILE_BASE_PATH).text.splitlines()

    fermi_files = []
    for line in html_content:
        filename = re.sub("<.*?>", " ", line).strip().split()
        if filename and filename[0].startswith("FERMI"):
            if (
                str(week) == filename[0].split("_")[3]
                and start_date == filename[0].split("_")[4]
                and end_date == filename[0].split("_")[5]
                and filetype in filename[0]
            ):
                fermi_files.append(FERMI_LAT_POINTING_FILE_BASE_PATH + filename[0])

    if len(fermi_files) == 0:
        return None

    # Read the most recent version
    try:
        hdu = fits.open(sorted(fermi_files, reverse=True)[0])
        data = Table(hdu[1].data)

        return data
    except HTTPError as e:
        if e.status == 404:
            # File wasn't found, so log a warning and try finding an older version
            logger.warning(
                f"{__name__}: {filetype} file for Fermi week {week} not found, skipping"
            )
            return None
        else:
            # We got an unexpected error
            logger.error(
                f"{__name__}: Reading {filetype} file for Fermi week {week} unexpectedly failed"
            )
            return None


def calculate_date_from_fermi_week(fermi_week: int) -> str:
    """
    Converts a Fermi week to a date of the form "YYYYDDD",
    where "DDD" is number of days from the beginning of that year
    E.g. "2025010" = Jan. 10, 2025.
    """
    fermi_week_in_datetime = Time(
        (fermi_week - 23) * 7 + FERMI_JD_WEEK_23, format="jd"
    ).yday
    fermi_week_year = fermi_week_in_datetime.split(":")[0]
    fermi_week_day = fermi_week_in_datetime.split(":")[1]

    return fermi_week_year + fermi_week_day


def create_schedule(
    telescope_id: str,
    fermi_week_to_ingest: int,
    data: Table,
    filetype: Literal["PRELIM", "FINAL"],
) -> dict[str, str | dict[str, str] | list]:
    return {
        "telescope_id": telescope_id,
        "name": f"fermi_lat_week_{fermi_week_to_ingest}",
        "date_range": {
            # START and STOP times are in seconds from 2001-01-01 00:00:00
            "begin": f"{(FERMI_TIME_START_EPOCH + data[0]['START'] * u.second).isot}",
            "end": f"{(FERMI_TIME_START_EPOCH + data[-1]['STOP'] * u.second).isot}",
        },
        "status": "planned",
        "fidelity": "low" if filetype == "PRELIM" else "high",
    }


def create_observation(
    instrument_id: str, fermi_week_to_ingest: int, i: int, row: Row
) -> dict[str, str | dict[str, str | float] | float]:
    return {
        "instrument_id": instrument_id,
        "object_name": f"fermi_week_{fermi_week_to_ingest}_observation_{i}",
        "pointing_position": {
            "ra": f"{row['RA_SCZ']}",
            "dec": f"{row['DEC_SCZ']}",
        },
        "date_range": {
            "begin": f"{(FERMI_TIME_START_EPOCH + row['START'] * u.second).isot}",
            "end": f"{(FERMI_TIME_START_EPOCH + row['STOP'] * u.second).isot}",
        },
        "external_observation_id": f"fermi_week_{fermi_week_to_ingest}_observation_{i}",
        "type": "imaging",
        "status": "planned",
        "pointing_angle": FERMI_LAT_POINTING_ANGLE,
        "exposure_time": float(row["STOP"] - row["START"]),
        "bandpass": {
            "filter_name": "fermi_lat",
            "min": FERMI_LAT_MIN_ENERGY,
            "max": FERMI_LAT_MAX_ENERGY,
            "type": "ENERGY",
            "unit": "GeV",
        },
    }


def ingest() -> list | None:
    """
    Method that posts Fermi Large Area Telescope (LAT) low and high fidelity observing schedules via
    pointing files found here: "https://fermi.gsfc.nasa.gov/ssc/observations/timeline/ft2/files/"

    Each file contains either a preliminary (low fideilty) or final (high fidelity) planned schedule
    for a given week, with each row containing a pointing observation given in one minute intervals.

    This method iterates over the rows of the pointing file to generate observations from each discrete pointing.
    It filters out times when the spacecraft is within the South Atlantic Anomaly (SAA) and is not taking data.
    """
    # Calculate current Fermi mission week
    now = get_current_time()
    current_fermi_week = int(np.floor((Time(now).jd - FERMI_JD_WEEK_23) / 7 + 23))

    # GET Telescope by name
    # TODO: Add Fermi observatory migration to server and uncomment the following lines
    # fermi_telescope_info = across_api.telescope.get({"name": "fermi_lat"})[0]
    # telescope_id = fermi_telescope_info["id"]
    # instrument_id = fermi_telescope_info["instruments"][0]["id"]

    # For now they're hardcoded
    telescope_id = "fermi_lat_telescope_uuid"
    instrument_id = "fermi_lat_instrument_uuid"

    # Initialize list of schedules to append
    schedules = []

    # Get the planned schedule for 3 weeks in the future, 1 week in the future, and the current week
    for weeks_ahead, filetype in FERMI_FILETYPE_DICTIONARY.items():
        fermi_week_to_ingest = current_fermi_week + weeks_ahead
        week_start_date = calculate_date_from_fermi_week(fermi_week_to_ingest)
        week_end_date = calculate_date_from_fermi_week(fermi_week_to_ingest + 1)
        # Loop through possible file versions starting with most recent
        try:
            data = retrieve_lat_pointing_file(
                filetype,
                fermi_week_to_ingest,
                week_start_date,
                week_end_date,
            )

        except Exception as e:
            logger.error(
                f"{__name__}: Reading {filetype} file for Fermi week {fermi_week_to_ingest} unexpectedly failed with error {e}",
                exc_info=True,
            )
            continue

        if data is None:
            logger.error(
                f"{__name__}: Could not read any {filetype} file for Fermi week {fermi_week_to_ingest}"
            )
            continue

        # Filter data by times when spacecraft is not in the SAA
        data = data[data["IN_SAA"] == False]  # noqa: E712

        # Write schedule metadata
        schedule = create_schedule(telescope_id, fermi_week_to_ingest, data, filetype)

        # Loop through each discrete pointing and write it as an observation
        observations = []
        for i, row in enumerate(data):
            observation = create_observation(
                instrument_id, fermi_week_to_ingest, i, row
            )
            observations.append(observation)

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
