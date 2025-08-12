import re
from datetime import datetime
from typing import Literal
from urllib.error import HTTPError

import astropy.units as u  # type: ignore[import-untyped]
import httpx
import numpy as np
import pydantic
import structlog
from astropy.io import fits  # type: ignore[import-untyped]
from astropy.table import Row, Table  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_WEEK
from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

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


class ScheduleFile(pydantic.BaseModel):
    name: str = ""
    fidelity: Literal["PRELIM", "FINAL"] = "PRELIM"
    week: int = 0
    start: str = ""
    end: str = ""


class FileData(pydantic.BaseModel):
    table: Table = Table()
    file: ScheduleFile = ScheduleFile()

    # needed for astropy.Table
    model_config = {"arbitrary_types_allowed": True}


def get_current_time():
    """Wrapper around datetime.now to enable better testing"""
    return datetime.now().isoformat()


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


def transform_to_schedule(
    telescope_id: str,
    fermi_week_to_ingest: int,
    filedata: FileData,
) -> sdk.ScheduleCreate:
    # START and STOP times are in seconds from 2001-01-01 00:00:00
    begin = (FERMI_TIME_START_EPOCH + filedata.table[0]["START"] * u.second).isot
    end = (FERMI_TIME_START_EPOCH + filedata.table[-1]["STOP"] * u.second).isot

    fidelity = (
        sdk.ScheduleFidelity.LOW
        if filedata.file.fidelity == "PRELIM"
        else sdk.ScheduleFidelity.HIGH
    )

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"fermi_lat_week_{fermi_week_to_ingest}",
        date_range=sdk.DateRange(
            begin=begin,
            end=end,
        ),
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=fidelity,
        observations=[],
    )


def transform_to_observation(
    instrument_id: str, fermi_week_to_ingest: int, obs_idx: int, row: Row
) -> sdk.ObservationCreate:
    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=f"fermi_week_{fermi_week_to_ingest}_observation_{obs_idx}",
        pointing_position=sdk.Coordinate(
            ra=row["RA_SCZ"],
            dec=row["DEC_SCZ"],
        ),
        date_range=sdk.DateRange(
            begin=(FERMI_TIME_START_EPOCH + row["START"] * u.second).isot,
            end=(FERMI_TIME_START_EPOCH + row["STOP"] * u.second).isot,
        ),
        external_observation_id=f"fermi_week_{fermi_week_to_ingest}_observation_{obs_idx}",
        type=sdk.ObservationType.IMAGING,
        status=sdk.ObservationStatus.PLANNED,
        pointing_angle=FERMI_LAT_POINTING_ANGLE,
        exposure_time=float(row["STOP"] - row["START"]),
        bandpass=sdk.Bandpass(
            sdk.EnergyBandpass(
                filter_name="fermi_lat",
                min=FERMI_LAT_MIN_ENERGY,
                max=FERMI_LAT_MAX_ENERGY,
                unit=sdk.EnergyUnit.GEV,
            )
        ),
    )


def get_pointing_filenames(
    html_lines: list[str], current_fermi_week: int
) -> list[ScheduleFile]:
    files: list[ScheduleFile] = []

    # create a dict to match filenames
    filename_dict: dict[str, str] = {}

    for line in html_lines:
        # split on any HTML tags
        split_line = re.sub("<.*?>", " ", line).strip().split()

        if split_line and split_line[0].startswith("FERMI"):
            # Example filename: FERMI_POINTING_FINAL_023_2008311_2008318_00.fits
            filename = split_line[0]

            # index of FINAL or PRELIM
            fermi_fidelity_index = 2
            parts = filename.split("_")
            key_tokens = parts[fermi_fidelity_index : len(parts) - 1]
            fidelity, fermi_week, start, end = key_tokens
            key = ".".join([fidelity, fermi_week, start, end])

            filename_dict[key] = filename

    # Get the planned schedule for 3 weeks in the future, 1 week in the future, and the current week
    for weeks_ahead, fermi_fidelity in FERMI_FILETYPE_DICTIONARY.items():
        week = current_fermi_week + weeks_ahead
        start_date = calculate_date_from_fermi_week(week)
        end_date = calculate_date_from_fermi_week(week + 1)

        key = ".".join([fermi_fidelity, str(week), start_date, end_date])

        filename = filename_dict.get(key, "")

        if filename:
            files.append(
                ScheduleFile(
                    name=filename,
                    fidelity=fermi_fidelity,
                    week=week,
                    start=start_date,
                    end=end_date,
                )
            )
        else:
            logger.warning(
                "No matching filename for the week.",
                week=week,
                start=start_date,
                end=end_date,
            )

    return files


def get_schedule_file_data(files: list[ScheduleFile]) -> FileData:
    # Try the most recent version first, then try the older ones
    sorted_files = sorted(files, reverse=True, key=lambda file: file.week)

    for file in sorted_files:
        url = FERMI_LAT_POINTING_FILE_BASE_PATH + file.name

        try:
            hdu = fits.open(url)
            data = Table(hdu[1].data)

            return FileData(table=data, file=file)
        except HTTPError as err:
            if err.status == 404:
                # File wasn't found, so log a warning and try finding an older version
                logger.warning("File not found, skipping.", url=url)
            else:
                logger.error(
                    "Failed to read the file due to an HTTP error.",
                    url=url,
                    err=err,
                    exc_info=True,
                )

            continue

    # returns an empty table and empty file
    return FileData()


def get_pointing_files_html_lines() -> list[str]:
    res = httpx.get(FERMI_LAT_POINTING_FILE_BASE_PATH)

    if res.status_code > 300:
        logger.error("Failed to GET Fermi LAT pointing HTML files.", res)
        return []

    return res.text.splitlines()


def get_planned_schedule_data(current_fermi_week: int):
    # Attempt to get the html first
    html = get_pointing_files_html_lines()

    # extract the filenames from html for the current week
    files = get_pointing_filenames(html, current_fermi_week)

    # open the files and create the table data
    filedata = get_schedule_file_data(files)

    if len(filedata.table):
        # filter out SAA using bitwise NOT `~` operator. Using "is False" doesn't
        # work because it is np.False, and using "== False" raises a ruff warning to use "is False"
        filedata.table = filedata.table[~filedata.table["IN_SAA"]]

    return filedata


def ingest() -> None:
    """
    Method that posts Fermi Large Area Telescope (LAT) low and high fidelity observing schedules via
    pointing files found here: "https://fermi.gsfc.nasa.gov/ssc/observations/timeline/ft2/files/"

    Each file contains either a preliminary (low fidelity) or final (high fidelity) planned schedule
    for a given week, with each row containing a pointing observation given in one minute intervals.

    This method iterates over the rows of the pointing file to generate observations from each discrete pointing.
    It filters out times when the spacecraft is within the South Atlantic Anomaly (SAA) and is not taking data.
    """
    # Calculate current Fermi mission week
    now = get_current_time()
    current_fermi_week = int(np.floor((Time(now).jd - FERMI_JD_WEEK_23) / 7 + 23))

    schedule_filedata = get_planned_schedule_data(current_fermi_week)

    if len(schedule_filedata.table) == 0:
        logger.warning("No schedule data to transform.")
        return

    # GET Telescope by name
    [telescope] = sdk.TelescopeApi(client).get_telescopes(name="lat")
    telescope_id = telescope.id

    if telescope.instruments:
        for instrument in telescope.instruments:
            if instrument.name == "Large Area Telescope":
                lat_instrument_id = instrument.id

    schedule = transform_to_schedule(
        telescope_id, current_fermi_week, schedule_filedata
    )

    for i, row in enumerate(schedule_filedata.table):
        observation = transform_to_observation(
            lat_instrument_id, schedule_filedata.file.week, i, row
        )
        schedule.observations.append(observation)

    try:
        sdk.ScheduleApi(client).create_schedule(schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.info("Schedule already exists.", schedule_name=schedule.name)


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
def entrypoint() -> None:
    try:
        ingest()
        logger.info("Task completed successfully")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Encountered an unknown error", err=e, exc_info=True)
