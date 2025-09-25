import re
from datetime import datetime
from typing import Literal
from urllib.error import HTTPError

import astropy.units as u  # type: ignore[import-untyped]
import httpx
import numpy as np
import pandas as pd
import pydantic
import structlog
from astropy.io import fits  # type: ignore[import-untyped]
from astropy.table import Table  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

FERMI_LAT_POINTING_FILE_BASE_PATH = (
    "https://fermi.gsfc.nasa.gov/ssc/observations/timeline/ft2/files/"
)

# Fermi schedules start at Fermi week 23 on the 311th day of 2008
FERMI_WEEK_START = 23
FERMI_SCHEDULE_DATE_START = Time("2008-01-01 00:00:00").jd + 310

FERMI_TIME_START_EPOCH = Time("2001-01-01")  # Start of Fermi time system
FERMI_LAT_MIN_ENERGY = 0.02  # GeV
FERMI_LAT_MAX_ENERGY = 300  # GeV

# Dictionary of filetypes and number of weeks ahead to ingest
FIDELITY_BY_WEEKS_AHEAD: dict[int, Literal["PRELIM", "FINAL"]] = {
    3: "PRELIM",
    1: "FINAL",
    0: "FINAL",
}

# Assume Fermi LAT footprint is a circle, so no pointing angle needed
FERMI_LAT_POINTING_ANGLE = 0

# Data columns that are used for transformation.
FERMI_DATA_COLS = ["RA_SCZ", "DEC_SCZ", "START", "STOP"]


class PointingFile(pydantic.BaseModel):
    name: str = ""
    fidelity: str = "PRELIM"
    week: int = 0
    start: str = ""
    end: str = ""
    rev: int = 0
    last_modified: datetime = datetime.now()


class PointingData(pydantic.BaseModel):
    df: pd.DataFrame = pd.DataFrame()
    file: PointingFile = PointingFile()

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
        (fermi_week - FERMI_WEEK_START) * 7 + FERMI_SCHEDULE_DATE_START, format="jd"
    ).yday
    fermi_week_year = fermi_week_in_datetime.split(":")[0]
    fermi_week_day = fermi_week_in_datetime.split(":")[1]

    return fermi_week_year + fermi_week_day


def parse_pointing_files(html_lines: list[str]) -> list[PointingFile]:
    """Parse all files into ScheduleFiles which can be sorted for the next batch later"""
    files: list[PointingFile] = []

    for line in html_lines:
        # split on any HTML tags
        split_line = re.sub("<.*?>", " ", line).strip().split()

        if split_line and split_line[0].startswith("FERMI"):
            # Example filename: FERMI_POINTING_FINAL_023_2008311_2008318_00.fits
            filename = split_line[0]

            # Parse filename into parts
            fermi_fidelity_index = 2
            # ignore beginning of the filename, it is the same for all.
            parts = filename.split("_")[fermi_fidelity_index:]
            fidelity, fermi_week, start, end, rev_ext = parts

            date = split_line[1]  # dd-MMM-YYYY
            time = split_line[2]  # hh:mm
            last_modified_str = f"{date} {time}"

            rev = int(rev_ext.split(".")[0])  # 00.fits

            files.append(
                PointingFile(
                    name=filename,
                    fidelity=fidelity,
                    week=int(fermi_week),
                    start=start,
                    end=end,
                    rev=rev,
                    last_modified=datetime.strptime(
                        last_modified_str, "%d-%b-%Y %H:%M"
                    ),
                )
            )

    return files


def download_pointings_data(
    week_files_groups: list[list[PointingFile]],
) -> list[PointingData]:
    """
    Download pointing data for the most recent available version of the week for each week.
    """
    pointings: list[PointingData] = []

    for files in week_files_groups:
        pointing_data: PointingData | None = None

        for file in files:
            try:
                url = FERMI_LAT_POINTING_FILE_BASE_PATH + file.name
                hdu = fits.open(url)
                tbl = Table(hdu[1].data)

                # filter out SAA using bitwise NOT `~` operator. Using "is False" doesn't
                # work because it is np.False, and using "== False" raises a ruff warning to use "is False"
                tbl = tbl[~tbl["IN_SAA"]]

                # Pull only needed columns, some columns are multi-dimensional
                # but currently, we only use 1D columns.
                # DataFrame is used downstream for leveraging vectorized processing
                # for optimization.
                df = tbl[FERMI_DATA_COLS].to_pandas()

                pointing_data = PointingData(df=df, file=file)

                # move on to the next week
                break
            except HTTPError as err:
                # File wasn't found or error; log and try finding an older version
                if err.status == 404:
                    logger.warning("File not found, skipping.", url=err.url)
                else:
                    logger.exception(
                        "Failed to read the file due to an HTTP error.",
                        url=err.url,
                    )

                # try the next file
                continue

        if pointing_data:
            pointings.append(pointing_data)
        else:
            logger.warning(
                "No pointing data found for a given week.",
                week=files[0].week,
                fidelity=files[0].fidelity,
            )

    return pointings


def get_pointing_files_html_lines() -> list[str]:
    res = httpx.get(FERMI_LAT_POINTING_FILE_BASE_PATH)

    if res.status_code > 300:
        logger.error("Failed to GET Fermi LAT pointing HTML files.", res)
        return []

    return res.text.splitlines()


def find_files_for_weeks_ahead(
    all_files: list[PointingFile], current_week: int
) -> list[list[PointingFile]]:
    """find files for each fidelity for the weeks ahead of the provided week"""
    files: list[list[PointingFile]] = []

    for weeks_ahead, fidelity in FIDELITY_BY_WEEKS_AHEAD.items():
        fidelity_files = [f for f in all_files if f.fidelity == fidelity]

        week = current_week + weeks_ahead

        week_files = [f for f in fidelity_files if f.week == week]

        # latest modified should be first
        week_files.sort(key=lambda f: f.last_modified, reverse=True)

        if not week_files:
            logger.warning(
                "No files found for the week",
                extra={
                    "fidelity": fidelity,
                    "week": week,
                    "date": calculate_date_from_fermi_week(week),
                },
            )

            continue

        files.append(week_files)

    return files


def transform_to_schedule(
    telescope_id: str,
    fermi_week_to_ingest: int,
    pointing_df: pd.DataFrame,
    fidelity: str,
) -> sdk.ScheduleCreate:
    # START and STOP times are in seconds from 2001-01-01 00:00:00
    begin = (FERMI_TIME_START_EPOCH + pointing_df["START"].iloc[0] * u.second).isot
    end = (FERMI_TIME_START_EPOCH + pointing_df["STOP"].iloc[-1] * u.second).isot

    across_fidelity: sdk.ScheduleFidelity = (
        sdk.ScheduleFidelity.LOW if fidelity == "PRELIM" else sdk.ScheduleFidelity.HIGH
    )

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"fermi_lat_week_{fermi_week_to_ingest}",
        date_range=sdk.DateRange(
            begin=begin,
            end=end,
        ),
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=across_fidelity,
        observations=[],
    )


def transform_to_observations(
    instrument_id: str, fermi_week_to_ingest: int, df: pd.DataFrame
):
    # Vectorize START/STOP → ISO times and exposure
    # Using vectorized processing as opposed to traditional for loops
    # helps speed up real file processing ~5x from ~6.5s to ~1s.
    starts_iso = (FERMI_TIME_START_EPOCH + df["START"].to_numpy() * u.second).isot
    stops_iso = (FERMI_TIME_START_EPOCH + df["STOP"].to_numpy() * u.second).isot
    exposures = (df["STOP"] - df["START"]).to_numpy().astype(float)

    # Precompute observation names
    obs_names = [
        f"fermi_week_{fermi_week_to_ingest}_observation_{i}" for i in range(len(df))
    ]

    # Add computed fields to DataFrame for easy iteration, avoid modifying og DF.
    df = df.copy()
    df["BEGIN"] = starts_iso
    df["END"] = stops_iso
    df["EXPOSURE"] = exposures
    df["OBS_NAME"] = obs_names

    observations = [
        sdk.ObservationCreate(
            instrument_id=instrument_id,
            object_name=row.OBS_NAME,  # type:ignore
            pointing_position=sdk.Coordinate(ra=row.RA_SCZ, dec=row.DEC_SCZ),  # type:ignore
            date_range=sdk.DateRange(begin=row.BEGIN, end=row.END),  # type:ignore
            external_observation_id=row.OBS_NAME,  # type:ignore
            exposure_time=row.EXPOSURE,  # type:ignore
            type=sdk.ObservationType.IMAGING,
            status=sdk.ObservationStatus.PLANNED,
            pointing_angle=FERMI_LAT_POINTING_ANGLE,
            bandpass=sdk.Bandpass(
                sdk.EnergyBandpass(
                    filter_name="fermi_lat",
                    min=FERMI_LAT_MIN_ENERGY,
                    max=FERMI_LAT_MAX_ENERGY,
                    unit=sdk.EnergyUnit.GEV,
                )
            ),
        )
        for row in df.itertuples(index=True)
    ]

    return observations


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
    current_fermi_week = int(
        np.floor((Time(now).jd - FERMI_SCHEDULE_DATE_START) / 7 + FERMI_WEEK_START)
    )

    html_lines = get_pointing_files_html_lines()
    pointing_files = parse_pointing_files(html_lines)
    week_files_groups = find_files_for_weeks_ahead(pointing_files, current_fermi_week)

    # open the files and create the table data
    pointings = download_pointings_data(week_files_groups)

    if len(pointings) == 0:
        logger.warning(
            "No pointing data to transform.", current_fermi_week=current_fermi_week
        )
        return

    telescope = sdk.TelescopeApi(client).get_telescopes(name="lat")[0]
    telescope_id = telescope.id

    if telescope.instruments:
        for instrument in telescope.instruments:
            if instrument.name == "Large Area Telescope":
                lat_instrument_id = instrument.id

    schedules: list[sdk.ScheduleCreate] = []

    for pointing in pointings:
        schedule = transform_to_schedule(
            telescope_id,
            pointing.file.week,
            pointing.df,
            fidelity=pointing.file.fidelity,
        )

        schedule.observations = transform_to_observations(
            instrument_id=lat_instrument_id,
            fermi_week_to_ingest=pointing.file.week,
            df=pointing.df,
        )

        schedules.append(schedule)

    sdk.ScheduleApi(client).create_many_schedules(
        sdk.ScheduleCreateMany(
            schedules=schedules,
            telescope_id=telescope_id,
        )
    )


@repeat_at(cron="22 2 * * *", logger=logger)
def entrypoint() -> None:
    try:
        ingest()
        logger.info("Task completed successfully")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Encountered an unknown error", err=e, exc_info=True)
