from datetime import datetime, timedelta

import httpx
import pandas as pd
import structlog
from bs4 import BeautifulSoup
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import sdk
from .handler import POINTING_COLUMNS, SCIENCE_OBS_TAGS_PREFIXES, EuclidScheduleHandler

logger: structlog.stdlib.BoundLogger = structlog.getLogger()

SCHEDULE_PAGE_URL = "https://www.cosmos.esa.int/web/euclid/where-is-euclid-observing"
SCHEDULE_FILE_BASE_URL = "https://www.cosmos.esa.int"
DAYS_IN_FUTURE_TO_INGEST = 7


def retrieve_schedule_file_path(url: str) -> str:
    """
    Retrieve the schedule file from the schedule web page.
    Scrapes the page and extracts the href link to the schedule file.
    Returns the link to the schedule file to be read by pandas.
    """
    try:
        response = httpx.get(url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.error(
            "Long term planned schedule request failed",
            error=str(exc),
        )
        return ""

    soup = BeautifulSoup(response.text, "html.parser")
    tags = soup.find_all("a")
    for tag in tags:
        if tag.string and ".txt" in tag.string:
            schedule_file_url = str(tag["href"])
            return schedule_file_url

    logger.error("Could not find schedule file link on the schedule page.")
    return ""


def parse_pointing_file(filename: str) -> pd.DataFrame:
    """
    Parse the schedule file given a link to the file.
    Filter out rows that are not pointings, such as calibration
    observations. Return a dataframe of the pointings.
    """
    rows = []
    try:
        request = httpx.get(filename)
        request.raise_for_status()
    except Exception as e:
        logger.error(
            "Failed to retrieve schedule file",
            filename=filename,
            err=e,
            exc_info=True,
        )
        return pd.DataFrame([])

    f = request.text.splitlines()

    for i, line in enumerate(f, start=1):
        line = line.strip()
        if line and line.startswith("POINTING"):
            pointing_values = line.split()
            n = len(pointing_values)

            if n < len(POINTING_COLUMNS) - 1:
                logger.warning(
                    "Skipping line with insufficient columns",
                    line_number=i,
                    line_content=line,
                )
                continue

            row = pointing_values[: len(POINTING_COLUMNS) - 1]
            grism = (
                pointing_values[len(POINTING_COLUMNS) - 1]
                if n >= len(POINTING_COLUMNS)
                else ""
            )
            row.append(grism)

            # Check if the row is a science observation from the obs-tag field
            if any([row[1].startswith(prefix) for prefix in SCIENCE_OBS_TAGS_PREFIXES]):
                rows.append(row)

    df = pd.DataFrame(rows, columns=POINTING_COLUMNS)
    return df


def ingest() -> None:
    schedule_file_url = retrieve_schedule_file_path(SCHEDULE_PAGE_URL)
    if not len(schedule_file_url):
        logger.error("Failed to retrieve schedule file URL from schedule page.")
        return
    df = parse_pointing_file(SCHEDULE_FILE_BASE_URL + schedule_file_url)
    if not len(df):
        return
    filtered_df = df[
        (df["utc"] > datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        & (
            df["utc"]
            < (datetime.now() + timedelta(days=DAYS_IN_FUTURE_TO_INGEST)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )
    ]
    handler = EuclidScheduleHandler(
        observation_status=sdk.ObservationStatus.PLANNED,
        schedule_status=sdk.ScheduleStatus.PLANNED,
        schedule_fidelity=sdk.ScheduleFidelity.LOW,
        schedule_name="low_fidelity_planned",
    )
    handler.run(filtered_df)


@repeat_at(cron="56 2 * * 2", logger=logger)
async def entrypoint():
    try:
        logger.info("Schedule ingestion started.")
        ingest()
        logger.info("Schedule ingestion completed.")
        return
    except Exception as e:
        logger.error("Encountered an unknown error", err=e, exc_info=True)
        return
