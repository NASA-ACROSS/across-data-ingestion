from urllib.parse import urlencode

import numpy as np
import pandas as pd
import structlog
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import sdk
from .handler import XRISMScheduleHandler

logger: structlog.stdlib.BoundLogger = structlog.getLogger()


XRISM_SHORT_TERM_SCHEDULE_URL = (
    "https://xrism.isas.jaxa.jp/research/observers/shortterm/prefinal/index.html"
)
XRISM_TOO_TABLE_URL = (
    "https://xrism.isas.jaxa.jp/research/observers/generic_too/index.html"
)


def parse_short_term_schedule_page() -> pd.DataFrame:
    """
    Parse the short term schedule page to extract the most recent
    planned schedule table. Planned schedules are broken down by week,
    so this function identifies the most recent schedule and parses
    it into a pandas DataFrame.
    """
    short_term_schedules = pd.read_html(XRISM_SHORT_TERM_SCHEDULE_URL, flavor="bs4")
    if len(short_term_schedules):
        # The most recent schedule is the first table on the page
        short_term_schedule = short_term_schedules[0]
        short_term_schedule = short_term_schedule.rename(
            columns={"Seq": "observation_id", "Mnv start time (UT)": "start_time"}
        )
        return short_term_schedule
    logger.warning("Could not find any short term schedule tables to parse")
    return pd.DataFrame([])


def query_proposal_table(observation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Query the XRISM database to extract exposure times from the proposal table
    for the observation IDs in the short term schedule dataframe. This is necessary
    because the short term schedule table does not contain exposure time info.
    Adds the exposure time information to the short term schedule dataframe and returns it.
    Skips over calibration observations (which have proposal type "CAL") by assigning them NaN exposure times.
    """
    observation_ids = observation_df["observation_id"].to_list()
    query = f"SELECT * FROM xrism_proposal WHERE observation_id IN ({', '.join(f"'{obs_id}'" for obs_id in observation_ids)});"
    xrism_query_url = (
        "https://darts.isas.jaxa.jp/app/query/astroquery/sql.php?"
        + urlencode(query={"sql": query})
    )
    proposal_dfs = pd.read_html(xrism_query_url, flavor="bs4")
    if len(proposal_dfs):
        proposal_df = proposal_dfs[0]
        for _, row in observation_df.iterrows():
            obs_id = row["observation_id"]
            if obs_id in proposal_df["observation_id"].values:
                proposal_row = proposal_df.loc[proposal_df["observation_id"] == obs_id]
                if proposal_row["proposal_type"].values[0] != "CAL":
                    exposure_time = proposal_row["awarded_exposure"].values[
                        0
                    ]  # in ksec
                    observation_df.loc[
                        observation_df["observation_id"] == obs_id, "exposure_time"
                    ] = exposure_time
                else:
                    observation_df.loc[
                        observation_df["observation_id"] == obs_id, "exposure_time"
                    ] = np.nan

    else:
        logger.warning(
            "Could not find exposure time information for short term schedule observations"
        )

    return observation_df


def parse_too_table(observation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Query the ToO page to retrieve exposure times for ToO observations.
    ToO observations will not be in the XRISM proposal table, so this
    function scrapes the ToO information and crossmatches on sequence ID (aka observation ID)
    to add exposure time information to the short term schedule dataframe.
    Returns a dataframe of the short term schedule with exposure times.
    """
    too_dfs = pd.read_html(XRISM_TOO_TABLE_URL, flavor="bs4")
    if len(too_dfs):
        too_df = too_dfs[0]
    else:
        logger.warning("Could not read ToO table")
        return observation_df

    for _, row in observation_df.iterrows():
        obs_id = row["observation_id"]
        if obs_id in too_df["Seq"].values:
            exposure_time = too_df.loc[too_df["Seq"] == obs_id, "Exp. (ks)"].values[
                0
            ]  # In ksec
            observation_df.loc[
                observation_df["observation_id"] == obs_id, "exposure_time"
            ] = exposure_time

    return observation_df


def ingest() -> None:
    short_term_schedule_df = parse_short_term_schedule_page()
    short_term_schedule_df = query_proposal_table(short_term_schedule_df)
    short_term_schedule_df = parse_too_table(short_term_schedule_df)

    handler = XRISMScheduleHandler(
        observation_status=sdk.ObservationStatus.PLANNED,
        schedule_status=sdk.ScheduleStatus.PLANNED,
        schedule_fidelity=sdk.ScheduleFidelity.LOW,
        schedule_name="low_fidelity_planned",
    )
    handler.run(short_term_schedule_df)


@repeat_at(cron="5 4 * * 2,5", logger=logger)
async def entrypoint():
    try:
        logger.info("Schedule ingestion started.")
        ingest()
        logger.info("Schedule ingestion completed.")
        return
    except Exception as e:
        logger.error("Encountered an unknown error", err=e, exc_info=True)
        return
