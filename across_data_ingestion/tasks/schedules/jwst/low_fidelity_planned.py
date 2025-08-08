from io import StringIO

import astroquery.mast  # type: ignore[import-untyped]
import httpx
import pandas as pd
import structlog
from astropy.table import Table as ATable  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_WEEK
from ....util import across_api
from ..types import AcrossObservation, AcrossSchedule

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

JWST_SCIENCE_EXECUTION_PLAN_URL = (
    "https://www.stsci.edu/jwst/science-execution/observing-schedules"
)


def gen_proposal_id(row):
    """
    Pandas apply function
    Generates a proposal ID based on the VISIT_ID
    """
    visit_id_split = str(row["VISIT_ID"]).split(":")

    return visit_id_split[0]


def find_missing_params_from_mast_result(
    row, mast_observations: pd.DataFrame, instruments_info: dict
) -> pd.Series:
    """
    Pandas apply function:
    This function attempts to find the RA, DEC, INSTRUMENT, and bandpass info
    for a given row in the JWST planned execution schedule.
    It uses the MAST observations DataFrame to find the corresponding values.
    If the target name is not found, it returns None for those parameters.
    """

    try:
        mast_observation: pd.Series = (
            mast_observations.loc[
                mast_observations["target_name"] == row["TARGET_NAME"]
            ]
            .iloc[0]
            .fillna(0)
        )

        split_observation_instrument = mast_observation["instrument_name"].split("/")

        instrument_short_name = f"JWST_{split_observation_instrument[0]}"
        instrument_id = instruments_info[instrument_short_name]

        spectroscopy_keys = ["SLIT", "SLITLESS", "GRISM"]
        if len(split_observation_instrument) > 1 and any(
            key in split_observation_instrument[1] for key in spectroscopy_keys
        ):
            observation_type = "spectroscopy"
        else:
            observation_type = "imaging"

        # I have seen somtimes the bandpass information is nulled for planned information, ignore
        if all([mast_observation[col] == 0 for col in ["em_min", "em_max"]]):
            raise IndexError

        return pd.Series(
            {
                "RA": mast_observation["s_ra"],
                "DEC": mast_observation["s_dec"],
                "INSTRUMENT": mast_observation["instrument_name"],
                "INSTRUMENT_ID": instrument_id,
                "OBSERVATION_TYPE": observation_type,
                "FILTERS": mast_observation["filters"],
                "EM_MIN": mast_observation["em_min"],
                "EM_MAX": mast_observation["em_max"],
                "VALID": True,
            }
        )

    except IndexError:
        return pd.Series(
            {
                "RA": None,
                "DEC": None,
                "INSTRUMENT": None,
                "INSTRUMENT_ID": None,
                "OBSERVATION_TYPE": None,
                "FILTERS": None,
                "EM_MIN": None,
                "EM_MAX": None,
                "VALID": False,
            }
        )


def get_most_recent_jwst_planned_url() -> str:
    """Fetches the most recent JWST planned execution schedule URL from the STScI website."""
    # Send a GET request to the webpage
    response = httpx.get(JWST_SCIENCE_EXECUTION_PLAN_URL)
    response.raise_for_status()  # Raise an error for bad status codes

    # Parse the webpage content with BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract schedule information (adjust selectors based on the webpage structure)
    link_contains = "jwst/science-execution/observing-schedules/_documents"
    links = [
        link["href"]
        for link in soup.find_all("a")
        if link_contains in link["href"]  # type: ignore
    ]
    return str(links[0])


def read_mast_observations(mast_proposal_ids: list[str]) -> pd.DataFrame:
    """Fetches JWST planned observations from MAST based on proposal IDs."""
    jwst_planned_obs: ATable = astroquery.mast.Observations.query_criteria(
        obs_collection=["JWST"], proposal_id=mast_proposal_ids, calib_level=["-1"]
    )

    columns = [
        "instrument_name",
        "filters",
        "obs_id",
        "target_name",
        "s_ra",
        "s_dec",
        "em_min",
        "em_max",
    ]

    return jwst_planned_obs[columns].to_pandas()


def query_jwst_planned_execution_schedule(
    instruments_info: dict,
) -> pd.DataFrame:
    """Fetches the JWST planned execution schedule, processes it, and returns a DataFrame."""
    try:
        schedule_link = get_most_recent_jwst_planned_url()
        file_url = f"https://www.stsci.edu{schedule_link}"

        # Read the schedule file
        schedule_file_response = httpx.get(file_url)
        schedule_file_response.raise_for_status()

        lines = schedule_file_response.text.splitlines()
        # Step 1: Find header and underline line
        for i, line in enumerate(lines):
            if line.strip().startswith("VISIT ID"):
                header_line_index = i
                underline_line_index = i + 1
                data_start_index = i + 2
                break

        underline = lines[underline_line_index]

        # Step 2: Dynamically detect columns by finding dash runs
        colspecs = []
        in_dash = False
        start = 0
        for idx, char in enumerate(
            underline + " "
        ):  # Add trailing space to close last field
            if char == "-" and not in_dash:
                start = idx
                in_dash = True
            elif char != "-" and in_dash:
                colspecs.append((start, idx))
                in_dash = False

        # Optional: extract column names from header line
        header_line = lines[header_line_index]
        column_names = [
            header_line[start:end].strip().replace(" ", "_")
            for (start, end) in colspecs
        ]

        # Step 3: Read data into DataFrame
        data_text = "\n".join(lines[data_start_index:])
        df = pd.read_fwf(StringIO(data_text), colspecs=colspecs, names=column_names)

        columns_to_keep = [
            "VISIT_ID",
            "PCS_MODE",
            "SCHEDULED_START_TIME",
            "DURATION",
            "SCIENCE_INSTRUMENT_AND_MODE",
            "TARGET_NAME",
        ]
        # Filter the DataFrame to keep only relevant columns and rows
        ddf = df.loc[
            (~df["PCS_MODE"].isna())
            & (~df["PCS_MODE"].isin(["NONE", "COARSE"]))
            & (df["CATEGORY"] != "Calibration"),
            columns_to_keep,
        ]

        ddf["PROPOSAL_ID"] = ddf.apply(gen_proposal_id, axis=1)
        ddf["SCHEDULED_START_TIME"] = pd.to_datetime(ddf["SCHEDULED_START_TIME"])
        ddf["DURATION"] = pd.to_timedelta(df["DURATION"].str.replace("/", " days "))
        ddf["SCHEDULED_END_TIME"] = ddf["SCHEDULED_START_TIME"] + ddf["DURATION"]
        ddf["DURATION"] = ddf["DURATION"].dt.total_seconds()

        mast_observations = read_mast_observations(ddf["PROPOSAL_ID"].unique().tolist())

        ddf[
            [
                "RA",
                "DEC",
                "INSTRUMENT",
                "INSTRUMENT_ID",
                "OBSERVATION_TYPE",
                "FILTERS",
                "EM_MIN",
                "EM_MAX",
                "VALID",
            ]
        ] = ddf.apply(
            find_missing_params_from_mast_result,
            axis=1,
            args=(
                mast_observations,
                instruments_info,
            ),
        )
        filtered_ddf = ddf[ddf["VALID"]]
        return filtered_ddf

    except Exception:
        return pd.DataFrame({})


def jwst_to_across_schedule(
    telescope_id: str,
    data: pd.DataFrame,
    status: str,
    fidelity: str,
) -> AcrossSchedule | dict:
    """
    Creates a JWST schedule from the provided data.
    """

    begin = Time(min(data["SCHEDULED_START_TIME"])).isot
    end = Time(max(data["SCHEDULED_END_TIME"])).isot

    return {
        "telescope_id": telescope_id,
        "name": f"jwst_low_fidelity_planned_{begin.split('T')[0]}_{end.split('T')[0]}",
        "date_range": {
            "begin": begin,
            "end": end,
        },
        "status": status,
        "fidelity": fidelity,
    }


def jwst_to_across_observation(row: dict) -> AcrossObservation:
    """
    Creates a JWST observation from the provided row of data.
    Calculates the exposure time from the End - Start
    Sets the external_id a custom value based off of the P S and Pnum values
    """
    obs_start_at = Time(row["SCHEDULED_START_TIME"]).isot
    obs_end_at = Time(row["SCHEDULED_END_TIME"]).isot

    bandpass = {
        "min": row["EM_MIN"],
        "max": row["EM_MAX"],
        "unit": "nm",
        "filter_name": row["FILTERS"] if pd.notna(row["FILTERS"]) else "unknown_filter",
    }

    return {
        "instrument_id": row["INSTRUMENT_ID"],
        "object_name": f"{row["TARGET_NAME"]}",
        "pointing_position": {
            "ra": f"{round(row["RA"], 8)}",
            "dec": f"{round(row["DEC"], 8)}",
        },
        "object_position": {
            "ra": f"{round(row["RA"], 8)}",
            "dec": f"{round(row["DEC"], )}",
        },
        "date_range": {
            "begin": obs_start_at,
            "end": obs_end_at,
        },
        "external_observation_id": row["VISIT_ID"],
        "type": row["OBSERVATION_TYPE"],
        "status": "planned",
        "exposure_time": row["DURATION"],
        "bandpass": bandpass,
        "pointing_angle": 0.0,  # No Value for position angle -_-
    }


def ingest() -> None:
    """
    Fetches the JWST schedule from the specified URL and returns the parsed data.
    """
    # GET Telescope by name
    jwst_telescope_information = across_api.telescope.get({"name": "jwst"})[0]
    telescope_id = jwst_telescope_information["id"]
    instruments_info = {}
    for inst in jwst_telescope_information["instruments"]:
        instruments_info[inst["short_name"]] = inst["id"]

    # Query the JWST planned execution schedule
    latest_jwst_plan = query_jwst_planned_execution_schedule(instruments_info)

    if latest_jwst_plan.empty:
        logger.warn("Failed to read JWST observation data")
        return

    # Initialize schedule
    schedule = jwst_to_across_schedule(
        telescope_id=telescope_id,
        data=latest_jwst_plan,
        status="planned",
        fidelity="low",
    )

    # Transform dataframe to list of dictionaries
    schedule_observations = latest_jwst_plan.to_dict(orient="records")

    # Transform observations
    schedule["observations"] = [
        jwst_to_across_observation(row) for row in schedule_observations
    ]

    across_api.schedule.post(dict(schedule))


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
def entrypoint():
    try:
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Schedule ingestion encountered an unknown error.", err=e)
