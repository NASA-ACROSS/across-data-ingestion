from io import StringIO

import astroquery.mast  # type: ignore[import-untyped]
import httpx
import pandas as pd
import structlog
from astropy.table import Table as ATable  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

JWST_SCIENCE_EXECUTION_PLAN_URL = (
    "https://www.stsci.edu/jwst/science-execution/observing-schedules"
)


def gen_proposal_id(row: pd.Series) -> str:
    """
    Pandas apply function
    Generates a proposal ID based on the VISIT_ID
    """
    visit_id_split = str(row["VISIT_ID"]).split(":")

    return visit_id_split[0]


def find_missing_params_from_mast_result(
    row: pd.Series, mast_observations: pd.DataFrame, instruments_info: dict
) -> pd.Series:
    """
    Pandas apply function:
    This function attempts to find the RA, DEC, INSTRUMENT, and bandpass info
    for a given row in the JWST planned execution schedule.
    It uses the MAST observations DataFrame to find the corresponding values.
    If the target name is not found, it returns None for those parameters.
    """

    empty_return = pd.Series(
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

    # sometimes even the target name observation is not in the mast results
    in_mast_observations = (
        row["TARGET_NAME"] in mast_observations["target_name"].to_list()
    )
    if not in_mast_observations:
        return empty_return

    mast_observation: pd.Series = (
        mast_observations.loc[mast_observations["target_name"] == row["TARGET_NAME"]]
        .iloc[0]
        .fillna(0)
    )

    # I have seen somtimes the bandpass information is nulled for planned information, ignore
    if all([mast_observation[col] == 0 for col in ["em_min", "em_max"]]):
        return empty_return

    # Get the instrument ID from the record instrument name
    split_observation_instrument = mast_observation["instrument_name"].split("/")
    instrument_short_name = f"JWST_{split_observation_instrument[0]}"

    # off chance the instrument name isn't in the instrument dictionary
    if instrument_short_name not in instruments_info.keys():
        return empty_return

    instrument_id = instruments_info[instrument_short_name]

    # Find out what the observation type by hints from the instrument name
    spectroscopy_keys = ["SLIT", "SLITLESS", "GRISM"]
    if len(split_observation_instrument) > 1 and any(
        key in split_observation_instrument[1] for key in spectroscopy_keys
    ):
        observation_type = "spectroscopy"
    else:
        observation_type = "imaging"

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
        link["href"]  # type: ignore
        for link in soup.find_all("a")
        if link_contains in link["href"]  # type: ignore
    ]
    return str(links[0])


def read_mast_observations(mast_proposal_ids: list[str]) -> pd.DataFrame:
    """Fetches JWST planned observations from MAST based on proposal IDs."""
    jwst_planned_obs: ATable = astroquery.mast.Observations.query_criteria(
        obs_collection=["JWST"],
        proposal_id=mast_proposal_ids,  # , calib_level=["-1"]
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


def parse_jwst_data_to_fwf(response_text: str) -> pd.DataFrame:
    """
    Parses the result from the JWST file into a pandas dataframe, by dynamically discovering the fixed width file column specifications
    """
    data_lines = response_text.splitlines()

    # Step 1: Find header and underline line
    for i, line in enumerate(data_lines):
        if line.strip().startswith("VISIT ID"):
            header_line_index = i
            underline_line_index = i + 1
            data_start_index = i + 2
            break

    underline = data_lines[underline_line_index]

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
    header_line = data_lines[header_line_index]
    column_names = [
        header_line[start:end].strip().replace(" ", "_") for (start, end) in colspecs
    ]

    # Step 3: Read data into DataFrame
    data_text = "\n".join(data_lines[data_start_index:])
    return pd.read_fwf(StringIO(data_text), colspecs=colspecs, names=column_names)


def filter_jwst_dataframe(df: pd.DataFrame, instruments_info: dict) -> pd.DataFrame:
    """
    Filters the JWST Dataframe based on specific columns, and their parameters.
        PCS Mode cannot be NONE or COARSE
        and it cannot be in a CATEGORY of Calibration
    After massaging the time related fields, it crossmatches the targets with MAST
    And returns the valid fields.
    """

    columns_to_keep = [
        "VISIT_ID",
        "PCS_MODE",
        "SCHEDULED_START_TIME",
        "DURATION",
        "SCIENCE_INSTRUMENT_AND_MODE",
        "TARGET_NAME",
    ]
    # Filter the DataFrame to keep only relevant columns and rows
    # Don't ingest PCS_MODE of NONE or COARSE, and Calibration images
    # These don't correspond to records in MAST
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


def query_jwst_planned_execution_schedule(
    instruments_info: dict,
) -> pd.DataFrame:
    """Fetches the JWST planned execution schedule, processes it, and returns a DataFrame."""

    schedule_link = get_most_recent_jwst_planned_url()
    file_url = f"https://www.stsci.edu{schedule_link}"

    # Read the schedule file
    try:
        schedule_file_response = httpx.get(file_url)
        schedule_file_response.raise_for_status()
    except Exception:
        return pd.DataFrame({})

    # parse the data as a fwf into a pandas dataframe
    df = parse_jwst_data_to_fwf(schedule_file_response.text)

    # filter the dataframe and populate missing values
    completed_df = filter_jwst_dataframe(df, instruments_info)

    return completed_df


def jwst_to_across_schedule(
    telescope_id: str,
    data: pd.DataFrame,
    status: sdk.ScheduleStatus,
    fidelity: sdk.ScheduleFidelity,
) -> sdk.ScheduleCreate:
    """
    Creates a JWST schedule from the provided data.
    """

    begin_atime = Time(min(data["SCHEDULED_START_TIME"]))
    end_atime = Time(max(data["SCHEDULED_END_TIME"]))

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"jwst_low_fidelity_planned_{begin_atime.isot.split('T')[0]}_{end_atime.isot.split('T')[0]}",
        date_range=sdk.DateRange(
            begin=begin_atime.to_datetime(), end=end_atime.to_datetime()
        ),
        status=status,
        fidelity=fidelity,
        observations=[],
    )


def jwst_to_across_observation(row: dict) -> sdk.ObservationCreate:
    """
    Creates a JWST observation from the provided row of data.
    Calculates the exposure time from the End - Start
    Sets the external_id a custom value based off of the P S and Pnum values
    """
    obs_start_at = Time(row["SCHEDULED_START_TIME"]).to_datetime()
    obs_end_at = Time(row["SCHEDULED_END_TIME"]).to_datetime()

    bandpass = sdk.WavelengthBandpass.model_validate(
        {
            "min": row["EM_MIN"],
            "max": row["EM_MAX"],
            "unit": "nm",
            "filter_name": row["FILTERS"]
            if pd.notna(row["FILTERS"])
            else "unknown_filter",
        }
    )

    return sdk.ObservationCreate(
        instrument_id=row["INSTRUMENT_ID"],
        object_name=row["TARGET_NAME"],
        pointing_position=sdk.Coordinate(
            ra=round(row["RA"], 8), dec=round(row["DEC"], 8)
        ),
        object_position=sdk.Coordinate(
            ra=round(row["RA"], 8), dec=round(row["DEC"], 8)
        ),
        date_range=sdk.DateRange(begin=obs_start_at, end=obs_end_at),
        external_observation_id=row["VISIT_ID"],
        type=row["OBSERVATION_TYPE"],
        status=sdk.ObservationStatus.PLANNED,
        exposure_time=row["DURATION"],
        bandpass=sdk.Bandpass(bandpass),
        pointing_angle=0.0,
    )


def ingest() -> None:
    """
    Fetches the JWST schedule from the specified URL and returns the parsed data.
    """
    # GET Telescope by name
    (jwst_telescope_information,) = sdk.TelescopeApi(client).get_telescopes(name="jwst")
    telescope_id = jwst_telescope_information.id
    instruments_info = {}
    jwst_instruments = (
        jwst_telescope_information.instruments
        if jwst_telescope_information.instruments
        else []
    )
    for inst in jwst_instruments:
        instruments_info[inst.short_name] = inst.id

    # Query the JWST planned execution schedule
    latest_jwst_plan = query_jwst_planned_execution_schedule(instruments_info)

    if latest_jwst_plan.empty:
        logger.warn("Failed to read JWST observation data")
        return

    # Initialize schedule
    jwst_schedule = jwst_to_across_schedule(
        telescope_id=telescope_id,
        data=latest_jwst_plan,
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=sdk.ScheduleFidelity.LOW,
    )

    # Transform dataframe to list of dictionaries
    schedule_observations = latest_jwst_plan.to_dict(orient="records")

    # Transform observations
    jwst_schedule.observations = [
        jwst_to_across_observation(row) for row in schedule_observations
    ]

    try:
        sdk.ScheduleApi(client).create_schedule(jwst_schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.warning("A schedule already exists", extra=err.__dict__)
        else:
            raise err


@repeat_at(cron="33 22 * * *", logger=logger)  # Weekly
async def entrypoint():
    try:
        logger.info("Schedule ingestion started.")
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Schedule ingestion encountered an unknown error.", err=e)
