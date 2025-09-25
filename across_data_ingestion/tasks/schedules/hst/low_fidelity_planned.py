from datetime import datetime, timedelta
from typing import NamedTuple, Type, cast

import astropy.units as u  # type: ignore[import-untyped]
import bs4
import httpx
import pandas as pd
import pydantic
import structlog
from astropy.coordinates import SkyCoord  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk
from ..types import Position


def get_logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger()


logger = get_logger()

HST_EXPOSURE_CATALOG_URL = "https://archive.stsci.edu/hst/catalogs/paec_7-present.cat"
BASE_TIMELINE_URL = "https://www.stsci.edu/ftp/observing/weekly_timeline/"

EXPOSURE_CATALOG_COLUMN_NAMES = [
    "object_name",
    "ra_h",
    "ra_m",
    "ra_s",
    "dec_d",
    "dec_m",
    "dec_s",
    "config",
    "mode",
    "aper",
    "spec",
    "wave",
    "time",
    "prop",
    "cy",
    "dataset",
    "release",
]


class Col(pydantic.BaseModel):
    name: str
    type: Type[float] | Type[str] | Type[int]
    spacing: tuple[int, int]


TIMELINE_FILE_COLUMNS: list[Col] = [
    Col(name="date", type=str, spacing=(0, 8)),
    Col(name="begin_time", type=str, spacing=(8, 17)),
    Col(name="end_time", type=str, spacing=(17, 27)),
    Col(name="obs_id", type=str, spacing=(27, 35)),
    Col(name="PI", type=str, spacing=(35, 47)),
    Col(name="exposure", type=str, spacing=(47, 54)),
    Col(name="target_name", type=str, spacing=(55, 84)),
    Col(name="instrument", type=str, spacing=(84, 94)),
    Col(name="mode", type=str, spacing=(94, 101)),
    Col(name="aperture", type=str, spacing=(101, 114)),
    Col(name="element", type=str, spacing=(114, 127)),
    Col(name="exp_time", type=float, spacing=(127, 137)),
    Col(name="ob", type=str, spacing=(137, 140)),
    Col(name="al", type=str, spacing=(140, 143)),
    Col(name="ex", type=str, spacing=(143, 146)),
]

# List of target names found in observations to ignore
# Mostly calibration observations
TARGET_NAMES_TO_IGNORE = [
    "DARK-NM",
    "WAVEHITM",
    "DARK",
    "BIAS",
    "TUNGSTEN",
    "NONE",
    "WAVELINE",
    "ANY",
    "WAVE",
    "DARK-EARTH-CALIB",
]


class InstrumentInfo(pydantic.BaseModel):
    id: str
    bandpass: sdk.Bandpass
    type: sdk.ObservationType


class Exposure(NamedTuple):
    object_name: str = ""
    ra_h: str = ""
    ra_m: str = ""
    ra_s: str = ""
    dec_d: str = ""
    dec_m: str = ""
    dec_s: str = ""
    config: str = ""
    mode: str = ""
    aper: str = ""
    spec: str = ""
    wave: str = ""
    time: str = ""
    prop: str = ""
    cy: str = ""
    dataset: str = ""
    release: str = ""


class TimelineRow(NamedTuple):
    date: float  # YYYY.DDD
    begin_time: str
    end_time: str
    obs_id: int
    PI: str
    exposure: str
    target_name: str
    instrument: str
    mode: str
    aperture: str
    element: str
    exp_time: float


def read_planned_exposure_catalog() -> pd.DataFrame:
    """
    Method to read the planned and archived exposure catalog as a pandas DataFrame object
    and return a subset of the dataframe corresponding to planned exposures
    """
    logger.info("Pulling HST exposure catalog...")
    start = datetime.now()

    planned_and_archived_df = pd.read_csv(
        HST_EXPOSURE_CATALOG_URL,
        names=EXPOSURE_CATALOG_COLUMN_NAMES,
        sep=r"\s+",
        on_bad_lines="skip",
    )

    end = datetime.now()
    logger.info(
        "Pulling HST exposure catalog...",
        duration=(end - start).total_seconds(),
    )

    return planned_and_archived_df[planned_and_archived_df["dataset"] == "PLANNED"]


def get_latest_timeline_file() -> str:
    """
    Method to scrape the webpage of planned timeline files,
    retrieving the latest file
    """
    response = httpx.get(BASE_TIMELINE_URL)

    html_content = response.text
    soup = bs4.BeautifulSoup(html_content, "html.parser")
    a_tags = list(soup.find_all("a"))
    href_links = [
        tag.get("href")  # type: ignore[attr-defined]
        for tag in a_tags
        if tag.get("href") and "timeline_" in tag.get("href")  # type: ignore[attr-defined]
    ]

    # Sort the links by ascending chronological order, grab latest
    href_links.sort(
        key=lambda date: datetime.strptime(date.replace("timeline_", ""), "%m_%d_%y")
    )
    newest_link = href_links[-1]

    return newest_link


def read_timeline_file(filename: str) -> pd.DataFrame:
    """
    Method to read an HST timeline file as a pandas DataFrame.
    Scapes the HTML using BeautifulSoup, separates columns based on
    fixed number of characters, and returns the data as a DataFrame object.
    """
    timeline_url = BASE_TIMELINE_URL + filename
    timeline_df = pd.read_fwf(
        timeline_url,
        colspecs=[c.spacing for c in TIMELINE_FILE_COLUMNS],
        names=[c.name for c in TIMELINE_FILE_COLUMNS],
    )

    # Drop rows that are not observations by filtering rows that do not have a date format (YYYY.DDD)
    is_date_format = timeline_df["date"].astype(str).str.match(r"\d{4}.\d{3}")
    # reset the index for all "complete" rows of data
    schedules = timeline_df[is_date_format].reset_index(drop=True)

    # Apply type conversions after reading and filtering
    for col in TIMELINE_FILE_COLUMNS:
        if col.type is str:
            schedules[col.name] = schedules[col.name].astype(str).str.strip()
        elif col.type is int:
            schedules[col.name] = pd.to_numeric(schedules[col.name]).astype("int64")
        elif col.type is float:
            schedules[col.name] = pd.to_numeric(schedules[col.name])

    return schedules


def transform_to_across_schedule(
    filename: str, telescope_id: str
) -> sdk.ScheduleCreate:
    """Format the schedule data in the ACROSS format"""
    start_datetime = datetime.strptime(filename.replace("timeline_", ""), "%m_%d_%y")
    end_datetime = start_datetime + timedelta(days=7)

    return sdk.ScheduleCreate(
        name=f"HST_planned_{filename}",
        telescope_id=telescope_id,
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=sdk.ScheduleFidelity.LOW,
        date_range=sdk.DateRange(begin=start_datetime, end=end_datetime),
        observations=[],
    )


def extract_observation_pointing_coordinates(
    planned_exposures_df: pd.DataFrame,
    observation_data: TimelineRow,
) -> Position | None:
    """
    Extract the coordinates from the planned exposures catalog and
    add them to the observation data payload.
    If coordinates cannot be found, return an empty dict.
    """
    # The planned exposure 'name' column is longer, so we need to check if it contains or equals
    # the target name from the timeline observation data
    object_name = planned_exposures_df["object_name"]
    target_name = observation_data.target_name

    planned_observation_data = planned_exposures_df[
        (object_name.str.contains(target_name[:16])) | (object_name == target_name)
    ]
    if len(planned_observation_data) == 0:
        # Could not find coordinates for this target in the planned exposure catalog, so skip
        # (this is likely a ToO or DDT observation)
        return None

    ra = (
        f"{planned_observation_data["ra_h"].values[0]}:"
        f"{planned_observation_data["ra_m"].values[0]}:"
        f"{planned_observation_data["ra_s"].values[0]}"
    )
    dec = (
        f"{planned_observation_data["dec_d"].values[0]}:"
        f"{planned_observation_data["dec_m"].values[0]}:"
        f"{planned_observation_data["dec_s"].values[0]}"
    )

    return Position(ra=ra, dec=dec)


def extract_instrument_info(
    observation_data: TimelineRow,  # pd namedtuple
    instruments: list[sdk.Instrument],
) -> InstrumentInfo | None:
    """
    Extract the ACROSS instrument model, correct bandpass,
    and corresponding observation type from the observation parameters
    """
    # Extract instrument name
    obs_instrument = observation_data.instrument

    if "ACS" in obs_instrument:
        instrument_short_name = "HST_ACS"
    elif "COS" in obs_instrument:
        instrument_short_name = "HST_COS"
    elif "STIS" in obs_instrument:
        instrument_short_name = "HST_STIS"
    elif "WFC3" in obs_instrument and "UV" in obs_instrument:
        instrument_short_name = "HST_WFC3_UVIS"
    elif "WFC3" in obs_instrument and "IR" in obs_instrument:
        instrument_short_name = "HST_WFC3_IR"
    else:
        logger.warning(
            "Could not match data to ACROSS instrument.",
            instrument=obs_instrument,
        )
        return None

    # Get the correct instrument model given the correct name
    across_instrument = next(
        (i for i in instruments if i.short_name == instrument_short_name)
    )

    # Get the correct filter from the list of filter models by
    # matching to an element or aperture from the observation data
    element = observation_data.element
    aperture = observation_data.aperture

    matching_filters: list[sdk.Filter] = []
    if across_instrument.filters:
        for across_filter in across_instrument.filters:
            filter_name = across_filter.name

            is_element_match = bool(element) and element in filter_name
            is_aperture_match = (
                bool(aperture) and aperture in filter_name and aperture != "WFC"
            )

            if is_element_match or is_aperture_match:
                # ACS WFC filters are matched by element,
                # other instruments can be matched by aperture or element
                matching_filters.append(across_filter)

    if not matching_filters:
        logger.warning(
            "Could not find filter for instrument.",
            element=observation_data.element,
            aperture=observation_data.aperture,
        )
        return None

    if len(matching_filters) > 1:
        logger.warning(
            "Multiple filters matched for an element/aperture combination. Selecting the first filter...",
            matches=matching_filters,
            element=element,
            aperture=aperture,
        )

    matching_filter = matching_filters[0]

    bandpass_parameters = sdk.WavelengthBandpass(
        filter_name=matching_filter.name,
        min=matching_filter.min_wavelength,
        max=matching_filter.max_wavelength,
        unit=sdk.WavelengthUnit.ANGSTROM,
    )

    # Get the observation type
    # Parse from filter name without HST or instrument name
    filter_descriptor = matching_filter.name.split(" ")[-1]

    # Filter element key
    # "G" = grism
    # "E" = grating
    # "P" = prism
    # "FR" = ACS grating ramp filter elements
    spectroscopy_filter_element = filter_descriptor.startswith(("G", "E", "P", "FR"))

    # All COS observations are spectroscopic
    is_spectroscopy = (
        "COS" in across_instrument.short_name or spectroscopy_filter_element
    )

    if is_spectroscopy:
        obs_type = sdk.ObservationType.SPECTROSCOPY
    else:
        # All the rest are imaging elements
        obs_type = sdk.ObservationType.IMAGING

    return InstrumentInfo(
        id=across_instrument.id,
        bandpass=sdk.Bandpass(bandpass_parameters),
        type=obs_type,
    )


def transform_to_across_observation(
    planned_exposures_df: pd.DataFrame,
    observation_data: TimelineRow,
    instruments: list[sdk.Instrument],
) -> sdk.ObservationCreate | None:
    """
    Format the observation data in the ACROSS format
    Runs methods to extract pointing coordinates and
    instrument info from the raw observation data.
    """
    pointing_coord_dict = extract_observation_pointing_coordinates(
        planned_exposures_df, observation_data
    )

    if not pointing_coord_dict:
        # Ignoring observations without matching coordinates
        return None

    instrument_info = extract_instrument_info(observation_data, instruments)
    if instrument_info is None:
        return None

    pointing_coord = SkyCoord(
        pointing_coord_dict["ra"], pointing_coord_dict["dec"], unit=(u.hourangle, u.deg)
    )

    begin_at = datetime.strptime(
        f"{observation_data.date} {observation_data.begin_time}",
        "%Y.%j %H:%M:%S",
    )
    end_at = datetime.strptime(
        f"{observation_data.date} {observation_data.end_time}",
        "%Y.%j %H:%M:%S",
    )

    return sdk.ObservationCreate(
        instrument_id=instrument_info.id,
        object_name=observation_data.target_name,
        external_observation_id=str(observation_data.obs_id),
        pointing_position=sdk.Coordinate(
            ra=pointing_coord.ra.deg,
            dec=pointing_coord.dec.deg,
        ),
        object_position=sdk.Coordinate(
            ra=pointing_coord.ra.deg,
            dec=pointing_coord.dec.deg,
        ),
        pointing_angle=0.0,  # Assuming no roll
        date_range=sdk.DateRange(begin=begin_at, end=end_at),
        exposure_time=float(observation_data.exp_time),
        status=sdk.ObservationStatus.PLANNED,
        type=instrument_info.type,
        bandpass=instrument_info.bandpass,
    )


def ingest() -> None:
    """
    Ingests low fidelity planned HST schedules.
    Reads uploaded timeline file for a week in the future and cross-matches
    planned observations with the planned and archived exposure catalog
    to retrieve pointing coordinates.

    NOTE: This does NOT include ToO or DDT observations, because these are not found
    in the planned and archived exposure catalog. Therefore this should be treated
    as a very low fidelity schedule.
    """
    # Read the planned and archived exposure catalog and the weekly timeline file
    planned_exposures_df = read_planned_exposure_catalog()
    timeline_file = get_latest_timeline_file()
    timeline_df = read_timeline_file(timeline_file)

    if timeline_df is None:
        return

    # GET telescope and instrument info from the server
    [telescope] = sdk.TelescopeApi(client).get_telescopes(name="HST")
    instruments = sdk.InstrumentApi(client).get_instruments(telescope_id=telescope.id)

    # Format schedule metadata
    across_schedule = transform_to_across_schedule(timeline_file, telescope.id)

    # leverage pandas masking with vectorization to filter
    non_calibration = ~timeline_df["target_name"].isin(TARGET_NAMES_TO_IGNORE)
    non_acq_mode = ~timeline_df["mode"].str.contains("ACQ", na=False)
    filtered_observation_data = list(
        timeline_df[non_calibration & non_acq_mode].itertuples()
    )

    if len(filtered_observation_data) == 0:
        return None

    for observation_data in filtered_observation_data:
        # Format observation data in ACROSS format
        across_observation = transform_to_across_observation(
            planned_exposures_df, cast(TimelineRow, observation_data), instruments
        )
        if across_observation:
            across_schedule.observations.append(across_observation)

    try:
        sdk.ScheduleApi(client).create_schedule(across_schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.info("Schedule already exists.", schedule_name=across_schedule.name)
        else:
            raise err


@repeat_at(cron="59 22 * * *", logger=logger)
def entrypoint():
    try:
        ingest()
        logger.info("HST schedule ingestion ran successfully")
    except Exception as e:
        logger.error(
            "HST schedule ingestion encountered an unexpected error",
            err=e,
            exc_info=True,
        )
