from datetime import datetime, timedelta

import astropy.units as u  # type: ignore[import-untyped]
import httpx
import pandas as pd
import structlog
from astropy.coordinates import SkyCoord  # type: ignore[import-untyped]
from bs4 import BeautifulSoup
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_WEEK
from ....util import across_api
from ..types import AcrossObservation, AcrossSchedule

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


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


def read_planned_exposure_catalog() -> pd.DataFrame:
    """
    Method to read the planned and archived exposure catalog as a pandas DataFrame object
    and return a subset of the dataframe corresponding to planned exposures
    """
    planned_and_archived_df = pd.read_csv(
        "https://archive.stsci.edu/hst/catalogs/paec_7-present.cat",
        names=EXPOSURE_CATALOG_COLUMN_NAMES,
        sep="\s+",
        skiprows=22,
        on_bad_lines="skip",
    )
    planned_df = planned_and_archived_df[
        planned_and_archived_df["dataset"] == "PLANNED"
    ]
    return planned_df


def get_latest_timeline_file() -> str:
    """
    Method to scrape the webpage of planned timeline files,
    retrieving the latest file
    """
    response = httpx.get(BASE_TIMELINE_URL)

    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
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


def read_timeline_file(filename: str) -> pd.DataFrame | None:
    """
    Method to read an HST timeline file as a pandas DataFrame.
    Scapes the HTML using BeautifulSoup, separates columns based on
    fixed number of characters, and returns the data as a DataFrame object.
    """
    timeline_url = BASE_TIMELINE_URL + filename
    timeline_response = httpx.get(timeline_url)
    timeline_file = timeline_response.text

    soup = BeautifulSoup(timeline_file, "html.parser")

    if not len(soup.text):
        logger.warning("Could not scrape timeline file")
        return None

    raw_observations = []
    lines = soup.text.split("\n")
    for line in lines:
        if line.startswith("20"):  # Observation lines start with the year
            date = line[:8]
            begin = line[9:17].strip()
            end = line[18:27].strip()
            obs_id = line[28:35].strip()
            pi = line[36:47].strip()
            exposure = line[48:54].strip()
            target = line[55:84].strip()
            instrument = line[85:94].strip()
            mode = line[95:101].strip()
            aperture = line[102:114].strip()
            element = line[115:127].strip()
            exp_time = line[128:137].strip()
            raw_observations.append(
                {
                    "date": date,
                    "begin_time": begin,
                    "end_time": end,
                    "obs_id": obs_id,
                    "PI": pi,
                    "exposure": exposure,
                    "target_name": target,
                    "instrument": instrument,
                    "mode": mode,
                    "aperture": aperture,
                    "element": element,
                    "exp_time": exp_time,
                }
            )

    schedules = pd.DataFrame(raw_observations)
    return schedules


def transform_to_across_schedule(filename: str, telescope_id: str) -> AcrossSchedule:
    """Format the schedule data in the ACROSS format"""
    start_datetime = datetime.strptime(filename.replace("timeline_", ""), "%m_%d_%y")
    end_datetime = start_datetime + timedelta(days=7)
    return AcrossSchedule(
        name=f"HST_planned_{filename}",
        telescope_id=telescope_id,
        status="planned",
        fidelity="low",
        date_range={
            "begin": start_datetime.isoformat(),
            "end": end_datetime.isoformat(),
        },
        observations=[],
    )


def is_calibration(observation_data: dict) -> bool:
    """Filter to determine if an observation is a calibration obs"""
    return observation_data["target_name"] in TARGET_NAMES_TO_IGNORE


def extract_observation_pointing_coordinates(
    planned_exposures: pd.DataFrame,
    observation_data: dict,
) -> dict:
    """
    Extract the coordinates from the planned exposures catalog and
    add them to the observation data payload.
    If coordinates cannot be found, return an empty dict.
    """
    # The planned exposure 'name' column is longer, so we need to check if it contains or equals
    # the target name from the timeline observation data
    planned_observation_data = planned_exposures[
        (
            planned_exposures["object_name"].str.contains(
                observation_data["target_name"][:16]
            )
        )
        | (planned_exposures["object_name"] == observation_data["target_name"])
    ]
    if len(planned_observation_data) == 0:
        # Could not find coordinates for this target in the planned exposure catalog, so skip
        # (this is likely a ToO or DDT observation)
        return {}

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

    return {"ra": ra, "dec": dec}


def extract_instrument_info_from_observation(
    observation_data: dict,
    across_instrument: dict,
) -> dict:
    """
    Extract the correct bandpass and corresponding observation type
    from the observation parameters
    """
    matching_filter = None
    for filter_info in across_instrument["filters"]:
        element = observation_data["element"]
        aperture = observation_data["aperture"]
        filter_name = filter_info["name"]

        if element in filter_name or (aperture in filter_name and aperture != "WFC"):
            # ACS WFC filters are matched by element,
            # other instruments can be matched by aperture or element
            matching_filter = filter_info

    if not matching_filter:
        logger.warning(
            "Could not find filter for instrument.",
            element=observation_data["element"],
            aperture=observation_data["aperture"],
        )
        return {}

    bandpass_parameters = {
        "filter_name": matching_filter["name"],
        "min": matching_filter["min_wavelength"],
        "max": matching_filter["max_wavelength"],
        "type": "WAVELENGTH",
        "unit": "angstrom",
    }

    filter_name = matching_filter["name"].split(" ")[
        -1
    ]  # Just get the filter name without HST or instrument name
    if "COS" in across_instrument["short_name"]:
        obs_type = "spectroscopy"  # All COS observations are spectroscopic
    elif filter_name[0] in ["G", "E", "P"] or filter_name[:2] == "FR":
        # "G", "E", "P" = grism, grating, or prism filter elements
        # "FR" = ACS grating ramp filter elements
        obs_type = "spectroscopy"
    else:
        # All the rest are imaging elements
        obs_type = "imaging"

    return {"type": obs_type, "bandpass": bandpass_parameters}


def transform_to_across_observation(
    planned_exposures: pd.DataFrame,
    observation_data: dict,
    instruments: list,
) -> AcrossObservation | dict:
    """
    Format the observation data in the ACROSS format
    Runs methods to extract pointing coordinates and
    instrument info from the raw observation data.
    """
    if "ACQ" in observation_data["mode"]:
        # Ignoring acquisition exposures, so skip
        return {}

    if is_calibration(observation_data):
        logger.debug(
            "Calibration observation", observation_id=observation_data["obs_id"]
        )
        return {}

    # Get instrument short name from observation parameters
    across_instrument_name = get_instrument_name_from_observation_data(
        dict(observation_data)
    )
    if not len(across_instrument_name):
        return {}

    pointing_coord_dict = extract_observation_pointing_coordinates(
        planned_exposures, dict(observation_data)
    )
    if not len(pointing_coord_dict):
        # Ignoring observations without matching coordinates
        return {}
    pointing_coord = SkyCoord(
        pointing_coord_dict["ra"], pointing_coord_dict["dec"], unit=(u.hourangle, u.deg)
    )

    # Get the correct instrument model given the correct name
    across_instrument = [
        instrument
        for instrument in instruments
        if instrument["short_name"] == across_instrument_name
    ][0]

    instrument_info = extract_instrument_info_from_observation(
        observation_data, across_instrument
    )
    if not len(instrument_info):
        return {}

    begin_at = datetime.strptime(
        observation_data["date"] + " " + observation_data["begin_time"],
        "%Y.%j %H:%M:%S",
    ).isoformat()
    end_at = datetime.strptime(
        observation_data["date"] + " " + observation_data["end_time"],
        "%Y.%j %H:%M:%S",
    ).isoformat()

    return AcrossObservation(
        **{
            "instrument_id": across_instrument["id"],
            "object_name": observation_data["target_name"],
            "external_observation_id": observation_data["obs_id"],
            "pointing_position": {
                "ra": pointing_coord.ra.deg,
                "dec": pointing_coord.dec.deg,
            },
            "object_position": {
                "ra": pointing_coord.ra.deg,
                "dec": pointing_coord.dec.deg,
            },
            "pointing_angle": 0.0,  # Assuming no roll
            "date_range": {"begin": begin_at, "end": end_at},
            "exposure_time": float(observation_data["exp_time"]),
            "status": "planned",
            "type": instrument_info["type"],
            "bandpass": instrument_info["bandpass"],
        }
    )


def get_instrument_name_from_observation_data(observation_data: dict) -> str:
    """Method to extract the correct HST instrument info dict given raw observation data"""
    if "ACS" in observation_data["instrument"]:
        short_name = "HST_ACS"
    elif "COS" in observation_data["instrument"]:
        short_name = "HST_COS"
    elif "STIS" in observation_data["instrument"]:
        short_name = "HST_STIS"
    elif (
        "WFC3" in observation_data["instrument"]
        and "UV" in observation_data["instrument"]
    ):
        short_name = "HST_WFC3_UVIS"
    elif (
        "WFC3" in observation_data["instrument"]
        and "IR" in observation_data["instrument"]
    ):
        short_name = "HST_WFC3_IR"
    else:
        logger.warning(
            f"Could not find across-server instrument for {observation_data["instrument"]}"
        )
        return ""

    return short_name


def ingest() -> None:
    """
    Ingests low fidelity planned HST schedules.
    Reads uploaded timeline file for a week in the future and crossmatches
    planned observations with the planned and archived exposure catalog
    to retrieve pointing coordinates.
    NOTE: This does NOT include ToO or DDT observations, because these are not found
    in the planned and archived exposure catalog. Therefore this should be treated
    as a very low fidelity schedule.
    """
    # GET telescope and instrument info from the server
    hst_telescope_info = across_api.telescope.get({"name": "HST"})[0]
    telescope_id = hst_telescope_info["id"]
    instruments = across_api.instrument.get({"telescope_id": telescope_id})

    # Read the planned and archived exposure catalog and the weekly timeline file
    planned_exposures = read_planned_exposure_catalog()
    timeline_file = get_latest_timeline_file()
    timeline_df = read_timeline_file(timeline_file)
    if timeline_df is None:
        return

    # Format schedule metadata
    across_schedule = transform_to_across_schedule(timeline_file, telescope_id)

    for _, observation_data in timeline_df.iterrows():
        # Format observation data in ACROSS format
        across_observation = transform_to_across_observation(
            planned_exposures, dict(observation_data), instruments
        )
        if len(across_observation):
            across_schedule["observations"].append(across_observation)

    across_api.schedule.post(across_schedule)


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
def entrypoint():
    try:
        ingest()
        logger.info("HST schedule ingestion ran successfully")
    except Exception as e:
        logger.error("HST schedule ingestion encountered an unexpected error", err=e)
