import astropy.units as u  # type: ignore[import-untyped]
import httpx
import pandas as pd
import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_WEEK
from ....util import across_api
from ..types import AcrossObservation, AcrossSchedule

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

IXPE_LTP_URL = "https://ixpe.msfc.nasa.gov/for_scientists/ltp.html"

IXPE_BANDPASS = {"min": 2.0, "max": 12.0, "unit": "keV", "filter_name": "IXPE"}


def query_ixpe_schedule(url) -> pd.DataFrame | None:
    try:
        # Send a GET request to the webpage
        response = httpx.get(url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the webpage content with BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract schedule information (adjust selectors based on the webpage structure)
        schedule_table = soup.find("table")  # Assuming the schedule is in a table
        schedule_data = []

        if schedule_table:
            rows = schedule_table.find_all("tr")  # type: ignore
            for row in rows:
                columns = row.find_all("td")
                schedule_data.append([col.get_text(strip=True) for col in columns])

            # Convert to a pandas dataframe with header information
            header = schedule_data.pop(0)

            ixpe_df = pd.DataFrame(schedule_data, columns=header)

            # IXPE doesn't give an end time for these schedules
            # Populate them with the next observations start time.
            # The last target won't have an end time, so fill it with its start time
            ixpe_df["Start"] = pd.to_datetime(ixpe_df["Start"])
            ixpe_df["Stop"] = ixpe_df["Start"].shift(-1).fillna(ixpe_df["Start"])
            ixpe_df["Start"] = ixpe_df["Start"].astype(str)
            ixpe_df["Stop"] = ixpe_df["Stop"].astype(str)

            return ixpe_df

        return None

    except Exception:
        return None


def ixpe_schedule(
    telescope_id: str, data: pd.DataFrame, status: str, fidelity: str
) -> AcrossSchedule | dict:
    """
    Creates a IXPE schedule from the provided data.
    """

    begin = Time(f"{min(data["Start"])}", format="iso").isot
    end = Time(f"{max(data["Stop"])}", format="iso").isot

    return {
        "telescope_id": telescope_id,
        "name": f"ixpe_ltp_{begin.split('T')[0]}_{end.split('T')[0]}",
        "date_range": {
            "begin": begin,
            "end": end,
        },
        "status": status,
        "fidelity": fidelity,
    }


def ixpe_observation(instrument_id: str, row: dict) -> AcrossObservation:
    """
    Creates a IXPE observation from the provided row of data.
    Calculates the exposure time from the End - Start
    Sets the external_id a custom value based off of the P S and Pnum values
    """
    obs_start_at = Time(row["Start"], format="iso")
    obs_end_at = Time(row["Stop"], format="iso")

    exposure_time = obs_end_at - obs_start_at

    external_id = f"ixpe_{str.replace(row["P S"], " ", "_")}_obs_{row['Pnum']}"

    return {
        "instrument_id": instrument_id,
        "object_name": f"{row["Name"]}",
        "pointing_position": {
            "ra": f"{row["RA"]}",
            "dec": f"{row["Dec"]}",
        },
        "object_position": {
            "ra": f"{row["RA"]}",
            "dec": f"{row["Dec"]}",
        },
        "date_range": {
            "begin": obs_start_at.isot,
            "end": obs_end_at.isot,
        },
        "external_observation_id": external_id,
        "type": "imaging",
        "status": "planned",
        "exposure_time": int(exposure_time.to(u.second).value),
        "bandpass": IXPE_BANDPASS,
        "pointing_angle": 0.0,  # No Value for position angle -_-
    }


def ingest() -> None:
    """
    Fetches the IXPE schedule from the specified URL and returns the parsed data.
    """
    ixpe_df = query_ixpe_schedule(IXPE_LTP_URL)
    if ixpe_df is None:
        logger.warn("Failed to read IXPE timeline file")
        return

    # GET Telescope by name
    tess_telescope_info = across_api.telescope.get({"name": "ixpe"})[0]
    telescope_id = tess_telescope_info["id"]
    instrument_id = tess_telescope_info["instruments"][0]["id"]

    # Initialize schedule
    schedule = ixpe_schedule(
        telescope_id=telescope_id, data=ixpe_df, status="planned", fidelity="low"
    )

    # Transform dataframe to list of dictionaries
    schedule_observations = ixpe_df.to_dict(orient="records")

    # Transform observations
    schedule["observations"] = [
        ixpe_observation(instrument_id, row) for row in schedule_observations
    ]

    # Post schedule
    across_api.schedule.post(dict(schedule))


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
def entrypoint():
    try:
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Schedule ingestion encountered an unknown error.", err=e)
