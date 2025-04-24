import logging

import astropy.units as u  # type: ignore[import-untyped]
import httpx
from astropy.time import Time  # type: ignore[import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....util import across_api

logger = logging.getLogger("uvicorn.error")

SECONDS_IN_A_WEEK = 60 * 60 * 24 * 7
IXPE_LTP_URL = "https://ixpe.msfc.nasa.gov/for_scientists/ltp.html"

IXPE_MIN_ENERGY = 2.0  # keV
IXPE_MAX_ENERGY = 8.0  # keV


def fetch_ixpe_schedule(url):
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
            rows = schedule_table.find_all("tr")
            for row in rows:
                columns = row.find_all("td")
                schedule_data.append([col.get_text(strip=True) for col in columns])

        return schedule_data

    except httpx._exceptions.RequestError as e:
        print(f"Error fetching the webpage: {e}")
        return None


def create_ixpe_observation(
    object_name, ra, dec, obs_start, obs_end, instrument_id, P_s, Pnum
):
    # orbit start/end times are strings (non isot)
    obs_start_at = Time(f"{obs_start}:00:00", format="isot")
    obs_end_at = Time(f"{obs_end}:00:00", format="isot")

    exposure_time = obs_end_at - obs_start_at

    mock_external_id = f"ixpe_{str.replace(P_s, " ", "_")}_obs_{Pnum}"
    return {
        "instrument_id": instrument_id,
        "object_name": object_name,
        "external_observation_id": mock_external_id,
        "pointing_position": {"ra": ra, "dec": dec},
        "pointing_angle": 0.0,
        "date_range": {"begin": str(obs_start_at), "end": str(obs_end_at)},
        "exposure_time": int(exposure_time.to(u.second).value),
        "status": "planned",
        "type": "imaging",
        "bandpass": {
            "min": IXPE_MIN_ENERGY,
            "max": IXPE_MAX_ENERGY,
            "unit": "keV",
            "filter_name": "IXPE",
        },
    }


def ingest():
    """
    Fetches the IXPE schedule from the specified URL and returns the parsed data.
    """
    tess_telescope_info = across_api.telescope.get({"name": "ixpe"})[0]
    telescope_id = tess_telescope_info["id"]
    instrument_id = tess_telescope_info["instruments"][0]["id"]
    schedule_data = fetch_ixpe_schedule(IXPE_LTP_URL)
    schedule_observations = []
    if schedule_data:
        # Process the schedule data as needed
        for i, entry in enumerate(schedule_data[1:]):  # Skip header row
            P_s = entry[0]
            Pnum = entry[1]
            Name = entry[2]
            Ra = float(entry[3])
            Dec = float(entry[4])
            start_time = entry[5]

            if i < len(schedule_data) - 1:
                end_time = schedule_data[i + 1][5]
            else:
                end_time = entry[5]

            observation = create_ixpe_observation(
                object_name=Name,
                ra=Ra,
                dec=Dec,
                obs_start=start_time,
                obs_end=end_time,
                instrument_id=instrument_id,
                P_s=P_s,
                Pnum=Pnum,
            )
            schedule_observations.append(observation)

        schedule_start = schedule_observations[0]["date_range"]["begin"]
        schedule_end = schedule_observations[-1]["date_range"]["end"]

        schedule = {
            "name": "IXPE_LTP",
            "telescope_id": telescope_id,
            "status": "planned",
            "fidelity": "low",
        }

        sched_start_at = Time(schedule_start, format="isot")
        sched_end_at = Time(schedule_end, format="isot")

        schedule["date_range"] = {
            "begin": str(sched_start_at.to_datetime()),
            "end": str(sched_end_at.to_datetime()),
        }
        schedule["observations"] = schedule_observations
        across_api.schedule.post(schedule)
    else:
        print("No schedule data found.")


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
def entrypoint():
    current_time = Time.now()

    try:
        schedules = ingest()
        logger.info(f"{__name__} ran at {current_time}")
        return schedules
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(f"{__name__} encountered an error {e} at {current_time}")
        return


if __name__ == "__main__":
    # Run the function to test it
    ingest()
