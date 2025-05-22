import logging

import astropy.units as u  # type: ignore[import-untyped]
import pandas as pd
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_WEEK
from ....util import across_api

logger = logging.getLogger("uvicorn.error")

TESS_BANDPASS = {
    "min": 6000,
    "max": 10000,
    "peak_wavelength": 7865,
    "unit": "angstrom",
    "filter_name": "TESS_red",
}

# When running locally and debugging, it is recommended to save the results of these files and load them from a local path to reduce external thrashing
TESS_POINTINGS_FILE = "https://raw.githubusercontent.com/tessgi/tesswcs/main/src/tesswcs/data/pointings.csv"
TESS_ORBIT_TIMES_FILE = "https://tess.mit.edu/public/files/TESS_orbit_times.csv"


def create_orbit_observation(
    sector, ra, dec, roll, i, orbit, obs_start, obs_end, instrument_id
):
    # orbit start/end times are strings (non isot)
    obs_start_str = str.replace(obs_start, " ", "T")
    obs_end_str = str.replace(obs_end, " ", "T")
    obs_start_at = Time(obs_start_str, format="isot")
    obs_end_at = Time(obs_end_str, format="isot")
    exposure_time = obs_end_at - obs_start_at

    return {
        "instrument_id": instrument_id,
        "object_name": f"TESS_sector_{int(sector)}_obs_{i}_orbit_{int(orbit)}",
        "external_observation_id": f"TESS_sector_{int(sector)}_obs_{i}_orbit_{int(orbit)}",
        "pointing_position": {"ra": ra, "dec": dec},
        "pointing_angle": roll,
        "date_range": {"begin": obs_start_str, "end": obs_end_str},
        "exposure_time": int(exposure_time.to(u.second).value),
        "status": "planned",
        "type": "imaging",
        "bandpass": TESS_BANDPASS,
    }


def create_placeholder_observation(
    schedule, sector, ra, dec, roll, sched_start_at, sched_end_at, instrument_id
):
    exposure_time = sched_end_at - sched_start_at
    return {
        "instrument_id": instrument_id,
        "object_name": f"TESS_sector_{int(sector)}_placeholder",
        "external_observation_id": f"TESS_sector_{int(sector)}_placeholder",
        "pointing_position": {"ra": ra, "dec": dec},
        "pointing_angle": roll,
        "date_range": schedule["date_range"],
        "exposure_time": int(exposure_time.to(u.second).value),
        "status": "planned",
        "type": "imaging",
        "bandpass": TESS_BANDPASS,
    }


def ingest():
    """
    Method that posts TESS low fidelity observing schedules via two known webfiles:
        sector_pointings_file:
            https://raw.githubusercontent.com/tessgi/tesswcs/main/src/tesswcs/data/pointings.csv
        -> Contains information about sectors and their pointing times.

        orbit time file:
            https://tess.mit.edu/public/files/TESS_orbit_times.csv
        -> Contains information about the individual sectors orbits.

    It iterates over the values of the sector_pointings_file, and cross-references the orbit time files to find intervals
    in which the telescope was not observing. If it doesn't find a cross-reference to the orbit file it will default to a
    schedule with a single observation with the date range being for the entire sector.
    """

    # Pointings file is used to determine the sector schedules and contains the schedule start/end and RA/DEC values
    sector_pointings_df = pd.read_csv(TESS_POINTINGS_FILE)

    # TESS_orbit_times.csv is used to discretize each orbit as an observation for a given sector from the schedule above
    orbit_observations_df = pd.read_csv(TESS_ORBIT_TIMES_FILE)

    columns = ["Sector", "RA", "Dec", "Roll", "Start", "End"]
    sector_schedules = list(zip(*(sector_pointings_df[col] for col in columns)))

    # GET Telescope by name
    tess_telescope_info = across_api.telescope.get({"name": "tess"})[0]
    telescope_id = tess_telescope_info["id"]
    instrument_id = tess_telescope_info["instruments"][0]["id"]

    # Initialize List of Schedules to append
    schedules = []

    # Iterate pointings file by row
    for sector, ra, dec, roll, sector_start_date, sector_end_date in sector_schedules:
        # Create base schedule from  each pointings file row and set date range
        schedule = {
            "name": f"TESS_sector_{int(sector)}",
            "telescope_id": telescope_id,
            "status": "planned",
            "fidelity": "low",
        }
        sched_start_at = Time(sector_start_date, format="jd")
        sched_end_at = Time(sector_end_date, format="jd")
        schedule["date_range"] = {
            "begin": str(sched_start_at.to_datetime()),
            "end": str(sched_end_at.to_datetime()),
        }

        # Find planned orbits from TESS_orbit_times.csv for current sector from pointings row
        orbit_observations = orbit_observations_df.loc[
            orbit_observations_df["Sector"] == str(int(sector))
        ]
        observation_columns = ["Orbit", "Start of Orbit", "End of Orbit"]
        sector_orbit_observations = list(
            zip(*(orbit_observations[col] for col in observation_columns))
        )

        # Initialize list of observations to append
        observations = []
        # When TESS_orbit_times.csv has planned orbits for current sector
        if len(sector_orbit_observations):
            # Iterate and add orbits as observations
            for i, (orbit, obs_start, obs_end) in enumerate(sector_orbit_observations):
                observation = create_orbit_observation(
                    sector, ra, dec, roll, i, orbit, obs_start, obs_end, instrument_id
                )
                observations.append(observation)
        else:
            # Create one placeholder observation from low fidelity sector pointing file when no orbit times exist
            # The pointings file contains a location and can be treated as one observation within the schedule when no orbits are yet planned
            placeholder_observation = create_placeholder_observation(
                schedule,
                sector,
                ra,
                dec,
                roll,
                sched_start_at,
                sched_end_at,
                instrument_id,
            )
            observations.append(placeholder_observation)

        schedule["observations"] = observations
        schedules.append(schedule)

        across_api.schedule.post(schedule)

    return schedules


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
