import logging
from uuid import uuid4

import astropy.units as u  # type:ignore
import pandas as pd  # type:ignore
from astropy.time import Time  # type:ignore
from fastapi_utils.tasks import repeat_every

logger = logging.getLogger("uvicorn.error")

SECONDS_IN_A_WEEK = 60 * 60 * 24 * 7


async def ingest():
    # When running locally and debugging, it is recommended to save the results of these files and load them from a local path to reduce external thrashing
    # Pointings file is used to determine the sector schedules and contains the schedule start/end and RA/DEC values
    tess_pointings_file = "https://raw.githubusercontent.com/tessgi/tesswcs/main/src/tesswcs/data/pointings.csv"
    sector_pointings_df = pd.read_csv(tess_pointings_file)

    # TESS_orbit_times.csv is used to discretize each orbit as an observation for a given sector from the schedule above
    tess_orbit_times_file = "https://tess.mit.edu/public/files/TESS_orbit_times.csv"
    orbit_observations_df = pd.read_csv(tess_orbit_times_file)

    sector_schedules = zip(
        sector_pointings_df["Sector"],
        sector_pointings_df["RA"],
        sector_pointings_df["Dec"],
        sector_pointings_df["Roll"],
        sector_pointings_df["Start"],
        sector_pointings_df["End"],
    )

    # GET Telecope by name not yet implemented in across-server
    tess_telescope_info = {"id": "some-tess-telescope-uuid"}

    # Initialize List of Schedules to append to
    schedules = []

    for sector, ra, dec, roll, sector_start_date, sector_end_date in sector_schedules:
        schedule = {
            "id": str(uuid4()),
            "name": f"TESS_sector_{sector}",
            "telescope_id": tess_telescope_info["id"],
            "status": "planned",
            "fidelity": "low",
        }

        sched_start_at = Time(sector_start_date, format="jd")
        sched_end_at = Time(sector_end_date, format="jd")

        schedule["date_range"] = {
            "begin": str(sched_start_at.to_datetime()),
            "end": str(sched_end_at.to_datetime()),
        }

        orbit_observations = orbit_observations_df.loc[
            orbit_observations_df["Sector"] == str(sector)
        ]

        sector_orbit_observations = list(
            zip(
                orbit_observations["Orbit"],
                orbit_observations["Start of Orbit"],
                orbit_observations["End of Orbit"],
            )
        )

        # Initialize list of observations to append to
        observations = []

        if len(sector_orbit_observations) == 0:
            # Create one overarching placeholder observation from low fidelity sector pointing schedule when no orbit times exist
            # The pointings file contains a location and can be treated as one observation within the schedule when no orbits are yet planned
            exposure_time = sched_end_at - sched_start_at
            placeholder_observation = {
                "id": str(uuid4()),
                "telescope_id": tess_telescope_info["id"],
                "schedule_id": schedule["id"],
                "object_name": f"TESS_sector_{sector}_placeholder",
                "pointing_position": {"ra": ra, "dec": dec},
                "pointing_angle": roll,
                "date_range": schedule["date_range"],
                "exposure_time": int(exposure_time.to(u.second).value),
                "status": "planned",
                "type": "imaging",
                "bandpass": {
                    "central_wavelength": 8487.01,
                    "bandwidth": 5368.88,
                    "filter_name": "TESS",
                },
            }
            observations.append(placeholder_observation)
        else:
            for i, (orbit, obs_start, obs_end) in enumerate(sector_orbit_observations):
                # orbit start/end times are strings (non isot)
                obs_start_str = str.replace(obs_start, " ", "T")
                obs_end_str = str.replace(obs_end, " ", "T")
                obs_start_at = Time(obs_start_str, format="isot")
                obs_end_at = Time(obs_end_str, format="isot")
                exposure_time = obs_end_at - obs_start_at

                observation = {
                    "id": str(uuid4()),
                    "telescope_id": tess_telescope_info["id"],
                    "schedule_id": schedule["id"],
                    "object_name": f"TESS_sector_{sector}_obs_{i}_orbit_{int(orbit)}",
                    "pointing_position": {"ra": ra, "dec": dec},
                    "pointing_angle": roll,
                    "date_range": {"begin": obs_start_str, "end": obs_end_str},
                    "exposure_time": int(exposure_time.to(u.second).value),
                    "status": "planned",
                    "type": "imaging",
                    "bandpass": {
                        "central_wavelength": 8487.01,
                        "bandwidth": 5368.88,
                        "filter_name": "TESS",
                    },
                }

                observations.append(observation)

        schedule["observations"] = observations
        schedules.append(schedule)

    # POST Schedule not yet implemented in across-server
    logger.info(schedules)
    return


@repeat_every(seconds=SECONDS_IN_A_WEEK)  # Weekly
async def TESS_low_fidelity_schedule_ingestion_task():
    current_time = Time.now()

    try:
        await ingest()
    except Exception as E:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(f"{__name__} encountered an error {E}")

    logger.info(f"{__name__} ran successfully at {current_time}")
