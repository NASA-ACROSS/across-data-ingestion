from datetime import datetime
from typing import Any

import pandas as pd
import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.getLogger()

TESS_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "min": 6000,
        "max": 10000,
        "peak_wavelength": 7865,
        "unit": sdk.WavelengthUnit.ANGSTROM,
        "filter_name": "TESS_red",
    }
)

# When running locally and debugging, it is recommended to save the results of these files and load them from a local path to reduce external thrashing
TESS_POINTINGS_FILE = "https://raw.githubusercontent.com/tessgi/tesswcs/main/src/tesswcs/data/pointings.csv"
TESS_ORBIT_TIMES_FILE = "https://tess.mit.edu/public/files/TESS_orbit_times.csv"


def transform_to_across_orbit_observation(
    idx: int,
    obs: Any,  # Pandas namedtuple; no good typing for it
    pointing: Any,
    instrument_id,
) -> sdk.ObservationCreate:
    # orbit start/end times are strings (non isot)
    obs_start_str = str.replace(obs.start_of_orbit, " ", "T")
    obs_end_str = str.replace(obs.end_of_orbit, " ", "T")
    begin: datetime = Time(obs_start_str, format="isot").to_datetime()
    end: datetime = Time(obs_end_str, format="isot").to_datetime()

    exposure_time = end - begin

    object_name = f"TESS_sector_{int(obs.sector)}_obs_{idx}_orbit_{int(obs.orbit)}"

    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=object_name,
        external_observation_id=object_name,
        pointing_position=sdk.Coordinate(ra=pointing.ra, dec=pointing.dec),
        pointing_angle=pointing.roll,
        date_range=sdk.DateRange(begin=begin, end=end),
        exposure_time=exposure_time.total_seconds(),
        status=sdk.ObservationStatus.PLANNED,
        type=sdk.ObservationType.IMAGING,
        bandpass=sdk.Bandpass(TESS_BANDPASS),
    )


def transform_to_across_placeholder_observation(
    pointing: Any,
    date_range: sdk.DateRange,
    instrument_id: str,
):
    exposure_time = date_range.end - date_range.begin
    object_name = f"TESS_sector_{pointing.sector}_placeholder"

    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=object_name,
        external_observation_id=object_name,
        pointing_position=sdk.Coordinate(ra=pointing.ra, dec=pointing.dec),
        pointing_angle=pointing.roll,
        date_range=date_range,
        exposure_time=exposure_time.total_seconds(),
        status=sdk.ObservationStatus.PLANNED,
        type=sdk.ObservationType.IMAGING,
        bandpass=sdk.Bandpass(TESS_BANDPASS),
    )


def ingest():
    """
    Method that posts TESS low fidelity observing schedules via two known web files:
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

    logger.info("Gathering schedule data")

    files = [
        # Pointings file is used to determine the sector schedules
        # and contains the schedule start/end and RA/DEC values
        TESS_POINTINGS_FILE,
        # TESS_orbit_times.csv is used to discretize each orbit as
        # an observation for a given sector from the schedule above
        TESS_ORBIT_TIMES_FILE,
    ]

    sector_pointings_df, orbit_observations_df = [
        pd.read_csv(file, comment="#").rename(
            # rename to add underscores and lowercase for more pythonic field naming.
            columns=lambda c: c.replace(" ", "_").lower()
        )
        for file in files
    ]

    # GET Telescope by name
    (tess_telescope,) = sdk.TelescopeApi(client).get_telescopes(name="tess")
    telescope_id = tess_telescope.id
    instrument_id = tess_telescope.instruments[0].id

    # Initialize List of Schedules to append
    schedules = []

    logger.info("Transforming schedules...")

    # Iterate pointings file by row
    for pointing in sector_pointings_df.itertuples(index=False):
        # Create base schedule from  each pointings file row and set date range
        sched_start_at = Time(pointing.start, format="jd")
        sched_end_at = Time(pointing.end, format="jd")
        schedule_name = f"TESS_sector_{pointing.sector}"

        logger.debug("Transforming schedule", name=schedule_name)

        schedule = sdk.ScheduleCreate(
            name=schedule_name,
            telescope_id=telescope_id,
            status=sdk.ScheduleStatus.PLANNED,
            fidelity=sdk.ScheduleFidelity.LOW,
            date_range=sdk.DateRange(
                begin=sched_start_at.to_datetime(),
                end=sched_end_at.to_datetime(),
            ),
            observations=[],
        )

        # Find planned orbits from TESS_orbit_times.csv for current sector from pointings row
        matched_orbit_observations_df = orbit_observations_df.loc[
            orbit_observations_df.sector == pointing.sector
        ]

        orbit_observations = list(matched_orbit_observations_df.itertuples(index=False))

        # When TESS_orbit_times.csv has planned orbits for current sector
        logger.debug(
            "Transforming observations...", orbit_observations=len(orbit_observations)
        )
        if len(orbit_observations):
            # Iterate and add orbits as observations
            for idx, orbit_data in enumerate(orbit_observations):
                observation = transform_to_across_orbit_observation(
                    idx, orbit_data, pointing, instrument_id
                )
                schedule.observations.append(observation)
        else:
            # Create one placeholder observation from low fidelity sector pointing file when no orbit times exist
            # The pointings file contains a location and can be treated as one observation within the schedule
            # when no orbits are yet planned
            placeholder_observation = transform_to_across_placeholder_observation(
                pointing,
                schedule.date_range,
                instrument_id,
            )

            schedule.observations.append(placeholder_observation)

        schedules.append(schedule)

    try:
        logger.debug("Posting Schedules")
        create_many = sdk.ScheduleCreateMany(
            schedules=schedules,
            telescope_id=telescope_id,
        )
        sdk.ScheduleApi(client).create_many_schedules(create_many)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.warning("A schedule already exists", extra=err.__dict__)
        else:
            raise err

    return schedules


@repeat_at(cron="12 22 * 8 *", logger=logger)
async def entrypoint():
    try:
        logger.info("Schedule ingestion started.")
        schedules = ingest()
        logger.info("Schedule ingestion completed.")
        return schedules
    except Exception as e:
        logger.error("Encountered an unknown error", err=e, exc_info=True)
        return
