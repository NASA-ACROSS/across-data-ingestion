import io
from datetime import datetime

import httpx
import pandas as pd
import structlog
from bs4 import BeautifulSoup
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk
from ....util.footprint_util import project_footprint
from . import util as chandra_util

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


CHANDRA_TOO_SCHEDULE_PAGE_PATH = "https://cxc.harvard.edu/target_lists/tooscheds/"


def read_chandra_too_schedule_page(url: str = CHANDRA_TOO_SCHEDULE_PAGE_PATH) -> str:
    """GET the Chandra dynamic ToO schedule page contents, or raise an error if the request fails"""
    response = httpx.get(url)

    response.raise_for_status()

    return response.text


def match_schedule_name_status_fidelity(
    schedule_name: str,
) -> tuple[sdk.ScheduleStatus, sdk.ScheduleFidelity]:
    """
    Matches the schedule name to the status and fidelity
    """
    if "Potential TOO/DDT" in schedule_name:
        return (
            sdk.ScheduleStatus.PLANNED,
            sdk.ScheduleFidelity.LOW,
        )  # NOTE: Replace with new dynamic status in the future
    elif "Preliminary Schedule" in schedule_name:
        return (sdk.ScheduleStatus.PLANNED, sdk.ScheduleFidelity.LOW)
    elif "Final Schedule" in schedule_name:
        return (sdk.ScheduleStatus.PLANNED, sdk.ScheduleFidelity.HIGH)
    else:
        logger.warning(
            "Could not parse schedule name for status and fidelity",
            schedule_name=schedule_name,
        )
        return (sdk.ScheduleStatus.PLANNED, sdk.ScheduleFidelity.LOW)


def transform_to_across_schedule(
    telescope_id: str,
    observations: pd.DataFrame,
    schedule_name: str,
) -> sdk.ScheduleCreate:
    """Transform the contents of the observation table to the ACROSS ScheduleCreate interface"""
    begin = datetime.strptime(observations["Start"].min(), "%Y:%j:%H:%M:%S.%f")
    end = datetime.strptime(observations["Stop"].max(), "%Y:%j:%H:%M:%S.%f")

    schedule_status, schedule_fidelity = match_schedule_name_status_fidelity(
        schedule_name
    )

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"chandra_{schedule_name.replace(' ', '_').lower()}_{begin.isoformat().split('T')[0]}_{end.isoformat().split('T')[0]}",
        date_range=sdk.DateRange(begin=begin, end=end),
        status=schedule_status,
        fidelity=schedule_fidelity,
        observations=[],
    )


def get_observation_data_from_html_table(
    df: pd.DataFrame,
    instruments_by_short_name: dict[str, sdk.TelescopeInstrument],
) -> list[sdk.ObservationCreate]:
    """Iterate over the observations DataFrame and transform each row to the ACROSS ObservationCreate interface"""

    across_observations: list[sdk.ObservationCreate] = []
    for _, observation_data in df.iterrows():
        observation_params = chandra_util.ChandraObservationInstrumentParams(
            instrument=observation_data["SI"],
            grating=observation_data["Grat"],
        )
        instrument = chandra_util.match_instrument_from_observation(
            instruments_by_short_name, observation_params
        )
        across_observations.append(
            transform_to_observation(observation_data, instrument)
        )

    return across_observations


def transform_to_observation(
    row: pd.Series, instrument: sdk.TelescopeInstrument
) -> sdk.ObservationCreate:
    """Transform ecach row in the observations DataFrame to the ACROSS ObservationCreate interface"""
    begin = datetime.strptime(row["Start"], "%Y:%j:%H:%M:%S.%f")
    end = datetime.strptime(row["Stop"], "%Y:%j:%H:%M:%S.%f")
    exposure_time = row["Time"] * 1000.0

    observation_type = chandra_util.CHANDRA_OBSERVATION_TYPES[
        instrument.short_name or ""
    ]
    bandpass = chandra_util.CHANDRA_BANDPASSES[instrument.short_name or ""]

    footprint = None
    if instrument.footprints:
        footprint = project_footprint(
            instrument.footprints,
            ra=float(row["RA"]),
            dec=float(row["Dec"]),
            roll_angle=float(row["Roll"]),
        )

    return sdk.ObservationCreate(
        instrument_id=instrument.id,
        object_name=row["Target"],
        pointing_position=sdk.Coordinate(
            ra=float(row["RA"]),
            dec=float(row["Dec"]),
        ),
        date_range=sdk.DateRange(
            begin=begin,
            end=end,
        ),
        external_observation_id=str(row["ObsID"]),
        type=observation_type,
        status=sdk.ObservationStatus.PLANNED,
        pointing_angle=float(row["Roll"]),
        exposure_time=exposure_time,
        bandpass=bandpass,
        footprint=footprint,
    )


async def ingest() -> None:
    # GET Telescope by name
    telescope = sdk.TelescopeApi(client).get_telescopes(
        name="chandra", include_footprints=True
    )[0]

    if not telescope.instruments:
        logger.warning("No instruments found.")
        return

    instruments_by_short_name = {
        instrument.short_name: instrument
        for instrument in telescope.instruments
        if instrument.short_name
    }

    too_schedule_page = read_chandra_too_schedule_page()

    soup = BeautifulSoup(too_schedule_page, "html.parser")
    schedules: list[sdk.ScheduleCreate] = []

    # Scrape schedule tables
    tables = soup.find_all("pre")
    for table in tables:
        schedule_name_heading = table.find_previous("h3")
        if not schedule_name_heading:
            logger.warning("Could not read schedule name heading")
            continue
        schedule_name = schedule_name_heading.text

        df = pd.read_fwf(io.StringIO(table.text), header=0, skiprows=[2])

        # Drop calibration rows (no instrument info)
        df.drop(df[df["SI"] == "--"].index, inplace=True)

        if len(df) > 0:
            schedule = transform_to_across_schedule(telescope.id, df, schedule_name)
            observations = get_observation_data_from_html_table(
                df,
                instruments_by_short_name,
            )
            schedule.observations = observations
            schedules.append(schedule)

    for schedule in schedules:
        try:
            sdk.ScheduleApi(client).create_schedule(schedule)
        except sdk.ApiException as err:
            if err.status == 409:
                logger.info("Schedule already exists.", schedule_name=schedule.name)


@repeat_at(cron="42 */2 * * *", logger=logger)
async def entrypoint() -> None:
    try:
        await ingest()
        logger.info("Chandra preliminary ToO schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(
            "Chandra preliminary ToO schedule ingestion encountered an error",
            err=e,
            exc_info=True,
        )
        return
