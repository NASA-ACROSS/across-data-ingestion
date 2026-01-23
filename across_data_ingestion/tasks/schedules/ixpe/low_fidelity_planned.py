import astropy.units as u  # type: ignore[import-untyped]
import bs4
import httpx
import pandas as pd
import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from across_data_ingestion.util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

IXPE_LTP_URL = "https://ixpe.msfc.nasa.gov/for_scientists/ltp.html"

IXPE_BANDPASS = sdk.EnergyBandpass(
    min=2.0,
    max=8.0,
    unit=sdk.EnergyUnit.KEV,
    filter_name="IXPE",
)


def query_ixpe_schedule() -> pd.DataFrame:
    # Send a GET request to the webpage
    response = httpx.get(IXPE_LTP_URL)
    response.raise_for_status()

    try:
        # Parse the webpage content with BeautifulSoup
        soup = bs4.BeautifulSoup(response.text, "html.parser")

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

    except Exception as e:
        logger.error(
            "Unknown error occurred while converting from HTML to pd.DataFrame",
            err=e,
            exc_info=True,
        )

    return pd.DataFrame()


def ixpe_to_across_schedule(
    telescope_id: str,
    data: pd.DataFrame,
    status: sdk.ScheduleStatus,
    fidelity: sdk.ScheduleFidelity,
) -> sdk.ScheduleCreate:
    """
    Creates a IXPE schedule from the provided data.
    """

    begin = Time(f"{min(data['Start'])}", format="iso").isot
    end = Time(f"{max(data['Stop'])}", format="iso").isot

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"ixpe_ltp_{begin.split('T')[0]}_{end.split('T')[0]}",
        date_range=sdk.DateRange(
            begin=begin,
            end=end,
        ),
        status=status,
        fidelity=fidelity,
        observations=[],
    )


def ixpe_to_across_observation(instrument_id: str, row: dict) -> sdk.ObservationCreate:
    """
    Creates a IXPE observation from the provided row of data.
    Calculates the exposure time from the End - Start
    Sets the external_id a custom value based off of the P S and Pnum values
    """
    obs_start_at = Time(row["Start"], format="iso")
    obs_end_at = Time(row["Stop"], format="iso")

    exposure_time = obs_end_at - obs_start_at

    external_id = f"{str.replace(row['P S'], ' ', '_')}_obs_{row['Pnum']}"

    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=f"{row['Name']}",
        pointing_position=sdk.Coordinate(
            ra=float(row["RA"]),
            dec=float(row["Dec"]),
        ),
        object_position=sdk.Coordinate(
            ra=float(row["RA"]),
            dec=float(row["Dec"]),
        ),
        date_range=sdk.DateRange(
            begin=obs_start_at.isot,
            end=obs_end_at.isot,
        ),
        external_observation_id=external_id,
        type=sdk.ObservationType.IMAGING,
        status=sdk.ObservationStatus.PLANNED,
        exposure_time=int(exposure_time.to(u.second).value),
        bandpass=sdk.Bandpass(IXPE_BANDPASS),
        pointing_angle=0.0,  # No Value for position angle -_-
    )


def ingest() -> None:
    """
    Fetches the IXPE schedule from the specified URL and returns the parsed data.
    """
    ixpe_df = query_ixpe_schedule()
    if len(ixpe_df) == 0:
        logger.warning("Failed to read IXPE timeline file")
        return

    # GET Telescope by name
    telescope = sdk.TelescopeApi(client).get_telescopes(name="ixpe")[0]

    if not telescope.instruments:
        logger.error("Telescope has no instruments")
        return

    instrument_id = telescope.instruments[0].id

    # Initialize schedule
    schedule = ixpe_to_across_schedule(
        telescope_id=telescope.id,
        data=ixpe_df,
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=sdk.ScheduleFidelity.LOW,
    )

    # Transform dataframe to list of dictionaries
    observation_rows = ixpe_df.to_dict(orient="records")

    # Transform observations
    schedule.observations = [
        ixpe_to_across_observation(instrument_id, row) for row in observation_rows
    ]

    # Post schedule
    try:
        sdk.ScheduleApi(client).create_schedule(schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.info("Schedule already exists.", schedule_name=schedule.name)


@repeat_at(cron="29 0 * * 2", logger=logger)
async def entrypoint():
    try:
        ingest()
        logger.info("Schedule ingestion completed.")
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error("Schedule ingestion encountered an unknown error.", err=e)
