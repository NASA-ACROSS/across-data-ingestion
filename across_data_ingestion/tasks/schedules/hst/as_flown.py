from datetime import datetime, timedelta, timezone

import pandas as pd
import structlog
from astropy.table import Table  # type: ignore
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk
from ....util.vo_service import VOService
from . import util as hst_util

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


HST_TAP_URL = "https://mast.stsci.edu/vo-tap/api/v0.1/missionmast/async"


async def get_observation_data_from_tap() -> Table | None:
    """
    Get as-flown observations that were taken in the last day.
    Retrieves them from the MAST TAP service.
    More information can be found here: https://mast.stsci.edu/vo-tap/api/v0.1/missionmast/
    """
    async with VOService(HST_TAP_URL) as vo_service:
        # Query for initial parameters
        day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        observations_query = (
            "SELECT * "
            "FROM dbo.hst_science_missionmast "
            "WHERE "
            f"sci_start_time > '{day_ago}' "
            "ORDER BY "
            "sci_start_time desc"
        )

        observations_table = await vo_service.query(observations_query)
        if not observations_table:
            logger.warning("No HST observations found from TAP")
            return None

    return observations_table


def transform_to_across_schedule(
    observations: pd.DataFrame,
    telescope_id: str,
) -> sdk.ScheduleCreate:
    """Format the schedule data in the ACROSS format"""
    start_datetime = min(observations["sci_start_time"])
    end_datetime = max(observations["sci_stop_time"])

    return sdk.ScheduleCreate(
        name=f"HST_as_flown_{start_datetime.split(' ')[0]}_{end_datetime.split(' ')[0]}",
        telescope_id=telescope_id,
        status=sdk.ScheduleStatus.PERFORMED,
        fidelity=sdk.ScheduleFidelity.HIGH,
        date_range=sdk.DateRange(begin=start_datetime, end=end_datetime),
        observations=[],
    )


def extract_instrument_info(
    observation_data: pd.Series,
    instruments: list[sdk.Instrument],
) -> hst_util.InstrumentInfo | None:
    """
    Extract the ACROSS instrument model, correct bandpass,
    and corresponding observation type from the observation parameters
    """
    # Extract instrument name
    obs_instrument = observation_data.sci_instrument_config

    instrument_short_name = hst_util.get_instrument_short_name_from_observation(
        obs_instrument
    )
    if instrument_short_name is None:
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
    filter_names = observation_data.sci_spec_1234.lower()

    matching_filters: list[sdk.Filter] = []
    if across_instrument.filters:
        for across_filter in across_instrument.filters:
            across_filter_name = across_filter.name.lower()

            for filter_name in filter_names.split(";"):
                if (
                    filter_name in across_filter_name
                    or filter_name == across_filter_name
                ):
                    matching_filters.append(across_filter)

    if not matching_filters:
        logger.warning(
            "Could not find filter for instrument.",
            filter_names=observation_data.sci_spec_1234,
        )
        return None

    if len(matching_filters) > 1:
        logger.warning(
            "Multiple filters matched for an element/aperture combination. Selecting the first filter...",
            matches=matching_filters,
        )

    matching_filter = matching_filters[0]

    bandpass_parameters = sdk.WavelengthBandpass(
        filter_name=matching_filter.name,
        min=matching_filter.min_wavelength,
        max=matching_filter.max_wavelength,
        unit=sdk.WavelengthUnit.ANGSTROM,
    )

    obs_type = hst_util.get_obs_type(matching_filter, across_instrument)

    return hst_util.InstrumentInfo(
        id=across_instrument.id,
        bandpass=sdk.Bandpass(bandpass_parameters),
        type=obs_type,
    )


def transform_to_across_observation(
    observation_data: pd.Series,
    instruments: list[sdk.Instrument],
) -> sdk.ObservationCreate | None:
    """
    Format the observation data in the ACROSS format
    """
    instrument_info = extract_instrument_info(observation_data, instruments)
    if instrument_info is None:
        return None
    return sdk.ObservationCreate(
        instrument_id=instrument_info.id,
        object_name=observation_data.sci_targname,
        external_observation_id=str(observation_data.sci_pep_id)
        + str(observation_data.sci_obset_id),
        pointing_position=sdk.Coordinate(
            ra=observation_data.sci_ra,
            dec=observation_data.sci_dec,
        ),
        object_position=sdk.Coordinate(
            ra=observation_data.sci_ra,
            dec=observation_data.sci_dec,
        ),
        pointing_angle=observation_data.sci_pa_aper,
        date_range=sdk.DateRange(
            begin=observation_data.sci_start_time, end=observation_data.sci_stop_time
        ),
        exposure_time=float(observation_data.sci_actual_duration),
        status=sdk.ObservationStatus.PERFORMED,
        type=instrument_info.type,
        bandpass=instrument_info.bandpass,
    )


async def ingest():
    # GET telescope and instrument info from the server
    [telescope] = sdk.TelescopeApi(client).get_telescopes(name="HST")
    instruments = sdk.InstrumentApi(client).get_instruments(telescope_id=telescope.id)

    observations_tab = await get_observation_data_from_tap()
    if not observations_tab:
        return None

    # Convert to pandas df for easier masking
    observations_df = observations_tab.to_pandas()
    across_schedule = transform_to_across_schedule(observations_df, telescope.id)

    non_calibration = ~observations_df["sci_targname"].isin(
        hst_util.TARGET_NAMES_TO_IGNORE
    )
    non_acq_mode = ~observations_df["sci_operating_mode"].str.contains("ACQ", na=False)
    filtered_observation_data = list(
        observations_df[non_calibration & non_acq_mode].itertuples()
    )

    if len(filtered_observation_data) == 0:
        return None

    for row in filtered_observation_data:
        across_obs = transform_to_across_observation(row, instruments)
        if across_obs:
            across_schedule.observations.append(across_obs)

    try:
        sdk.ScheduleApi(client).create_schedule(across_schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.info("Schedule already exists.", schedule_name=across_schedule.name)
        else:
            raise err


@repeat_at(cron="52 5 * * *", logger=logger)
async def entrypoint():
    try:
        await ingest()
        logger.info("HST as-flown schedule ingestion ran successfully")
    except Exception as e:
        logger.error(
            "HST as-flown schedule ingestion encountered an unexpected error",
            err=e,
            exc_info=True,
        )
