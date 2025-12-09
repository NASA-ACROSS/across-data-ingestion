from datetime import datetime, timedelta, timezone

import structlog
from astropy.table import Row, Table  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk
from ....util.vo_service import VOService
from . import util as chandra_util

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


async def get_observation_data_from_tap() -> Table:
    """Query Chandra TAP service to get observation parameters"""
    async with VOService(chandra_util.CHANDRA_TAP_URL) as vo_service:
        # Query for initial parameters
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        observations_query = (
            "SELECT "
            "   o.obsid, "
            "   o.target_name, "
            "   o.start_date, "
            "   o.ra, "
            "   o.dec, "
            "   o.instrument, "
            "   o.grating, "
            "   o.exposure_mode, "
            "   o.exposure_time, "
            "   o.proposal_number "
            "FROM cxc.observation o "
            "WHERE "
            f"  o.start_date > '{week_ago}' AND o.status='observed' "
            "ORDER BY "
            "   o.start_date DESC"
        )

        observations_table = await vo_service.query(observations_query)
        if not observations_table:
            logger.warning("No observations found.")
            return Table()

        return observations_table


def transform_to_observation(
    tap_obs: Row, instrument: sdk.TelescopeInstrument
) -> sdk.ObservationCreate:
    begin = tap_obs["start_date"]
    end = (
        Time(begin, format="isot")
        + timedelta(seconds=tap_obs["exposure_time"] * 1000.0)
    ).isot

    return sdk.ObservationCreate(
        instrument_id=instrument.id,
        object_name=tap_obs["target_name"],
        pointing_position=sdk.Coordinate(
            ra=float(tap_obs["ra"]),
            dec=float(tap_obs["dec"]),
        ),
        object_position=sdk.Coordinate(
            ra=float(tap_obs["ra"]),
            dec=float(tap_obs["dec"]),
        ),
        date_range=sdk.DateRange(
            begin=begin,
            end=end,
        ),
        external_observation_id=str(tap_obs["obsid"]),
        type=chandra_util.CHANDRA_OBSERVATION_TYPES[instrument.short_name or ""],
        status=sdk.ObservationStatus.PERFORMED,
        pointing_angle=0.0,
        exposure_time=float(tap_obs["exposure_time"]) * 1000.0,
        bandpass=chandra_util.CHANDRA_BANDPASSES[instrument.short_name or ""],
        proposal_reference=str(tap_obs["proposal_number"]),
    )


async def ingest() -> None:
    """
    Ingests all executed Chandra observations within the past week
    by submitting a TAP query using the Chandra VO service.

    Transforms the data into ACROSS ScheduleCreate and ObservationCreate interfaces,
    matches the correct Chandra instrument given the observation parameters,
    and pushes the schedule to the across-server create schedule endpoint.
    """
    # GET Telescope by name
    telescope = sdk.TelescopeApi(client).get_telescopes(name="chandra")[0]

    if not telescope.instruments:
        logger.warning("No instruments found.")
        return

    tap_observation_table = await get_observation_data_from_tap()
    if not len(tap_observation_table):
        return

    schedule = chandra_util.create_schedule(
        telescope_id=telescope.id,
        tap_observations=tap_observation_table,
        schedule_type="as_flown",
        schedule_status=sdk.ScheduleStatus.PERFORMED,
        schedule_fidelity=sdk.ScheduleFidelity.HIGH,
    )

    instruments_by_short_name = {
        instrument.short_name: instrument
        for instrument in telescope.instruments
        if instrument.short_name
    }

    for observation_data in tap_observation_table:
        instrument = chandra_util.match_instrument_from_tap_observation(
            instruments_by_short_name, observation_data
        )
        observation = transform_to_observation(observation_data, instrument)
        schedule.observations.append(observation)

    try:
        sdk.ScheduleApi(client).create_schedule(schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.info("Schedule already exists.", schedule_name=schedule.name)


@repeat_at(cron="13 4 * * 7", logger=logger)
async def entrypoint() -> None:
    try:
        await ingest()
        logger.info("Chandra as-flown schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(
            "Chandra as-flown schedule ingestion encountered an error",
            err=e,
            exc_info=True,
        )
        return
