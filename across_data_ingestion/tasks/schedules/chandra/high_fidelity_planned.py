from datetime import datetime, timedelta, timezone

import structlog
from astropy.table import Row, Table, join  # type: ignore[import-untyped]
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every

from ....core.constants import SECONDS_IN_A_DAY
from ....util.across_server import client, sdk
from ....util.vo_service import VOService

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


# Chandra has multiple instruments with different bandpasses
CHANDRA_ACIS_BANDPASS = sdk.Bandpass(
    sdk.EnergyBandpass(
        filter_name="Chandra ACIS",
        min=0.1,
        max=10.0,
        unit=sdk.EnergyUnit.KEV,
    )
)

CHANDRA_HETG_BANDPASS = sdk.Bandpass(
    sdk.EnergyBandpass(
        filter_name="Chandra HETG",
        min=0.6,
        max=10.0,
        unit=sdk.EnergyUnit.KEV,
    )
)

CHANDRA_LETG_BANDPASS = sdk.Bandpass(
    sdk.EnergyBandpass(
        filter_name="Chandra LETG",
        min=0.1,
        max=6.0,
        unit=sdk.EnergyUnit.KEV,
    )
)

CHANDRA_HRC_BANDPASS = sdk.Bandpass(
    sdk.EnergyBandpass(
        filter_name="Chandra HRC",
        min=0.1,
        max=10.0,
        unit=sdk.EnergyUnit.KEV,
    )
)

CHANDRA_BANDPASSES: dict[str, sdk.Bandpass] = {
    "ACIS": CHANDRA_ACIS_BANDPASS,
    "ACIS-HETG": CHANDRA_HETG_BANDPASS,
    "ACIS-LETG": CHANDRA_LETG_BANDPASS,
    "ACIS-CC": CHANDRA_ACIS_BANDPASS,
    "HRC": CHANDRA_HRC_BANDPASS,
    "HRC-HETG": CHANDRA_HETG_BANDPASS,
    "HRC-LETG": CHANDRA_LETG_BANDPASS,
    "HRC-Timing": CHANDRA_HRC_BANDPASS,
}


# Each Chandra instrument has a different observation type
CHANDRA_OBSERVATION_TYPES: dict[str, sdk.ObservationType] = {
    "ACIS": sdk.ObservationType.IMAGING,
    "ACIS-HETG": sdk.ObservationType.SPECTROSCOPY,
    "ACIS-LETG": sdk.ObservationType.SPECTROSCOPY,
    "ACIS-CC": sdk.ObservationType.TIMING,
    "HRC": sdk.ObservationType.IMAGING,
    "HRC-HETG": sdk.ObservationType.SPECTROSCOPY,
    "HRC-LETG": sdk.ObservationType.SPECTROSCOPY,
    "HRC-Timing": sdk.ObservationType.TIMING,
}

CHANDRA_TAP_URL = "https://cda.cfa.harvard.edu/cxctap/async"


def match_instrument_from_tap_observation(
    instruments_by_short_name: dict[str, sdk.IDNameSchema], tap_obs: Row
) -> sdk.IDNameSchema:
    """
    Constructs the instrument name from the observation parameters and
    returns both the name and the instrument id in across-server
    """

    short_name = None
    if "ACIS" in tap_obs["instrument"]:
        if tap_obs["grating"] == "NONE" and tap_obs["exposure_mode"] != "CC":
            short_name = "ACIS"
        elif tap_obs["grating"] in ["HETG", "LETG"]:
            short_name = f"ACIS-{tap_obs["grating"]}"
        elif tap_obs["exposure_mode"] == "CC":
            short_name = "ACIS-CC"

    elif "HRC" in tap_obs["instrument"]:
        if tap_obs["exposure_mode"] != "":
            short_name = "HRC-Timing"
        elif tap_obs["grating"] == "NONE":
            short_name = "HRC"
        elif tap_obs["grating"] in ["HETG", "LETG"]:
            short_name = f"HRC-{tap_obs["grating"]}"

    if not short_name:
        logger.warning(
            "Could not parse observation parameters for correct instrument",
            tap_observation=tap_obs,
        )
        return sdk.IDNameSchema(id="", name="", short_name=None)

    return instruments_by_short_name[short_name]


def create_schedule(telescope_id: str, tap_observations: Table) -> sdk.ScheduleCreate:
    begin = f"{min([data["start_date"] for data in tap_observations])}"
    end = f"{max([data["start_date"] for data in tap_observations])}"

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"chandra_high_fidelity_planned_{begin.split('T')[0]}_{end.split('T')[0]}",
        date_range=sdk.DateRange(
            begin=datetime.fromisoformat(begin), end=datetime.fromisoformat(end)
        ),
        status=sdk.ScheduleStatus.SCHEDULED,
        fidelity=sdk.ScheduleFidelity.HIGH,
        observations=[],
    )


async def get_observation_data_from_tap() -> Table:
    """Query Chandra TAP service to get most of the observation parameters"""
    async with VOService(CHANDRA_TAP_URL) as vo_service:
        # Query for initial parameters
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        observations_query = (
            "SELECT "
            "   o.obsid, "
            "   o.start_date, "
            "   o.ra, "
            "   o.dec, "
            "   o.instrument, "
            "   o.grating, "
            "   o.exposure_mode "
            "FROM cxc.observation o "
            "WHERE "
            f"  o.start_date > '{now}' AND o.status='scheduled' "
            "ORDER BY "
            "   o.start_date DESC"
        )

        observations_table = await vo_service.query(observations_query)
        if not observations_table:
            logger.warning("No observations found.")
            return Table()

        # Query for exposure times needs ids to be strings, since
        # they are VARCHARs in from TAP Schema
        obs_ids = "', '".join(str(obsid["obsid"]) for obsid in observations_table)

        exposure_times_query = (
            "SELECT "
            "   obs_id, "
            "   target_name, "
            "   t_plan_exptime "
            "FROM ivoa.obsplan "
            "WHERE "
            f"   obs_id in ('{obs_ids}')"
        )
        exposure_times_table = await vo_service.query(exposure_times_query)

        if not exposure_times_table:
            logger.warning("No exposure times for observations found")
            return Table()

        if len(exposure_times_table) != len(observations_table):
            logger.warning(
                "Mismatched number of exposure time records to actual observation records.",
            )

        # convert both id cols to strings for joining the tables
        exposure_times_table["obs_id"] = exposure_times_table["obs_id"].astype(str)
        observations_table["obsid"] = observations_table["obsid"].astype(str)

        joined_table = join(
            observations_table,
            exposure_times_table,
            keys_left="obsid",
            keys_right="obs_id",
            join_type="left",
        )

        return joined_table


def transform_to_observation(
    tap_obs: Row, instrument: sdk.IDNameSchema
) -> sdk.ObservationCreate:
    begin = tap_obs["start_date"]
    end = (
        Time(begin, format="isot") + timedelta(seconds=tap_obs["t_plan_exptime"])
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
        external_observation_id=tap_obs["obs_id"],
        type=CHANDRA_OBSERVATION_TYPES[instrument.short_name or ""],
        status=sdk.ObservationStatus.SCHEDULED,
        pointing_angle=0.0,
        exposure_time=float(tap_obs["t_plan_exptime"]),
        bandpass=CHANDRA_BANDPASSES[instrument.short_name or ""],
    )


async def ingest() -> None:
    """
    Ingests all scheduled Chandra observations by submitting a TAP query using
    the Chandra VO service.

    Performs queries of two different tables to retrieve all required parameters.
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

    schedule = create_schedule(telescope.id, tap_observation_table)

    instruments_by_short_name = {
        instrument.short_name: instrument
        for instrument in telescope.instruments
        if instrument.short_name
    }

    for observation_data in tap_observation_table:
        instrument = match_instrument_from_tap_observation(
            instruments_by_short_name, observation_data
        )
        observation = transform_to_observation(observation_data, instrument)
        schedule.observations.append(observation)

    try:
        sdk.ScheduleApi(client).create_schedule(schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.info("Schedule already exists.", schedule_name=schedule.name)


@repeat_every(seconds=SECONDS_IN_A_DAY)  # Daily
async def entrypoint() -> None:
    try:
        await ingest()
        logger.info("Chandra high-fidelity planned schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(
            "Chandra high-fidelity planned schedule ingestion encountered an error",
            err=e,
            exc_info=True,
        )
        return
