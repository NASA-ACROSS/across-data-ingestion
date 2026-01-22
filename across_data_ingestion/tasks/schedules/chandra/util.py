from datetime import datetime

import structlog
from astropy.table import Row, Table  # type: ignore[import-untyped]

from ....util.across_server import sdk

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
    instruments_by_short_name: dict[str, sdk.TelescopeInstrument], tap_obs: Row
) -> sdk.TelescopeInstrument:
    """
    Constructs the instrument name from the observation parameters and
    returns both the name and the instrument id in across-server
    """

    short_name = None
    if "ACIS" in tap_obs["instrument"]:
        if tap_obs["grating"] == "NONE" and tap_obs["exposure_mode"] != "CC":
            short_name = "ACIS"
        elif tap_obs["grating"] in ["HETG", "LETG"]:
            short_name = f"ACIS-{tap_obs['grating']}"
        elif tap_obs["exposure_mode"] == "CC":
            short_name = "ACIS-CC"

    elif "HRC" in tap_obs["instrument"]:
        if tap_obs["exposure_mode"] != "":
            short_name = "HRC-Timing"
        elif tap_obs["grating"] == "NONE":
            short_name = "HRC"
        elif tap_obs["grating"] in ["HETG", "LETG"]:
            short_name = f"HRC-{tap_obs['grating']}"

    if not short_name:
        logger.warning(
            "Could not parse observation parameters for correct instrument",
            tap_observation=tap_obs,
        )
        return sdk.TelescopeInstrument(
            id="", name="", short_name="", created_on=datetime.now()
        )

    return instruments_by_short_name[short_name]


def create_schedule(
    telescope_id: str,
    tap_observations: Table,
    schedule_type: str,
    schedule_status: sdk.ScheduleStatus,
    schedule_fidelity: sdk.ScheduleFidelity,
) -> sdk.ScheduleCreate:
    begin = f"{min([data['start_date'] for data in tap_observations])}"
    end = f"{max([data['start_date'] for data in tap_observations])}"

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"chandra_{schedule_type}_{begin.split('T')[0]}_{end.split('T')[0]}",
        date_range=sdk.DateRange(
            begin=datetime.fromisoformat(begin), end=datetime.fromisoformat(end)
        ),
        status=schedule_status,
        fidelity=schedule_fidelity,
        observations=[],
    )
