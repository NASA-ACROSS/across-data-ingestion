from datetime import datetime, timedelta, timezone
from typing import Literal

import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every
from swifttools import swift_too  # type: ignore[import-untyped]
from swifttools.swift_too.swift_planquery import (  # type: ignore[import-untyped]
    PPSTEntry,
)
from swifttools.swift_too.swift_uvot import (  # type: ignore[import-untyped]
    UVOTModeEntry,
)

from ....core.constants import SECONDS_IN_A_DAY
from ....util import across_api
from ..types import AcrossObservation, AcrossSchedule

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

SWIFT_XRT_BANDPASS = {
    "name": "Swift XRT",
    "min": 0.3,
    "max": 10.0,
    "unit": "keV",
}
SWIFT_BAT_BANDPASS = {
    "name": "Swift BAT",
    "min": 15.0,
    "max": 150.0,
    "unit": "keV",
}

SWIFT_UVOT_BANDPASS_DICT = {
    "u": {"name": "Swift UVOT u", "min": 308, "max": 385, "unit": "nm"},
    "b": {"name": "Swift UVOT b", "min": 391, "max": 487, "unit": "nm"},
    "v": {"name": "Swift UVOT v", "min": 509, "max": 585, "unit": "nm"},
    "uvw1": {"name": "Swift UVOT uvw1", "min": 226, "max": 294, "unit": "nm"},
    "uvw2": {"name": "Swift UVOT uvw2", "min": 160, "max": 225, "unit": "nm"},
    "uvm2": {"name": "Swift UVOT uvm2", "min": 200, "max": 249, "unit": "nm"},
    "white": {"name": "Swift UVOT white", "min": 160, "max": 800, "unit": "nm"},
}


class CustomUVOTModeEntry:
    """
    Custom UVOTModeEntry to handle the UVOT mode as a string instead of a UVOTMode object.
    This is necessary to avoid multiple HTTP requests to the Swift TOO catalog.
    """

    filter_name: str
    weight: float

    def __init__(self, **kwargs):
        """
        Initializes a CustomUVOTModeEntry from keyword arguments.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __eq__(self, value):
        assert isinstance(
            value, CustomUVOTModeEntry
        ), "Can only compare with another CustomUVOTModeEntry"
        return self.filter_name == value.filter_name and self.weight == value.weight

    @classmethod
    def from_entry(cls, entry: UVOTModeEntry) -> "CustomUVOTModeEntry":
        """
        Converts a UVOTModeEntry to a CustomUVOTModeEntry.
        """
        return cls(filter_name=entry.filter_name, weight=entry.weight)


class CustomSwiftObsEntry:
    """
    Custom Swift_PPST_Entry to handle the UVOT mode as a string instead of a UVOTMode object.
    This is necessary to avoid multiple HTTP requests to the Swift TOO catalog.
    """

    obsid: str
    targname: str
    ra: str
    dec: str
    begin: str
    end: str
    exposure: float
    roll: float
    uvot: str
    bat: str
    xrt: str
    fom: float
    segment: int
    target_id: str

    def __init__(self, **kwargs):
        """
        Initializes a CustomSwiftEntry from keyword arguments.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def from_entry(cls, entry: PPSTEntry) -> "CustomSwiftObsEntry":
        """
        Converts a PPSTEntry to a CustomSwiftEntry.
        """
        return cls(
            obsid=entry.obsid,
            targname=entry.targname,
            ra=entry.ra,
            dec=entry.dec,
            begin=Time(entry.begin).isot,
            end=Time(entry.end).isot,
            exposure=entry.exposure.seconds,
            roll=entry.roll,
            uvot=entry.uvot,
            bat=entry.bat,
            xrt=entry.xrt,
            fom=entry.fom,
            segment=entry.segment,
            target_id=entry.target_id,
        )


def query_swift_plan(days_in_future: int = 4) -> list[CustomSwiftObsEntry] | None:
    """
    Queries the Swift catalog for all Swift planned observations from now until 4 days from now.
    """
    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(days=days_in_future)
    try:
        query = swift_too.PlanQuery(begin=start_time, end=end_time)
    except Exception:
        return None

    non_saa_query = [
        CustomSwiftObsEntry.from_entry(observation)
        for observation in query
        if observation.uvot not in ["0x0009"]
    ]

    return non_saa_query


def swift_uvot_mode_dict(modes: list[str]) -> dict[str, list[CustomUVOTModeEntry]]:
    """
    Creates a dictionary of UVOT modes from a list of mode names.
    This is used to avoid multiple HTTP requests to the Swift TOO catalog.
    """
    # import json
    uvot_mode_dict = {}
    for mode in modes:
        entries = swift_too.UVOTMode(mode).entries

        if not entries:
            continue

        uvot_mode_dict[mode] = [
            CustomUVOTModeEntry.from_entry(mode_entry) for mode_entry in entries
        ]

    test = {}
    for mode, entries in uvot_mode_dict.items():
        test[mode] = [entry.__dict__ for entry in entries]

    return uvot_mode_dict


def swift_schedule(
    telescope_id: str, telescope_short_name: str, data: list[CustomSwiftObsEntry]
) -> AcrossSchedule | dict:
    if len(data) == 0:
        # Empty schedule, return
        return {}

    begins = [obs.begin for obs in data]
    ends = [obs.end for obs in data]

    begin = Time(min(begins)).isot
    end = Time(max(ends)).isot

    return {
        "telescope_id": telescope_id,
        "name": f"swift_{telescope_short_name}_low_fidelity_planned_{begin.split('T')[0]}_{end.split('T')[0]}",
        "date_range": {
            "begin": begin,
            "end": end,
        },
        "status": "planned",
        "fidelity": "low",
    }


def swift_observation(
    instrument_id: str,
    row: CustomSwiftObsEntry,
    bandpass: dict,
    observation_type: Literal["imaging", "spectroscopy", "timing"],
    exposure_time: float,
) -> AcrossObservation:
    return {
        "instrument_id": instrument_id,
        "object_name": row.targname,
        "pointing_position": {
            "ra": row.ra,
            "dec": row.dec,
        },
        "object_position": {
            "ra": row.ra,
            "dec": row.dec,
        },
        "date_range": {
            "begin": Time(row.begin).isot,
            "end": Time(row.end).isot,
        },
        "external_observation_id": row.obsid,
        "type": observation_type,
        "status": "planned",
        "pointing_angle": row.roll,
        "exposure_time": exposure_time,
        "bandpass": bandpass,
    }


def ingest(days_in_future: int = 4) -> list[AcrossSchedule | dict]:
    """
    Method that POSTs Swift low fidelity planned observing schedules to the ACROSS server
    For the Swift Observatory, this includes the XRT, BAT, and UVOT Telescopes.
    It interprets a single planned observation as an observation for each instrument since it
     takes data in parallel

    Queries planned observations via the swifttools Swift TOO catalog
    This is a low fidelity schedule, meaning it is not guaranteed to be accurate or complete.
    """

    # Get the swift telescope ids along with their instrument ids
    swift_observation_data = query_swift_plan(days_in_future)
    if swift_observation_data is None:
        logger.warn("Failed to query Swift planned observations.")
        return [{}]

    # XRT
    # Get the swift xrt telescope ids along with their instrument ids
    swift_xrt_telescope_info = across_api.telescope.get({"name": "swift_xrt"})[0]
    xrt_telescope_id = swift_xrt_telescope_info["id"]
    xrt_instrument_id = swift_xrt_telescope_info["instruments"][0]["id"]

    # Create the schedule for Swift XRT
    swift_xrt_schedule = swift_schedule(
        telescope_id=xrt_telescope_id,
        telescope_short_name="xrt",
        data=swift_observation_data,
    )

    # Create observations for Swift XRT
    swift_xrt_schedule["observations"] = [
        swift_observation(
            instrument_id=xrt_instrument_id,
            row=obs,
            bandpass=SWIFT_XRT_BANDPASS,
            observation_type="spectroscopy",
            exposure_time=obs.exposure,
        )
        for obs in swift_observation_data
    ]

    # BAT
    # Get the swift bat telescope ids along with their instrument ids
    swift_bat_telescope_info = across_api.telescope.get({"name": "swift_bat"})[0]
    bat_telescope_id = swift_bat_telescope_info["id"]
    bat_instrument_id = swift_bat_telescope_info["instruments"][0]["id"]

    # Create the schedule for Swift BAT
    swift_bat_schedule = swift_schedule(
        telescope_id=bat_telescope_id,
        telescope_short_name="bat",
        data=swift_observation_data,
    )
    # Create observations for Swift BAT
    swift_bat_schedule["observations"] = [
        swift_observation(
            instrument_id=bat_instrument_id,
            row=obs,
            bandpass=SWIFT_BAT_BANDPASS,
            observation_type="spectroscopy",
            exposure_time=obs.exposure,
        )
        for obs in swift_observation_data
    ]

    # UVOT
    # Get the swift uvot telescope ids along with their instrument ids
    swift_uvot_telescope_info = across_api.telescope.get({"name": "swift_uvot"})[0]
    uvot_telescope_id = swift_uvot_telescope_info["id"]
    uvot_instrument_id = swift_uvot_telescope_info["instruments"][0]["id"]

    # Aggregate unique uvot modes
    uvot_modes = list(set([obs.uvot for obs in swift_observation_data]))

    # This triggers an HTTP request via swifttools to get the UVOT modes
    # doing it here over unique list to avoid multiple requests
    uvot_mode_dict = swift_uvot_mode_dict(uvot_modes)

    # Create the schedule for Swift UVOT
    swift_uvot_schedule = swift_schedule(
        telescope_id=uvot_telescope_id,
        telescope_short_name="uvot",
        data=swift_observation_data,
    )

    uvot_schedule_observations = []
    for obs in swift_observation_data:
        # Get the UVOT observations from the uvot mode
        if obs.uvot not in uvot_mode_dict:
            logger.warn(
                f"UVOT: {obs.uvot} not found in uvot_mode_dict, skipping observations for this mode."
            )
            continue

        swift_uvot_observations = uvot_mode_dict[obs.uvot]

        # Calculate the total weight of all UVOT observations to normalize exposure times
        observation_total_weight = sum(
            entry.weight for entry in swift_uvot_observations if entry.weight
        )

        for uvot_observation in swift_uvot_observations:
            # If the filter name is not in the SWIFT_UVOT_BANDPASS_DICT, skip the observation
            if uvot_observation.filter_name not in SWIFT_UVOT_BANDPASS_DICT:
                logger.warn(
                    f"UVOT: {obs.uvot} filter {uvot_observation.filter_name} not found in SWIFT_UVOT_BANDPASS_DICT, skipping."
                )
            else:
                # Calculate exposure time factor based on the weight of the observation
                exposure_time_factor = (
                    uvot_observation.weight / observation_total_weight
                )

                uvot_schedule_observations.append(
                    swift_observation(
                        instrument_id=uvot_instrument_id,
                        row=obs,
                        bandpass=SWIFT_UVOT_BANDPASS_DICT[uvot_observation.filter_name],
                        observation_type="imaging",
                        exposure_time=obs.exposure * exposure_time_factor,
                    )
                )

    swift_uvot_schedule["observations"] = uvot_schedule_observations

    # Post the schedules to the ACROSS API
    across_api.schedule.post(dict(swift_xrt_schedule))
    across_api.schedule.post(dict(swift_bat_schedule))
    across_api.schedule.post(dict(swift_uvot_schedule))

    return [swift_xrt_schedule, swift_bat_schedule, swift_uvot_schedule]


@repeat_every(seconds=1 * SECONDS_IN_A_DAY)  # daily
def entrypoint() -> None:
    try:
        ingest()
        logger.info("Swift low fidelity planned schedule ingestion completed.")
        return
    except Exception as e:
        # Surface the error through logging, if we do not catch everything and log, the errors get voided
        logger.error(
            "Swift low fidelity planned schedule ingestion encountered an error", err=e
        )
        return
