from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Literal

import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from fastapi_utils.tasks import repeat_every
from swifttools import swift_too  # type: ignore
from swifttools.swift_too.swift_planquery import PPSTEntry  # type: ignore
from swifttools.swift_too.swift_uvot import UVOTModeEntry  # type: ignore

from ....core.constants import SECONDS_IN_A_DAY
from ....util import across_api
from ..types import AcrossObservation, AcrossSchedule

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

SWIFT_XRT_BANDPASS = {
    "filter_name": "Swift XRT",
    "min": 0.3,
    "max": 10.0,
    "unit": "keV",
}
SWIFT_BAT_BANDPASS = {
    "filter_name": "Swift BAT",
    "min": 15.0,
    "max": 150.0,
    "unit": "keV",
}

SWIFT_UVOT_BANDPASS_DICT = {
    "u": {"filter_name": "Swift UVOT u", "min": 308, "max": 385, "unit": "nm"},
    "b": {"filter_name": "Swift UVOT b", "min": 391, "max": 487, "unit": "nm"},
    "v": {"filter_name": "Swift UVOT v", "min": 509, "max": 585, "unit": "nm"},
    "uvw1": {"filter_name": "Swift UVOT uvw1", "min": 226, "max": 294, "unit": "nm"},
    "uvw2": {"filter_name": "Swift UVOT uvw2", "min": 160, "max": 225, "unit": "nm"},
    "uvm2": {"filter_name": "Swift UVOT uvm2", "min": 200, "max": 249, "unit": "nm"},
    "white": {"filter_name": "Swift UVOT white", "min": 160, "max": 800, "unit": "nm"},
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

    if len(non_saa_query):
        return non_saa_query

    return None


def swift_uvot_mode_dict(modes: list[str]) -> dict[str, list[CustomUVOTModeEntry]]:
    """
    Creates a dictionary of UVOT modes from a list of mode names.
    This is used to avoid multiple HTTP requests to the Swift TOO catalog.
    """
    uvot_mode_dict = {}
    for mode in modes:
        entries = swift_too.UVOTMode(mode).entries

        if not entries:
            continue

        uvot_mode_dict[mode] = [
            CustomUVOTModeEntry.from_entry(mode_entry) for mode_entry in entries
        ]

    return uvot_mode_dict


def swift_to_across_schedule(
    telescope_id: str, telescope_short_name: str, data: list[CustomSwiftObsEntry]
) -> AcrossSchedule | dict:
    begins = [obs.begin for obs in data]
    ends = [obs.end for obs in data]

    begin = Time(min(begins)).isot
    end = Time(max(ends)).isot

    return {
        "telescope_id": telescope_id,
        "name": f"{telescope_short_name}_low_fidelity_planned_{begin.split('T')[0]}_{end.split('T')[0]}",
        "date_range": {
            "begin": begin,
            "end": end,
        },
        "status": "planned",
        "fidelity": "low",
    }


def swift_to_across_observation(
    instrument_id: str,
    observation: CustomSwiftObsEntry,
    bandpass: dict,
    observation_type: Literal["imaging", "spectroscopy", "timing"],
    exposure_time: float,
) -> AcrossObservation:
    return {
        "instrument_id": instrument_id,
        "object_name": observation.targname,
        "pointing_position": {
            "ra": observation.ra,
            "dec": observation.dec,
        },
        "object_position": {
            "ra": observation.ra,
            "dec": observation.dec,
        },
        "date_range": {
            "begin": Time(observation.begin).isot,
            "end": Time(observation.end).isot,
        },
        "external_observation_id": observation.obsid,
        "type": observation_type,
        "status": "planned",
        "pointing_angle": observation.roll,
        "exposure_time": exposure_time,
        "bandpass": bandpass,
    }


def create_observations(
    instrument_id: str,
    observation_data: list[CustomSwiftObsEntry],
    bandpass: dict,
    observation_type: Literal["imaging", "spectroscopy", "timing"],
) -> list[AcrossObservation]:
    return [
        swift_to_across_observation(
            instrument_id, obs, bandpass, observation_type, obs.exposure
        )
        for obs in observation_data
    ]


def create_uvot_observations(
    instrument_id: str,
    observation_data: list[CustomSwiftObsEntry],
    *kwargs: dict[str, Any],
):
    # Aggregate unique uvot modes
    uvot_modes = list(set([obs.uvot for obs in observation_data]))

    # This triggers an HTTP request via swifttools to get the UVOT modes
    # doing it here over unique list to avoid multiple requests
    uvot_mode_dict = swift_uvot_mode_dict(uvot_modes)

    uvot_schedule_observations = []

    observations = [obs for obs in observation_data if obs.uvot in uvot_mode_dict]

    for obs_data in observations:
        swift_uvot_observations = uvot_mode_dict[obs_data.uvot]

        # Calculate the total weight of all UVOT observations to normalize exposure times
        observation_total_weight = sum(
            uvot_obs.weight for uvot_obs in swift_uvot_observations if uvot_obs.weight
        )

        unknown_filter_observations = [
            uvot_obs
            for uvot_obs in swift_uvot_observations
            if uvot_obs.filter_name not in SWIFT_UVOT_BANDPASS_DICT
        ]
        known_filter_observations = [
            uvot_obs
            for uvot_obs in swift_uvot_observations
            if uvot_obs.filter_name in SWIFT_UVOT_BANDPASS_DICT
        ]

        for uvot_obs in unknown_filter_observations:
            logger.warn(
                "Skipping observation, filter not found.",
                uvot=obs_data.uvot,
                filter=uvot_obs.filter_name,
            )

        for uvot_obs in known_filter_observations:
            # Calculate exposure time factor based on the weight of the observation
            exposure_time_factor = uvot_obs.weight / observation_total_weight

            uvot_schedule_observations.append(
                swift_to_across_observation(
                    instrument_id=instrument_id,
                    observation=obs_data,
                    bandpass=SWIFT_UVOT_BANDPASS_DICT[uvot_obs.filter_name],
                    observation_type="imaging",
                    exposure_time=obs_data.exposure * exposure_time_factor,
                )
            )

    return uvot_schedule_observations


def create_swift_across_schedule(
    telescope_name: str,
    observation_data: list[CustomSwiftObsEntry],
    observation_type: Literal["imaging", "spectroscopy", "timing"],
    create_observations: Callable = create_observations,
    bandpass: dict = {},
) -> AcrossSchedule | dict:
    telescope_info = across_api.telescope.get({"name": telescope_name})[0]
    telescope_id = telescope_info["id"]
    instrument_id = telescope_info["instruments"][0]["id"]

    schedule = swift_to_across_schedule(
        telescope_id=telescope_id,
        telescope_short_name=telescope_name,
        data=observation_data,
    )

    schedule["observations"] = create_observations(
        instrument_id,
        observation_data,
        bandpass,
        observation_type,
    )

    return schedule


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
    swift_xrt_schedule = create_swift_across_schedule(
        telescope_name="swift_xrt",
        observation_data=swift_observation_data,
        bandpass=SWIFT_XRT_BANDPASS,
        observation_type="spectroscopy",
        create_observations=create_observations,
    )

    # BAT
    swift_bat_schedule = create_swift_across_schedule(
        telescope_name="swift_bat",
        observation_data=swift_observation_data,
        bandpass=SWIFT_BAT_BANDPASS,
        observation_type="imaging",
        create_observations=create_observations,
    )

    # UVOT
    swift_uvot_schedule = create_swift_across_schedule(
        telescope_name="swift_uvot",
        observation_data=swift_observation_data,
        observation_type="imaging",
        create_observations=create_uvot_observations,
    )

    # Post the schedules to the ACROSS API
    across_api.schedule.post(swift_xrt_schedule)
    across_api.schedule.post(swift_bat_schedule)
    across_api.schedule.post(swift_uvot_schedule)

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
