from collections.abc import Callable
from typing import Any

import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from swifttools import swift_too  # type: ignore

from ....util.across_server import client, sdk
from .constants import SWIFT_BAT_BANDPASS, SWIFT_UVOT_BANDPASS_DICT, SWIFT_XRT_BANDPASS
from .custom_uvot_mode_entry import CustomUVOTModeEntry
from .swift_observation_entry import SwiftObservationEntry

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


def observation_in_saa(obs: SwiftObservationEntry) -> bool:
    """
    Determines if a Swift observation occurred during South Atlantic Anomaly (SAA) passage.
    An observation is considered to be in the SAA if its target ID falls within known SAA target ID ranges
    and its target name contains the substring "saa-cold".
    """
    target_id = int(obs.target_id)
    in_saa_target_id_range = (target_id >= 70000 and target_id < 80000) or (
        target_id >= 3600000 and target_id <= 3699999
    )
    return in_saa_target_id_range and "saa-cold" in str.lower(obs.targname)


def build_uvot_mode_dict(modes: list[str]) -> dict[str, list[CustomUVOTModeEntry]]:
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
    telescope_id: str,
    telescope_short_name: str,
    data: list[SwiftObservationEntry],
    status: sdk.ScheduleStatus,
    fidelity: sdk.ScheduleFidelity,
    schedule_name_attr: str,
) -> sdk.ScheduleCreate:
    """Converts a list of SwiftObservationEntry to an ACROSS ScheduleCreate object."""
    begins = [obs.begin for obs in data]
    ends = [obs.end for obs in data]

    begin = Time(min(begins)).isot
    end = Time(max(ends)).isot

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"{telescope_short_name}_{schedule_name_attr}_{begin.split('T')[0]}_{end.split('T')[0]}",
        date_range=sdk.DateRange(
            begin=begin,
            end=end,
        ),
        status=status,
        fidelity=fidelity,
        observations=[],
    )


def swift_to_across_observation(
    instrument_id: str,
    swift_obs: SwiftObservationEntry,
    bandpass: sdk.Bandpass,
    observation_type: sdk.ObservationType,
    exposure_time: float,
    observation_status: sdk.ObservationStatus,
) -> sdk.ObservationCreate:
    """Converts a SwiftObservationEntry to an ACROSS ObservationCreate object."""
    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=swift_obs.targname,
        pointing_position=sdk.Coordinate(
            ra=float(swift_obs.ra),
            dec=float(swift_obs.dec),
        ),
        object_position=sdk.Coordinate(
            ra=float(swift_obs.ra),
            dec=float(swift_obs.dec),
        ),
        date_range=sdk.DateRange(
            begin=Time(swift_obs.begin).isot,
            end=Time(swift_obs.end).isot,
        ),
        external_observation_id=swift_obs.obsid,
        type=observation_type,
        status=observation_status,
        pointing_angle=swift_obs.roll,
        exposure_time=exposure_time,
        bandpass=bandpass,
    )


def create_observations(
    instrument_id: str,
    observation_data: list[SwiftObservationEntry],
    observation_status: sdk.ObservationStatus,
    bandpass: sdk.Bandpass,
    observation_type: sdk.ObservationType,
) -> list[sdk.ObservationCreate]:
    """Creates a list of ACROSS ObservationCreate objects from SwiftObservationEntry data."""
    return [
        swift_to_across_observation(
            instrument_id,
            obs,
            bandpass,
            observation_type,
            obs.exposure,
            observation_status,
        )
        for obs in observation_data
    ]


def create_uvot_observations(
    instrument_id: str,
    observation_data: list[SwiftObservationEntry],
    observation_status: sdk.ObservationStatus,
    *args: list[Any],
) -> list[sdk.ObservationCreate]:
    """Creates a list of ACROSS ObservationCreate objects specifically for UVOT observations."""
    # Aggregate unique uvot modes
    uvot_modes = list(set([obs.uvot for obs in observation_data]))

    # This triggers an HTTP request via swifttools to get the UVOT modes
    # doing it here over unique list to avoid multiple requests
    uvot_mode_dict = build_uvot_mode_dict(uvot_modes)

    across_observations = []

    # filter observations to match UVOT modes
    observations = [obs for obs in observation_data if obs.uvot in uvot_mode_dict]

    for obs_data in observations:
        uvot_obs_modes = uvot_mode_dict[obs_data.uvot]

        unknown_filter_observations = [
            mode
            for mode in uvot_obs_modes
            if mode.filter_name not in SWIFT_UVOT_BANDPASS_DICT
        ]

        if unknown_filter_observations:
            for mode in unknown_filter_observations:
                logger.warning(
                    "No observation will be created for the unmatched filter.",
                    uvot=obs_data.uvot,
                    filter=mode.filter_name,
                )

        # Calculate the total weight of all UVOT observations to normalize exposure times
        observation_total_weight = sum(
            mode.weight for mode in uvot_obs_modes if mode.weight
        )

        known_filter_observations = [
            mode
            for mode in uvot_obs_modes
            if mode.filter_name in SWIFT_UVOT_BANDPASS_DICT
        ]

        for mode in known_filter_observations:
            # Calculate exposure time factor based on the weight of the observation
            exposure_time_factor = mode.weight / observation_total_weight

            across_observations.append(
                swift_to_across_observation(
                    instrument_id=instrument_id,
                    swift_obs=obs_data,
                    bandpass=sdk.Bandpass(SWIFT_UVOT_BANDPASS_DICT[mode.filter_name]),
                    observation_type=sdk.ObservationType.IMAGING,
                    exposure_time=obs_data.exposure * exposure_time_factor,
                    observation_status=observation_status,
                )
            )

    return across_observations


def create_swift_across_schedule(
    telescope_name: str,
    observation_data: list[SwiftObservationEntry],
    observation_status: sdk.ObservationStatus,
    observation_type: sdk.ObservationType,
    schedule_status: sdk.ScheduleStatus,
    schedule_fidelity: sdk.ScheduleFidelity,
    schedule_name_attr: str,
    create_observations: Callable = create_observations,
    bandpass: sdk.Bandpass | None = None,
) -> sdk.ScheduleCreate:
    telescope = sdk.TelescopeApi(client).get_telescopes(name=telescope_name)[0]
    telescope_id = telescope.id
    if telescope.instruments:
        instrument_id = telescope.instruments[0].id

    schedule = swift_to_across_schedule(
        telescope_id=telescope_id,
        telescope_short_name=telescope_name,
        data=observation_data,
        status=schedule_status,
        fidelity=schedule_fidelity,
        schedule_name_attr=schedule_name_attr,
    )

    schedule.observations = create_observations(
        instrument_id, observation_data, observation_status, bandpass, observation_type
    )

    return schedule


class InstrumentConfig:
    """
    Configuration for a Swift instrument used in schedule creation.
    """

    def __init__(
        self,
        telescope_name: str,
        observation_type: sdk.ObservationType,
        bandpass: sdk.Bandpass | None = None,
    ):
        self.telescope_name = telescope_name
        self.observation_type = observation_type
        self.bandpass = bandpass


class SwiftScheduleHandler:
    """Handles the creation and posting of Swift schedules to the ACROSS API."""

    def __init__(
        self,
        observation_status: sdk.ObservationStatus,
        schedule_status: sdk.ScheduleStatus,
        schedule_fidelity: sdk.ScheduleFidelity,
        schedule_name: str,
    ):
        self.observation_status = observation_status
        self.schedule_status = schedule_status
        self.schedule_fidelity = schedule_fidelity
        self.schedule_name = schedule_name
        self.instrumnent_configs = [
            InstrumentConfig(
                telescope_name="swift_xrt",
                bandpass=sdk.Bandpass(SWIFT_XRT_BANDPASS),
                observation_type=sdk.ObservationType.SPECTROSCOPY,
            ),
            InstrumentConfig(
                telescope_name="swift_bat",
                bandpass=sdk.Bandpass(SWIFT_BAT_BANDPASS),
                observation_type=sdk.ObservationType.IMAGING,
            ),
            InstrumentConfig(
                telescope_name="swift_uvot",
                observation_type=sdk.ObservationType.IMAGING,
            ),
        ]

    def run(self, observation_data: list[SwiftObservationEntry]):
        # Create and post schedules for each instrument
        for config in self.instrumnent_configs:
            schedule = create_swift_across_schedule(
                telescope_name=config.telescope_name,
                observation_data=observation_data,
                observation_status=self.observation_status,
                bandpass=config.bandpass,
                observation_type=config.observation_type,
                schedule_status=self.schedule_status,
                schedule_fidelity=self.schedule_fidelity,
                schedule_name_attr=self.schedule_name,
                create_observations=(
                    create_uvot_observations
                    if config.telescope_name == "swift_uvot"
                    else create_observations
                ),
            )

            # Post the schedules to the ACROSS API
            sdk.ScheduleApi(client).create_schedule(schedule)
