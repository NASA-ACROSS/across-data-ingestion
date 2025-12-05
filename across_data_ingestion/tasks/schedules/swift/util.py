from typing import Any, Callable

import structlog
from astropy.time import Time  # type: ignore[import-untyped]
from swifttools import swift_too  # type: ignore
from swifttools.swift_too.swift_obsquery import Swift_AFST_Entry  # type: ignore
from swifttools.swift_too.swift_planquery import PPSTEntry  # type: ignore
from swifttools.swift_too.swift_uvot import UVOTModeEntry  # type: ignore

from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

SWIFT_XRT_BANDPASS = sdk.EnergyBandpass(
    filter_name="Swift XRT",
    min=0.3,
    max=10.0,
    unit=sdk.EnergyUnit.KEV,
)

SWIFT_BAT_BANDPASS = sdk.EnergyBandpass(
    filter_name="Swift BAT",
    min=15.0,
    max=150.0,
    unit=sdk.EnergyUnit.KEV,
)

SWIFT_UVOT_BANDPASS_DICT = {
    "u": sdk.WavelengthBandpass(
        filter_name="Swift UVOT u",
        min=308,
        max=385,
        unit=sdk.WavelengthUnit.NM,
    ),
    "b": sdk.WavelengthBandpass(
        filter_name="Swift UVOT b",
        min=391,
        max=487,
        unit=sdk.WavelengthUnit.NM,
    ),
    "v": sdk.WavelengthBandpass(
        filter_name="Swift UVOT v",
        min=509,
        max=585,
        unit=sdk.WavelengthUnit.NM,
    ),
    "uvw1": sdk.WavelengthBandpass(
        filter_name="Swift UVOT uvw1",
        min=226,
        max=294,
        unit=sdk.WavelengthUnit.NM,
    ),
    "uvw2": sdk.WavelengthBandpass(
        filter_name="Swift UVOT uvw2",
        min=160,
        max=225,
        unit=sdk.WavelengthUnit.NM,
    ),
    "uvm2": sdk.WavelengthBandpass(
        filter_name="Swift UVOT uvm2",
        min=200,
        max=249,
        unit=sdk.WavelengthUnit.NM,
    ),
    "white": sdk.WavelengthBandpass(
        filter_name="Swift UVOT white",
        min=160,
        max=800,
        unit=sdk.WavelengthUnit.NM,
    ),
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
        return cls(filter_name=str.lower(entry.filter_name), weight=entry.weight)


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
    def from_entry(cls, entry: Swift_AFST_Entry | PPSTEntry) -> "CustomSwiftObsEntry":
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
    data: list[CustomSwiftObsEntry],
    status: sdk.ScheduleStatus,
    fidelity: sdk.ScheduleFidelity,
    schedule_name_attr: str,
) -> sdk.ScheduleCreate:
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
    swift_obs: CustomSwiftObsEntry,
    bandpass: sdk.Bandpass,
    observation_type: sdk.ObservationType,
    exposure_time: float,
    observation_status: sdk.ObservationStatus,
) -> sdk.ObservationCreate:
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
    observation_data: list[CustomSwiftObsEntry],
    observation_status: sdk.ObservationStatus,
    bandpass: sdk.Bandpass,
    observation_type: sdk.ObservationType,
) -> list[sdk.ObservationCreate]:
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
    observation_data: list[CustomSwiftObsEntry],
    observation_status: sdk.ObservationStatus,
    *args: list[Any],
):
    # Aggregate unique uvot modes
    uvot_modes = list(set([obs.uvot for obs in observation_data]))

    # This triggers an HTTP request via swifttools to get the UVOT modes
    # doing it here over unique list to avoid multiple requests
    uvot_mode_dict = build_uvot_mode_dict(uvot_modes)

    uvot_schedule_observations = []

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

            uvot_schedule_observations.append(
                swift_to_across_observation(
                    instrument_id=instrument_id,
                    swift_obs=obs_data,
                    bandpass=sdk.Bandpass(SWIFT_UVOT_BANDPASS_DICT[mode.filter_name]),
                    observation_type=sdk.ObservationType.IMAGING,
                    exposure_time=obs_data.exposure * exposure_time_factor,
                    observation_status=observation_status,
                )
            )

    return uvot_schedule_observations


def create_swift_across_schedule(
    telescope_name: str,
    observation_data: list[CustomSwiftObsEntry],
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


class SwiftScheduleHandler:
    def __init__(
        self,
        observation_status: sdk.ObservationStatus,
        schedule_status: sdk.ScheduleStatus,
        schedule_fidelity: sdk.ScheduleFidelity,
        schedule_name_attr: str,
    ):
        self.observation_status = observation_status
        self.schedule_status = schedule_status
        self.schedule_fidelity = schedule_fidelity
        self.schedule_name_attr = schedule_name_attr

    def run(self, observation_data: list[CustomSwiftObsEntry]):
        # XRT
        swift_xrt_schedule = create_swift_across_schedule(
            telescope_name="swift_xrt",
            observation_data=observation_data,
            observation_status=self.observation_status,
            bandpass=sdk.Bandpass(SWIFT_XRT_BANDPASS),
            observation_type=sdk.ObservationType.SPECTROSCOPY,
            schedule_status=self.schedule_status,
            schedule_fidelity=self.schedule_fidelity,
            schedule_name_attr=self.schedule_name_attr,
            create_observations=create_observations,
        )

        # BAT
        swift_bat_schedule = create_swift_across_schedule(
            telescope_name="swift_bat",
            observation_data=observation_data,
            observation_status=self.observation_status,
            bandpass=sdk.Bandpass(SWIFT_BAT_BANDPASS),
            observation_type=sdk.ObservationType.IMAGING,
            schedule_status=self.schedule_status,
            schedule_fidelity=self.schedule_fidelity,
            schedule_name_attr=self.schedule_name_attr,
            create_observations=create_observations,
        )

        # UVOT
        swift_uvot_schedule = create_swift_across_schedule(
            telescope_name="swift_uvot",
            observation_data=observation_data,
            observation_status=self.observation_status,
            observation_type=sdk.ObservationType.IMAGING,
            schedule_status=self.schedule_status,
            schedule_fidelity=self.schedule_fidelity,
            schedule_name_attr=self.schedule_name_attr,
            create_observations=create_uvot_observations,
        )

        # Post the schedules to the ACROSS API
        sdk.ScheduleApi(client).create_schedule(swift_xrt_schedule)
        sdk.ScheduleApi(client).create_schedule(swift_bat_schedule)
        sdk.ScheduleApi(client).create_schedule(swift_uvot_schedule)
