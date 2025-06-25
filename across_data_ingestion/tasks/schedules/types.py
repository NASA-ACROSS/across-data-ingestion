from typing import Literal, TypedDict


class DateRange(TypedDict):
    begin: str
    end: str


class Position(TypedDict):
    ra: str
    dec: str


class AcrossSchedule(TypedDict):
    telescope_id: str
    name: str
    date_range: DateRange
    status: Literal["planned", "scheduled", "performed"]
    fidelity: Literal["high", "low"]
    observations: list


class AcrossObservation(TypedDict):
    instrument_id: str
    object_name: str
    pointing_position: Position
    object_position: Position | None
    date_range: DateRange
    external_observation_id: str
    type: Literal["imaging", "spectroscopy", "timing"]
    status: Literal["planned", "scheduled", "unscheduled", "performed", "aborted"]
    pointing_angle: float
    exposure_time: float
    bandpass: dict
