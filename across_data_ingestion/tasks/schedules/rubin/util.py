from datetime import datetime

import pandas as pd
from astropy.time import Time  # type: ignore[import-untyped]

from ....util.across_server import client, sdk
from .constants import RUBIN_BANDPASSES


def designate_filter_name_key(row: pd.Series) -> str:
    """Find filtername from em_min and em_max columns."""

    em_min_angstrom = row["em_min"] * 10e9
    em_max_angstrom = row["em_max"] * 10e9

    midpoint = (em_min_angstrom + em_max_angstrom) / 2

    if midpoint < 3955.5 and midpoint >= 3492.5:
        return "rubin_u"
    elif midpoint < 5549.5 and midpoint >= 4064.5:
        return "rubin_g"
    elif midpoint < 6920.5 and midpoint >= 5521.5:
        return "rubin_r"
    elif midpoint < 8202 and midpoint >= 6916:
        return "rubin_i"
    elif midpoint < 9200 and midpoint >= 8160:
        return "rubin_z"
    elif midpoint <= 10184 and midpoint >= 9322:
        return "rubin_y"
    else:
        return "unknown"


def designate_mjd_to_datetime(row: pd.Series, which: str) -> datetime:
    """Convert MJD to datetime."""

    if which == "begin":
        mjd = row["t_min"]
    else:
        mjd = row["t_max"]

    dt = Time(mjd, format="mjd").to_datetime()

    return dt


def rubin_to_across_schedule(
    telescope_id: str,
    data: pd.DataFrame,
    status: sdk.ScheduleStatus,
    fidelity: sdk.ScheduleFidelity,
    name: str,
    observations: list[sdk.ObservationCreate],
) -> sdk.ScheduleCreate:
    """
    Creates a Rubin schedule from the provided data.
    """

    begin = Time(min(data["date_range_begin"]))
    end = Time(max(data["date_range_end"]))

    return sdk.ScheduleCreate(
        telescope_id=telescope_id,
        name=f"{name}_{begin.isot.split('T')[0]}_{end.isot.split('T')[0]}",
        date_range=sdk.DateRange(
            begin=begin.to_datetime(),
            end=end.to_datetime(),
        ),
        status=status,
        fidelity=fidelity,
        observations=observations,
    )


def rubin_observation_to_across_observation(
    instrument_id: str,
    row: pd.Series,
    observation_type: sdk.ObservationType,
    observation_status: sdk.ObservationStatus,
) -> sdk.ObservationCreate:
    """
    Creates a Rubin observation from the provided row of data.
    """

    begin = Time(row["date_range_begin"])
    end = Time(row["date_range_end"])

    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=row["target_name"],
        pointing_position=sdk.Coordinate(
            ra=row["s_ra"],
            dec=row["s_dec"],
        ),
        date_range=sdk.DateRange(
            begin=begin.to_datetime(),
            end=end.to_datetime(),
        ),
        external_observation_id=str(row["obs_id"]),
        type=observation_type,
        status=observation_status,
        exposure_time=row["t_exptime"],
        bandpass=RUBIN_BANDPASSES[row["filter_name"]],
        pointing_angle=row["rubin_rot_sky_pos"],
        t_resolution=row["t_resolution"],
        em_res_power=row["em_res_power"],
        o_ucd=row["o_ucd"],
        pol_states=row["pol_states"],
        pol_xel=str(row["pol_xel"]),
        category=sdk.IVOAObsCategory(str.lower(row["category"])),
        priority=row["priority"],
        tracking_type=sdk.IVOAObsTrackingType(str.lower(row["tracking_type"])),
    )


class RubinSchedulerHandler:
    """Handler for Rubin scheduler tasks."""

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

    def run(self, df: pd.DataFrame) -> None:
        """Create a schedule from a DataFrame of Rubin LSST observations."""

        telescope = sdk.TelescopeApi(client).get_telescopes(name="lsst")[0]
        telescope_id = telescope.id
        if telescope.instruments:
            instrument_id = telescope.instruments[0].id

        observations = [
            rubin_observation_to_across_observation(
                instrument_id=instrument_id,
                row=row,
                observation_type=sdk.ObservationType.IMAGING,  # this should always be imaging for rubin
                observation_status=self.observation_status,
            )
            for _, row in df.iterrows()
        ]

        schedule = rubin_to_across_schedule(
            telescope_id=telescope_id,
            data=df,
            status=self.schedule_status,
            fidelity=self.schedule_fidelity,
            name=self.schedule_name,
            observations=observations,
        )

        print(schedule.model_dump())

        sdk.ScheduleApi(client).create_schedule(schedule)
