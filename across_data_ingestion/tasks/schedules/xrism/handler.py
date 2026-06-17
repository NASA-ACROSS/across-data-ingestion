from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import structlog
from astropy.time import Time  # type: ignore[import-untyped]

from ....util.across_server import client, sdk
from ....util.footprint_util import project_footprint
from .constants import XRISM_BANDPASSES, XRISM_OBSERVATION_TYPES

logger: structlog.stdlib.BoundLogger = structlog.getLogger()


class XRISMScheduleHandler:
    """Handles the creation and posting of XRISM schedules to the ACROSS API."""

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
        self.telescope = sdk.TelescopeApi(client).get_telescopes(
            name="X-ray Mirror Assembly", include_footprints=True
        )[0]

    def _create_xrism_observations(
        self,
        observation_df: pd.DataFrame,
        observation_status: sdk.ObservationStatus,
        instrument: sdk.TelescopeInstrument,
    ) -> list[sdk.ObservationCreate]:
        """
        Creates ACROSS ObservationCreate objects for XRISM observations from
        the short term schedule pandas DataFrame.
        """
        across_observations = []

        for _, obs in observation_df.iterrows():
            # Filter out calibration observations with NaN exposure times
            if not np.isnan(obs.exposure_time):
                footprint = None
                if instrument.footprints:
                    footprint = project_footprint(
                        instrument.footprints,
                        ra=float(obs.Ra),
                        dec=float(obs.Dec),
                        roll_angle=float(obs.roll_angle)
                        if hasattr(obs, "roll_angle")
                        else 0.0,
                    )
                across_observations.append(
                    sdk.ObservationCreate(
                        instrument_id=instrument.id,
                        object_name=obs.TargetName,
                        pointing_position=sdk.Coordinate(
                            ra=float(obs.Ra),
                            dec=float(obs.Dec),
                        ),
                        object_position=sdk.Coordinate(
                            ra=float(obs.Ra),
                            dec=float(obs.Dec),
                        ),
                        date_range=sdk.DateRange(
                            begin=datetime.strptime(
                                obs.start_time, "%Y-%m-%dT%H:%M:%S"
                            ),
                            end=datetime.strptime(obs.start_time, "%Y-%m-%dT%H:%M:%S")
                            + timedelta(seconds=obs.exposure_time * 1000.0),
                        ),
                        external_observation_id=str(obs.observation_id),
                        type=XRISM_OBSERVATION_TYPES.get(
                            instrument.short_name, sdk.ObservationType.IMAGING
                        ),
                        status=observation_status,
                        pointing_angle=float(obs.roll_angle)
                        if hasattr(obs, "roll_angle")
                        else 0.0,
                        exposure_time=obs.exposure_time * 1000.0,
                        bandpass=sdk.Bandpass(XRISM_BANDPASSES[instrument.short_name]),
                        footprint=footprint,
                    )
                )

        return across_observations

    def _create_xrism_across_schedule(
        self,
        schedule_status: sdk.ScheduleStatus,
        schedule_fidelity: sdk.ScheduleFidelity,
        schedule_name: str,
        observations: list[sdk.ObservationCreate],
    ) -> sdk.ScheduleCreate:
        """
        Creates ACROSS ScheduleCreate object given
        the status and fidelity of the schedule, the name of the schedule,
        and a list of ObservationCreate objects.
        """
        begins = [obs.date_range.begin for obs in observations]
        ends = [obs.date_range.end for obs in observations]

        begin = Time(min(begins)).isot
        end = Time(max(ends)).isot

        return sdk.ScheduleCreate(
            telescope_id=self.telescope.id,
            name=f"XRISM_{schedule_name}_{begin.split('T')[0]}_{end.split('T')[0]}",
            date_range=sdk.DateRange(
                begin=begin,
                end=end,
            ),
            status=schedule_status,
            fidelity=schedule_fidelity,
            observations=observations,
        )

    def run(self, observation_df: pd.DataFrame):
        """
        Run the schedule handler to create and post schedules and observations
        to the ACROSS server, given a pandas DataFrame of observation information.
        """
        # Create observations for each instrument
        xrism_obs = []
        for instrument in self.telescope.instruments:  # type: ignore[union-attr]
            xrism_obs += self._create_xrism_observations(
                observation_df=observation_df,
                observation_status=self.observation_status,
                instrument=instrument,
            )

        # Create schedule containing all observations
        xrism_schedule = self._create_xrism_across_schedule(
            schedule_status=self.schedule_status,
            schedule_fidelity=self.schedule_fidelity,
            schedule_name=self.schedule_name,
            observations=xrism_obs,
        )

        # Post the schedule to the ACROSS API
        try:
            sdk.ScheduleApi(client).create_schedule(xrism_schedule)
        except sdk.ApiException as err:
            if err.status == 409:
                logger.info(
                    "Schedule already exists.", schedule_name=xrism_schedule.name
                )
            else:
                raise err
