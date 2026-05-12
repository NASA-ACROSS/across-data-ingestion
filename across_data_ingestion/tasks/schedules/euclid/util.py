from datetime import datetime, timedelta

import pandas as pd
import structlog
from astropy.time import Time  # type: ignore[import-untyped]

from ....util.across_server import client, sdk
from .constants import (
    BANDPASS_EXPOSURE_TIMES,
    EUCLID_NISP_BANDPASS_DICT,
    EUCLID_NISP_BLUE_GRISM,
    EUCLID_NISP_RED_GRISM,
    EUCLID_VIS_BANDPASS,
)

logger: structlog.stdlib.BoundLogger = structlog.getLogger()


POINTING_COLUMNS = [
    "record_type",
    "obs_tag",
    "mjd",
    "utc",
    "duration",
    "obs_id",
    "pointing_id",
    "dither_id",
    "patch_id",
    "quat_x",
    "quat_y",
    "quat_z",
    "quat_w",
    "center_lon",
    "center_lat",
    "pos_angle",
    "saa",
    "alpha",
    "corner_x0",
    "corner_y0",
    "corner_x1",
    "corner_y1",
    "corner_x2",
    "corner_y2",
    "corner_x3",
    "corner_y3",
    "area",
    "grism",
]

SCIENCE_OBS_TAGS_PREFIXES = [
    "WIDE",
    "SOUTH",
    "NORTH",
    "CPC",
    "DEEP",
    "INFILL",
    "COLOR",
    "ROS",
    "PHOTO-Z",
]

NISP_BANDPASSES = ["Y", "J", "H"]


class EuclidScheduleHandler:
    """Handles the creation and posting of Euclid schedules to the ACROSS API."""

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
        self._extract_telescope_instrument_ids()

    def _extract_telescope_instrument_ids(self) -> None:
        """
        Extract telescope ID and build map of instrument name : instrument ID
        from the ACROSS API to be used for creating observations and schedules.
        Sets these as attributes on this class.
        """
        instrument_name_id_map = {}
        telescope = sdk.TelescopeApi(client).get_telescopes(name="Euclid")[0]
        self.telescope_id = telescope.id
        if telescope.instruments:
            for instrument in telescope.instruments:
                instrument_name_id_map[instrument.short_name] = instrument.id
        self.instrument_ids = instrument_name_id_map

    def _create_vis_observations(
        self,
        observation_df: pd.DataFrame,
        observation_status: sdk.ObservationStatus,
        instrument_id: str,
    ) -> list[sdk.ObservationCreate]:
        """
        Creates ACROSS ObservationCreate objects for VIS observations from
        a pandas DataFrame of Euclid pointings.
        Assumes a standard Euclid observing pattern where each field
        is tiled and observed in four pointings, each with
        an exposure time of 566 seconds in VIS.
        """
        vis_exposure_time = BANDPASS_EXPOSURE_TIMES[EUCLID_VIS_BANDPASS.filter_name]  # type: ignore
        return [
            sdk.ObservationCreate(
                instrument_id=instrument_id,
                object_name=pointing.obs_tag,
                pointing_position=sdk.Coordinate(
                    ra=float(pointing.center_lon),
                    dec=float(pointing.center_lat),
                ),
                date_range=sdk.DateRange(
                    begin=datetime.strptime(pointing.utc, "%Y-%m-%dT%H:%M:%S%z"),
                    end=datetime.strptime(pointing.utc, "%Y-%m-%dT%H:%M:%S%z")
                    + timedelta(seconds=vis_exposure_time),
                ),
                external_observation_id=pointing.obs_id,
                type=sdk.ObservationType.IMAGING,
                status=observation_status,
                pointing_angle=float(pointing.pos_angle),
                exposure_time=vis_exposure_time,
                bandpass=sdk.Bandpass(EUCLID_VIS_BANDPASS),
            )
            for _, pointing in observation_df.iterrows()
        ]

    def _create_nisp_observations(
        self,
        observation_df: pd.DataFrame,
        observation_status: sdk.ObservationStatus,
        instrument_id: str,
    ) -> list[sdk.ObservationCreate]:
        """
        Creates ACROSS ObservationCreate objects for NISP observations from
        a pandas DataFrame of Euclid pointings.
        Assumes a standard Euclid observing pattern where each field
        is tiled and observed in four pointings, each with
        one grism exposure and three photometric observations with NISP.
        """
        across_observations = []
        # Group by observation ID
        grouped_observations = observation_df.groupby("obs_id")
        for obs_id, group in grouped_observations:
            grisms = [val for val in group["grism"].values if val]
            if len(grisms):
                grism = grisms[0]
            else:
                # Default to red grism if observation group has no grism info
                # Red is the default grism for WIDE survey observations, but this is just a fallback.
                # Documentation here: https://www.cosmos.esa.int/documents/10647/20498717/MOCDC_v4.5.pdf
                logger.warning(
                    "Could not find grism information for observation ID, defaulting to RED grism",
                    obs_id=obs_id,
                )
                grism = "RED"

            # Look up grism bandpass by grism field on first row in each group
            if grism.startswith("RED"):
                grism_bandpass = EUCLID_NISP_RED_GRISM
            else:
                grism_bandpass = EUCLID_NISP_BLUE_GRISM

            # Add one grism obs + 3 imaging obs for each pointing
            for _, pointing in group.iterrows():
                # Grism observation:
                grism_exposure_time = BANDPASS_EXPOSURE_TIMES[
                    grism_bandpass.filter_name  # type: ignore
                ]
                across_observations.append(
                    sdk.ObservationCreate(
                        instrument_id=instrument_id,
                        object_name=pointing.obs_tag,
                        pointing_position=sdk.Coordinate(
                            ra=float(pointing.center_lon),
                            dec=float(pointing.center_lat),
                        ),
                        date_range=sdk.DateRange(
                            begin=datetime.strptime(
                                pointing.utc, "%Y-%m-%dT%H:%M:%S%z"
                            ),
                            end=datetime.strptime(pointing.utc, "%Y-%m-%dT%H:%M:%S%z")
                            + timedelta(seconds=grism_exposure_time),
                        ),
                        external_observation_id=pointing.obs_id,
                        type=sdk.ObservationType.SPECTROSCOPY,
                        status=observation_status,
                        pointing_angle=float(pointing.pos_angle),
                        exposure_time=grism_exposure_time,
                        bandpass=sdk.Bandpass(grism_bandpass),
                    )
                )

                # Imaging obs:
                for filt in NISP_BANDPASSES:
                    imaging_exposure_time = BANDPASS_EXPOSURE_TIMES[
                        EUCLID_NISP_BANDPASS_DICT[filt].filter_name  # type: ignore
                    ]
                    across_observations.append(
                        sdk.ObservationCreate(
                            instrument_id=instrument_id,
                            object_name=pointing.obs_tag,
                            pointing_position=sdk.Coordinate(
                                ra=float(pointing.center_lon),
                                dec=float(pointing.center_lat),
                            ),
                            date_range=sdk.DateRange(
                                begin=datetime.strptime(
                                    pointing.utc, "%Y-%m-%dT%H:%M:%S%z"
                                ),
                                end=datetime.strptime(
                                    pointing.utc, "%Y-%m-%dT%H:%M:%S%z"
                                )
                                + timedelta(seconds=imaging_exposure_time),
                            ),
                            external_observation_id=pointing.obs_id,
                            type=sdk.ObservationType.IMAGING,
                            status=observation_status,
                            pointing_angle=float(pointing.pos_angle),
                            exposure_time=imaging_exposure_time,
                            bandpass=sdk.Bandpass(EUCLID_NISP_BANDPASS_DICT[filt]),
                        )
                    )

        return across_observations

    def _create_euclid_across_schedule(
        self,
        telescope_id: str,
        schedule_status: sdk.ScheduleStatus,
        schedule_fidelity: sdk.ScheduleFidelity,
        schedule_name: str,
        observations: list[sdk.ObservationCreate],
    ) -> sdk.ScheduleCreate:
        """
        Creates ACROSS ScheduleCreate object given
        the telescope ID in the ACROSS server, the status and
        fidelity of the schedule, the name of the schedule,
        and a list of ObservationCreate objects.
        """
        begins = [obs.date_range.begin for obs in observations]
        ends = [obs.date_range.end for obs in observations]

        begin = Time(min(begins)).isot
        end = Time(max(ends)).isot

        return sdk.ScheduleCreate(
            telescope_id=telescope_id,
            name=f"Euclid_{schedule_name}_{begin.split('T')[0]}_{end.split('T')[0]}",
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
        vis_obs = self._create_vis_observations(
            observation_df=observation_df,
            observation_status=self.observation_status,
            instrument_id=self.instrument_ids["Euclid VIS"],
        )

        nisp_obs = self._create_nisp_observations(
            observation_df=observation_df,
            observation_status=self.observation_status,
            instrument_id=self.instrument_ids["Euclid NISP"],
        )

        # Create schedule containing all observations
        euclid_schedule = self._create_euclid_across_schedule(
            telescope_id=self.telescope_id,
            schedule_status=self.schedule_status,
            schedule_fidelity=self.schedule_fidelity,
            schedule_name=self.schedule_name,
            observations=vis_obs + nisp_obs,
        )

        # Post the schedule to the ACROSS API
        try:
            sdk.ScheduleApi(client).create_schedule(euclid_schedule)
        except sdk.ApiException as err:
            if err.status == 409:
                logger.info(
                    "Schedule already exists.", schedule_name=euclid_schedule.name
                )
            else:
                raise err
