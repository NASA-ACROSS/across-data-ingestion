from datetime import datetime, timedelta

import astropy.units as u  # type: ignore[import-untyped]
import pandas as pd
import structlog
from astropy.coordinates import SkyCoord  # type: ignore[import-untyped]
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk

pd.options.mode.chained_assignment = None  # Disable pandas chained assignment warning

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


PLANNED_SCHEDULE_TABLE_URL = (
    "https://xmm-tools.cosmos.esa.int/external/xmm_sched/short_term_schedule.php"
)
REVOLUTION_FILE_BASE_URL = "https://xmmweb.esac.esa.int/user/mplan/summaries/"

EPIC_BANDPASS = sdk.EnergyBandpass.model_validate(
    {
        "filter_name": "XMM-Newton EPIC",
        "min": 0.3,
        "max": 12.0,
        "type": "ENERGY",
        "unit": sdk.EnergyUnit.KEV,
    }
)
RGS_BANDPASS = sdk.EnergyBandpass.model_validate(
    {
        "filter_name": "XMM-Newton RGS",
        "min": 0.35,
        "max": 2.5,
        "type": "ENERGY",
        "unit": sdk.EnergyUnit.KEV,
    }
)
OM_UVW2_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "filter_name": "OM UVW2",
        "min": 187.0,
        "max": 237.0,
        "type": "WAVELENGTH",
        "unit": sdk.WavelengthUnit.NM,
    }
)
OM_UVM2_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "filter_name": "OM UVM2",
        "min": 207.0,
        "max": 255.0,
        "type": "WAVELENGTH",
        "unit": sdk.WavelengthUnit.NM,
    }
)
OM_UVW1_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "filter_name": "OM UVW1",
        "min": 249.5,
        "max": 332.5,
        "type": "WAVELENGTH",
        "unit": sdk.WavelengthUnit.NM,
    }
)
OM_U_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "filter_name": "OM U",
        "min": 302.0,
        "max": 386.0,
        "type": "WAVELENGTH",
        "unit": sdk.WavelengthUnit.NM,
    }
)
OM_B_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "filter_name": "OM B",
        "min": 397.5,
        "max": 502.5,
        "type": "WAVELENGTH",
        "unit": sdk.WavelengthUnit.NM,
    }
)
OM_V_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "filter_name": "OM V",
        "min": 508.0,
        "max": 578.0,
        "type": "WAVELENGTH",
        "unit": sdk.WavelengthUnit.NM,
    }
)
OM_WHITE_BANDPASS = sdk.WavelengthBandpass.model_validate(
    {
        "filter_name": "OM White",
        "min": 232.5,
        "max": 579.5,
        "type": "WAVELENGTH",
        "unit": sdk.WavelengthUnit.NM,
    }
)

XMM_BANDPASSES: dict[str, sdk.EnergyBandpass | sdk.WavelengthBandpass] = {
    "EPIC": EPIC_BANDPASS,
    "RGS": RGS_BANDPASS,
    "UVW2": OM_UVW2_BANDPASS,
    "UVM2": OM_UVM2_BANDPASS,
    "UVW1": OM_UVW1_BANDPASS,
    "U": OM_U_BANDPASS,
    "B": OM_B_BANDPASS,
    "V": OM_V_BANDPASS,
    "WHITE": OM_WHITE_BANDPASS,
}


def read_planned_schedule_table() -> pd.DataFrame:
    """Read the planned schedule table as a pandas DataFrame"""
    dfs: list[pd.DataFrame] = pd.read_html(
        PLANNED_SCHEDULE_TABLE_URL, flavor="bs4", header=0
    )
    if len(dfs) == 0:
        logger.warn("Could not read planned schedule table")
        return pd.DataFrame([])

    schedule_df = dfs[0]
    # Filter by future observations
    planned_schedule_df = schedule_df[
        schedule_df["UTC Obs Start yyyy-mm-dd hh:mm:ss"]
        > datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ]
    return planned_schedule_df


def read_revolution_timeline_file(revolution_id: int) -> pd.DataFrame:
    """Read a revolution timeline file by revolution ID as a pandas DataFrame"""
    dfs: list[pd.DataFrame] = pd.read_html(
        REVOLUTION_FILE_BASE_URL + f"{revolution_id}_nice.html", flavor="bs4", header=0
    )
    if len(dfs) == 0:
        logger.warn(
            "Could not read revolution timeline file", revolution_id=revolution_id
        )
        return pd.DataFrame([])

    # The second HTML table on that page is the timeline data, so return it
    return dfs[1]


def extract_om_exposures_from_timeline_data(timeline_df: pd.DataFrame) -> dict:
    """
    Read individual OM exposures from the timeline data and return them.
    Parses the timeline data to get the start and stop time of each exposure.
    Finds the correct OM filter by parsing the string in the "OM" field.
    Additionally parses the strings to get the exposure time per filter.
    """
    # Get the start and stop indices for each observation in this revolution
    obs_start_inds = timeline_df[timeline_df["Event"] == "OBS_START"].index
    obs_stop_inds = timeline_df[timeline_df["Event"] == "OBS_END"].index

    exposures = {}
    for start_ind, stop_ind in zip(*(obs_start_inds, obs_stop_inds)):
        # For each observation, get the OM exposures in each filter
        current_obs_df = timeline_df[start_ind:stop_ind]

        # Get the current observation ID
        obs_id_ind = current_obs_df[current_obs_df["Event"].str[:3] == "ID:"].index
        # The string is of form "ID: 12345", so slice it to just get the numerical part
        obs_id = current_obs_df["Event"][obs_id_ind].values[0][4:]

        # Construct a mask to find the start indices of each exposure by matching filter names
        om_timeline_df = current_obs_df[current_obs_df["OM"].notna()]
        filter_list = ["UVW2", "UVM2", "UVW1", "U", "B", "V", "WHITE"]
        # Split the value of the string and match filter names in the spit string
        split_om_logs = om_timeline_df["OM"].str.split().str[0]
        mask = [
            any(filt == split_string[:4] for filt in filter_list)
            for split_string in split_om_logs
        ]
        # The row that matches the filter has the start time
        exposure_start_inds = om_timeline_df[mask].index
        exposure_start_times = current_obs_df["Date & Time"][exposure_start_inds].values

        # Get the filter for each exposure
        exposure_filters = (
            current_obs_df["OM"][exposure_start_inds].str.split().str[0].values
        )

        # Get each unique exposure time (i.e., one entry per exposure)
        raw_exposure_times = om_timeline_df[om_timeline_df["OM"].str.endswith("sec")][
            "OM"
        ].unique()
        # exptime has the form "Image ID: 600 sec"
        exposure_times = [
            exptime.split(":")[-1].strip() for exptime in raw_exposure_times
        ]

        # Add it to the dictionary
        exposures[obs_id] = [
            {
                "filter": filt,
                "start_time": start_time.replace(
                    " | ", " "
                ),  # start_time has form "2025-08-20 | 00:00:00"
                "exposure_time": int(exptime.split()[0]),  # exptime has form "600 sec"
            }
            for filt, start_time, exptime in zip(
                *(exposure_filters, exposure_start_times, exposure_times)
            )
        ]

    return exposures


def transform_to_across_schedule(
    start_datetime: str, end_datetime: str, telescope_id: str
) -> sdk.ScheduleCreate:
    """Format the schedule data in the ACROSS format"""
    return sdk.ScheduleCreate(
        name=f"XMM_Newton_planned_{start_datetime.split()[0]}_{end_datetime.split()[0]}",
        telescope_id=telescope_id,
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=sdk.ScheduleFidelity.LOW,
        date_range=sdk.DateRange.model_validate(
            {
                "begin": start_datetime,
                "end": end_datetime,
            }
        ),
        observations=[],
    )


def transform_to_across_observation(
    row: pd.Series,
    exposure_start: str,
    exposure_time: float,
    instrument_id: str,
    observation_type: sdk.ObservationType,
    bandpass: sdk.Bandpass,
) -> sdk.ObservationCreate:
    """Construct ACROSS observation for the given exposure"""
    pointing_coord = SkyCoord(
        row["RA hh:mm:ss"], row["DEC dd:mm:ss"], unit=(u.hourangle, u.deg)
    )
    pointing_position = sdk.Coordinate.model_validate(
        {
            "ra": pointing_coord.ra.deg,
            "dec": pointing_coord.dec.deg,
        }
    )
    start_time = datetime.strptime(exposure_start, "%Y-%m-%d %H:%M:%S")
    end_time = start_time + timedelta(seconds=exposure_time)
    date_range = sdk.DateRange.model_validate(
        {
            "begin": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    return sdk.ObservationCreate(
        instrument_id=instrument_id,
        object_name=row["Target Name"],
        external_observation_id="0" + str(row["Obs Id."]),
        pointing_position=pointing_position,
        object_position=pointing_position,
        pointing_angle=row["PA ddd.dd"],
        date_range=date_range,
        exposure_time=exposure_time,
        status=sdk.ObservationStatus.PLANNED,
        type=observation_type,
        bandpass=bandpass,
    )


def create_mos_observations(
    observations_df: pd.DataFrame, instrument_id_dict: dict
) -> list[sdk.ObservationCreate]:
    observations_df["max_mos_exposure"] = observations_df.apply(
        lambda row: max(
            # "()" exposures signify closed filter, for our case we can ignore
            float(str(row["MOS1 Dur. Ks"]).replace("( ", "").replace(")", "")),
            float(str(row["MOS2 Dur. Ks"]).replace("( ", "").replace(")", "")),
        )
        * 1000.0,
        axis=1,
    )

    return [
        transform_to_across_observation(
            row,
            row["UTC Obs Start yyyy-mm-dd hh:mm:ss"],
            row["max_mos_exposure"],
            instrument_id_dict["EPIC-MOS"],
            sdk.ObservationType.IMAGING,
            sdk.Bandpass(XMM_BANDPASSES["EPIC"]),
        )
        for _, row in observations_df.iterrows()
    ]


def create_rgs_observations(
    observations_df: pd.DataFrame, instrument_id_dict: dict
) -> list[sdk.ObservationCreate]:
    observations_df["max_rgs_exposure"] = observations_df.apply(
        lambda row: max(
            float(row["RGS1 Dur. Ks"]),
            float(row["RGS2 Dur. Ks"]),
        )
        * 1000.0,
        axis=1,
    )

    return [
        transform_to_across_observation(
            row,
            row["UTC Obs Start yyyy-mm-dd hh:mm:ss"],
            row["max_rgs_exposure"],
            instrument_id_dict["RGS"],
            sdk.ObservationType.SPECTROSCOPY,
            sdk.Bandpass(XMM_BANDPASSES["RGS"]),
        )
        for _, row in observations_df.iterrows()
    ]


def create_pn_observations(
    observations_df: pd.DataFrame, instrument_id_dict: dict
) -> list[sdk.ObservationCreate]:
    return [
        transform_to_across_observation(
            row,
            row["UTC Obs Start yyyy-mm-dd hh:mm:ss"],
            float(str(row["PN Dur Ks"]).replace("( ", "").replace(")", "")) * 1000.0,
            instrument_id_dict["EPIC-PN"],
            sdk.ObservationType.IMAGING,
            sdk.Bandpass(XMM_BANDPASSES["EPIC"]),
        )
        for _, row in observations_df.iterrows()
    ]


def ingest() -> None:
    """
    Ingests low fidelity planned XMM-Newton schedules.
    Reads the published short term planned schedule HTML table
    for the upcoming 2-4 weeks to retrieve EPIC-MOS, EPIC-pn, and
    RGS scheduled observations. For individual OM exposures, this
    method crossmatches with the planned timefile files for a given
    revolution and extracts the filter, exposure time, and start time
    for each OM exposure, and adds them as ACROSS observations.
    """
    # GET telescope and instrument info
    telescope = sdk.TelescopeApi(client).get_telescopes(name="XMM-Newton")[0]
    if telescope.instruments:
        instrument_id_dict = {
            instrument.short_name: instrument.id for instrument in telescope.instruments
        }

    raw_planned_schedule_data = read_planned_schedule_table()
    if not len(raw_planned_schedule_data):
        return

    start_datetime = min(
        raw_planned_schedule_data["UTC Obs Start yyyy-mm-dd hh:mm:ss"].values
    )
    end_datetime = max(
        raw_planned_schedule_data["UTC Obs End yyyy-mm-dd hh:mm:ss"].values
    )
    across_schedule = transform_to_across_schedule(
        start_datetime, end_datetime, telescope.id
    )

    # Iterate over the planned schedule data by unique revolution ID,
    # getting OM observations from the revolution timeline file, and
    # constructing observations using the schedule data + OM exposure data
    unique_rev_ids = raw_planned_schedule_data["Revn #"].unique()
    for rev_id in unique_rev_ids:
        # Read the revolution timeline for this revolution
        revolution_timeline_df = read_revolution_timeline_file(rev_id)

        # Filter the dataframe for the current revolution
        current_revolution_observations_df = raw_planned_schedule_data[
            raw_planned_schedule_data["Revn #"] == rev_id
        ]

        # Create observations for each instrument
        across_mos_observations = create_mos_observations(
            current_revolution_observations_df, instrument_id_dict
        )
        across_schedule.observations.extend(across_mos_observations)

        across_rgs_observations = create_rgs_observations(
            current_revolution_observations_df, instrument_id_dict
        )
        across_schedule.observations.extend(across_rgs_observations)

        across_pn_observations = create_pn_observations(
            current_revolution_observations_df, instrument_id_dict
        )
        across_schedule.observations.extend(across_pn_observations)

        if len(revolution_timeline_df):
            # Get OM exposure info from the revolution timeline df
            om_exposures = extract_om_exposures_from_timeline_data(
                revolution_timeline_df
            )
            across_om_observations = [
                transform_to_across_observation(
                    row,
                    exposure["start_time"],
                    exposure["exposure_time"],
                    instrument_id_dict["OM"],
                    sdk.ObservationType.IMAGING,
                    sdk.Bandpass(XMM_BANDPASSES[exposure["filter"]]),
                )
                for _, row in current_revolution_observations_df.iterrows()
                for exposure in om_exposures["0" + str(row["Obs Id."])]
            ]
            across_schedule.observations.extend(across_om_observations)

    try:
        sdk.ScheduleApi(client).create_schedule(across_schedule)
    except sdk.ApiException as err:
        if err.status == 409:
            logger.warning("Schedule already exists.", err=err.__dict__)
        else:
            raise err


@repeat_at(cron="0 1,9,17 * * *", logger=logger)
async def entrypoint() -> None:
    try:
        ingest()
        logger.info("XMM-Newton schedule ingestion ran successfully")
    except Exception as e:
        logger.error(
            "XMM-Newton schedule ingestion encountered an unexpected error", err=e
        )
