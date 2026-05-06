from datetime import datetime, timedelta

import astropy.units as u  # type: ignore[import-untyped]
import httpx
import pandas as pd
import structlog
from astropy.coordinates import SkyCoord  # type: ignore[import-untyped]
from bs4 import BeautifulSoup
from fastapi_utilities import repeat_at  # type: ignore

from ....util.across_server import client, sdk
from ....util.footprint_util import project_footprint

pd.options.mode.chained_assignment = None  # Disable pandas chained assignment warning

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


PLANNED_SCHEDULE_TABLE_URL = (
    "https://xmm-tools.cosmos.esa.int/external/xmm_sched/short_term_schedule.php"
)

SCHEDULED_OBS_URL = (
    "https://xmmweb.esac.esa.int/cgi-bin_external/obs_search/selectobs_cosmos"
)

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
OM_FILTERS = ["UVW2", "UVM2", "UVW1", "U", "B", "V", "WHITE"]


def get_datetime_now() -> datetime:
    """Wraps the datetime.now method for easier testing"""
    return datetime.now()


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


def extract_om_exposures_from_observation_data(revolution_id: int) -> dict:
    """
    Read individual OM filter exposures using the scheduled observations search page.
    Submit an HTTP request containing the observation ID, scrape the HTML using bs4,
    and return the OM observation filters, start datetimes, and exposure times.

    Parameters
    -----------
        revolution_id (int): the ID of the observation

    Returns
    -----------
        list: a list containing dictionaries of filter, start date, and exposure times
    """
    expected_tables_per_page: int = (
        2  # The number of HTML tables that should be returned
    )
    data = {"revn": revolution_id}

    try:
        response = httpx.post(SCHEDULED_OBS_URL, data=data, timeout=10.0)
    except httpx.HTTPError as exc:
        logger.warning(
            "Scheduled observations page request failed",
            revolution_id=revolution_id,
            error=str(exc),
        )
        return {}

    if response.status_code != 200:
        logger.warning(
            "Scheduled observations page returned bad status code",
            status_code=response.status_code,
        )
        return {}

    soup = BeautifulSoup(response.text, features="html5lib")
    tables = soup.find_all("table")
    if len(tables) < expected_tables_per_page:
        # Page does not contain the correct HTML table to scrape
        return {}

    obs_tab = tables[1]

    om_observations_per_revolution: dict[str, list[dict]] = {}
    om_exposures: list[dict] = []
    obs_id = None

    # Observation ID rows have background-color style
    observation_row_style = "background-color: #EEA500;"
    for row in obs_tab.find_all("tr"):
        if row.attrs.get("style", "") == observation_row_style:
            # Reached a new observation, so add previously aggregated exposures
            # to the dictionary
            if obs_id is not None:
                om_observations_per_revolution[obs_id] = om_exposures
            obs_id = row.find("td").get_text(strip=True)  # type: ignore[union-attr]
            om_exposures = []
        else:
            first_div = row.find_all("td")[0]
            if "OM" in first_div.text and any(
                [filt in first_div.text.split() for filt in OM_FILTERS]
            ):
                filt = first_div.get_text(strip=True).split("-")[1].split()[0]
                start_time = (
                    row.find_all("td")[1].get_text(strip=True).replace("@", "T")
                )
                exposure_time = row.find_all("td")[2].get_text(strip=True)

                # Must convert the string start time to a valid future datetime
                # The start time only contains month and day, not year, so
                # we must assume it is in the future and add the correct year
                start_datetime = datetime.strptime(start_time, "%m-%dT%H:%M:%S")
                now = get_datetime_now()

                # Use current year, if passed, use next year
                target_year = now.year
                start_datetime = start_datetime.replace(year=target_year)

                if start_datetime < now:
                    start_datetime = start_datetime.replace(year=target_year + 1)

                start_time = start_datetime.strftime("%Y-%m-%d %H:%M:%S")

                om_exposures.append(
                    {
                        "filter": filt,
                        "start_time": start_time,
                        "exposure_time": float(exposure_time),
                    }
                )

    # Reached the end of the loop, so add the last observations aggregated to the dictionary
    if obs_id is not None:
        om_observations_per_revolution[obs_id] = om_exposures

    return om_observations_per_revolution


def transform_to_across_schedule(
    schedule_data: pd.DataFrame, telescope_id: str
) -> sdk.ScheduleCreate:
    """Format the schedule data in the ACROSS format"""
    start_datetime = min(schedule_data["UTC Obs Start yyyy-mm-dd hh:mm:ss"].values)
    end_datetime = max(schedule_data["UTC Obs End yyyy-mm-dd hh:mm:ss"].values)
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
    instrument_footprint: dict,
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

    footprint = None
    if instrument_id in instrument_footprint.keys():
        footprint = project_footprint(
            footprint_points=instrument_footprint[instrument_id],
            ra=pointing_coord.ra.deg,
            dec=pointing_coord.dec.deg,
            roll_angle=row["PA ddd.dd"],
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
        footprint=footprint,
    )


def create_mos_observations(
    observations_df: pd.DataFrame, instrument_id_dict: dict, instrument_footprint: dict
) -> list[sdk.ObservationCreate]:
    observations_df["max_mos_exposure"] = observations_df.apply(
        lambda row: (
            max(
                # "()" exposures signify closed filter, for our case we can ignore
                float(str(row["MOS1 Dur. Ks"]).replace("( ", "").replace(")", "")),
                float(str(row["MOS2 Dur. Ks"]).replace("( ", "").replace(")", "")),
            )
            * 1000.0
        ),
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
            instrument_footprint,
        )
        for _, row in observations_df.iterrows()
    ]


def create_rgs_observations(
    observations_df: pd.DataFrame, instrument_id_dict: dict, instrument_footprint: dict
) -> list[sdk.ObservationCreate]:
    observations_df["max_rgs_exposure"] = observations_df.apply(
        lambda row: (
            max(
                float(row["RGS1 Dur. Ks"]),
                float(row["RGS2 Dur. Ks"]),
            )
            * 1000.0
        ),
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
            instrument_footprint,
        )
        for _, row in observations_df.iterrows()
    ]


def create_pn_observations(
    observations_df: pd.DataFrame, instrument_id_dict: dict, instrument_footprint: dict
) -> list[sdk.ObservationCreate]:
    return [
        transform_to_across_observation(
            row,
            row["UTC Obs Start yyyy-mm-dd hh:mm:ss"],
            float(str(row["PN Dur Ks"]).replace("( ", "").replace(")", "")) * 1000.0,
            instrument_id_dict["EPIC-PN"],
            sdk.ObservationType.IMAGING,
            sdk.Bandpass(XMM_BANDPASSES["EPIC"]),
            instrument_footprint,
        )
        for _, row in observations_df.iterrows()
    ]


def aggregate_observations(
    schedule_data: pd.DataFrame, instrument_id_dict: dict, instrument_footprint: dict
) -> list[sdk.ObservationCreate]:
    """
    Iterate over the planned schedule data by unique revolution ID,
    getting OM observations from the revolution timeline file, and
    constructing observations using the schedule data + OM exposure data
    """
    across_observations: list[sdk.ObservationCreate] = []
    unique_rev_ids = schedule_data["Revn #"].unique()
    for rev_id in unique_rev_ids:
        # Filter the dataframe for the current revolution
        current_revolution_observations_df = schedule_data[
            schedule_data["Revn #"] == rev_id
        ]

        # Create observations for each instrument
        across_mos_observations = create_mos_observations(
            current_revolution_observations_df, instrument_id_dict, instrument_footprint
        )
        across_observations.extend(across_mos_observations)

        across_rgs_observations = create_rgs_observations(
            current_revolution_observations_df, instrument_id_dict, instrument_footprint
        )
        across_observations.extend(across_rgs_observations)

        across_pn_observations = create_pn_observations(
            current_revolution_observations_df, instrument_id_dict, instrument_footprint
        )
        across_observations.extend(across_pn_observations)

        om_exposures_for_revn = extract_om_exposures_from_observation_data(
            revolution_id=rev_id
        )
        if len(om_exposures_for_revn) == 0:
            logger.warning(
                "Did not find OM exposures from scheduled observations search page",
                rev_id=rev_id,
            )
        else:
            for obs_id, om_exposures in om_exposures_for_revn.items():
                # Edge case where the OM observation may not be in the current revolution df
                # if the current revolution overlaps with datetime.now()
                if int(obs_id) in current_revolution_observations_df["Obs Id."].values:
                    row = current_revolution_observations_df[
                        current_revolution_observations_df["Obs Id."] == int(obs_id)
                    ].iloc[0]
                    across_om_observations = [
                        transform_to_across_observation(
                            row,
                            exposure["start_time"],
                            exposure["exposure_time"],
                            instrument_id_dict["OM"],
                            sdk.ObservationType.IMAGING,
                            sdk.Bandpass(XMM_BANDPASSES[exposure["filter"]]),
                            instrument_footprint,
                        )
                        for exposure in om_exposures
                    ]
                    across_observations.extend(across_om_observations)

    return across_observations


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
    instrument_footprint = {}
    if telescope.instruments:
        instrument_id_dict = {
            instrument.short_name: instrument.id for instrument in telescope.instruments
        }
        instrument_footprint = {
            instrument.id: instrument.footprints for instrument in telescope.instruments
        }

    raw_planned_schedule_data = read_planned_schedule_table()
    if not len(raw_planned_schedule_data):
        return

    across_schedule = transform_to_across_schedule(
        raw_planned_schedule_data, telescope.id
    )

    across_schedule.observations = aggregate_observations(
        raw_planned_schedule_data, instrument_id_dict, instrument_footprint
    )

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
