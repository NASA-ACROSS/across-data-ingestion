import os
from collections.abc import Generator
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import httpx
import numpy as np
import pandas as pd
import pytest
from astropy.io import fits  # type: ignore[import-untyped]
from httpx import Response

from across_data_ingestion.tasks.schedules.fermi import lat_planned
from across_data_ingestion.tasks.schedules.fermi.lat_planned import (
    FERMI_LAT_MAX_ENERGY,
    FERMI_LAT_MIN_ENERGY,
    FERMI_LAT_POINTING_ANGLE,
    PointingFile,
)
from across_data_ingestion.util.across_server import sdk


@pytest.fixture
def mock_base_path() -> str:
    return os.path.join(os.path.dirname(__file__), "mocks/")


@pytest.fixture(autouse=True)
def patch_base_path(monkeypatch: pytest.MonkeyPatch, mock_base_path: str):
    monkeypatch.setattr(
        lat_planned, "FERMI_LAT_POINTING_FILE_BASE_PATH", mock_base_path
    )


@pytest.fixture
def fake_fermi_week() -> int:
    return 875


@pytest.fixture
def fake_fermi_html(fake_fermi_week: int) -> str:
    return (
        '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">\n'
        "<html>\n"
        "<head>\n"
        "<title>Index of /ssc/observations/timeline/ft2/files</title>\n"
        "</head>\n"
        "<body>\n"
        "</h1>\n"
        "<pre>\n"
        '<img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>\n'
        '<a href="?C=M;O=A">Last modified</a><a href="?C=S;O=A">Size</a>\n'
        '<a href="?C=D;O=A">Description</a>\n'
        "<hr>\n"
        '<img src="/icons/back.gif" alt="[DIR]"><a href="/ssc/observations/timeline/ft2/">Parent Directory</a>-<img src="/icons/unknown.gif" alt="[   ]"> \n'
        f'<a href="FERMI_POINTING_FINAL_{fake_fermi_week}_2025065_2025072_00.fits">FERMI_POINTING_FINAL_{fake_fermi_week}_2025065_2025072_00.fits</a> 19-May-2010 13:19  970K  <img src="/icons/unknown.gif" alt="[   ]">\n'
        f'<a href="FERMI_POINTING_FINAL_{fake_fermi_week+1}_2025072_2025079_00.fits">FERMI_POINTING_FINAL_{fake_fermi_week+1}_2025072_2025079_00.fits</a> 25-May-2010 13:19  970K  <img src="/icons/unknown.gif" alt="[   ]">\n'
        f'<a href="FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_00.fits">FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_00.fits</a> 20-May-2010 13:19  970K  <img src="/icons/unknown.gif" alt="[   ]">\n'
        f'<a href="FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_00.fits">FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_01.fits</a> 21-May-2010 13:19  970K  <img src="/icons/unknown.gif" alt="[   ]">\n'
        "<hr>\n"
        "</pre>\n"
        "</body>\n"
        "</html>"
    )


@pytest.fixture
def fake_pointing_files(fake_fermi_week: int) -> list[PointingFile]:
    return [
        PointingFile(
            name=f"FERMI_POINTING_FINAL_{fake_fermi_week}_2025065_2025072_00.fits",
            fidelity="FINAL",
            week=fake_fermi_week,
            start="2025065",
            end="2025072",
            last_modified=datetime.strptime("19-May-2010 13:19", "%d-%b-%Y %H:%M"),
            rev=0,
        ),
        PointingFile(
            name=f"FERMI_POINTING_FINAL_{fake_fermi_week+1}_2025072_2025079_00.fits",
            fidelity="FINAL",
            week=fake_fermi_week + 1,
            start="2025072",
            end="2025079",
            last_modified=datetime.strptime("25-May-2010 13:19", "%d-%b-%Y %H:%M"),
            rev=0,
        ),
        PointingFile(
            name=f"FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_00.fits",
            fidelity="PRELIM",
            week=fake_fermi_week + 3,
            start="2025086",
            end="2025093",
            last_modified=datetime.strptime("20-May-2010 13:19", "%d-%b-%Y %H:%M"),
            rev=0,
        ),
        PointingFile(
            name=f"FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_01.fits",
            fidelity="PRELIM",
            week=fake_fermi_week + 3,
            start="2025086",
            end="2025093",
            last_modified=datetime.strptime("21-May-2010 13:19", "%d-%b-%Y %H:%M"),
            rev=1,
        ),
    ]


@pytest.fixture
def fake_pointing_row() -> dict:
    return {
        "START": 762911940.0,
        "STOP": 762912000.0,
        "SC_POSITION": [6682914.0, -406353.65625, 1569664.875],
        "LAT_GEO": 13.414999961853027,
        "LON_GEO": -167.14100646972656,
        "RAD_GEO": 499774.0320751281,
        "RA_ZENITH": 356.5204162597656,
        "DEC_ZENITH": 13.194375038146973,
        "B_MCILWAIN": np.nan,
        "L_MCILWAIN": np.nan,
        "GEOMAG_LAT": np.nan,
        "IN_SAA": False,
        "RA_SCZ": 30.83741569519043,
        "DEC_SCZ": -26.251014709472656,
        "RA_SCX": 309.1402893066406,
        "DEC_SCX": 16.320697784423828,
        "RA_NPOLE": 115.71969604492188,
        "DEC_NPOLE": 64.471435546875,
        "ROCK_ANGLE": 51.66666793823242,
        "LAT_MODE": 5,
        "LAT_CONFIG": 1,
        "DATA_QUAL": 1,
        "LIVETIME": 55.800000000000004,
        "QSJ_1": -0.7956808314569652,
        "QSJ_2": 0.29672259378153204,
        "QSJ_3": -0.3302468528907897,
        "QSJ_4": 0.4120494302231729,
        "RA_SUN": 346.8106994628906,
        "DEC_SUN": -5.648721218109131,
    }


@pytest.fixture
def fake_hdu(fake_pointing_row):
    mock = MagicMock()
    mock.data = [fake_pointing_row]

    return mock


@pytest.fixture()
def mock_fits(monkeypatch: pytest.MonkeyPatch, fake_hdu):
    mock = MagicMock()
    mock.open = MagicMock(return_value=[None, fake_hdu])

    monkeypatch.setattr(fits, "open", mock.open)

    return mock


@pytest.fixture(autouse=True)
def mock_httpx(
    monkeypatch: pytest.MonkeyPatch, fake_fermi_html: str
) -> Generator[MagicMock]:
    mock = MagicMock(return_value=Response(status_code=200, text=fake_fermi_html))
    monkeypatch.setattr(httpx, "get", mock)
    return mock


@pytest.fixture(autouse=True)
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock]:
    mock = MagicMock()
    monkeypatch.setattr(lat_planned, "logger", mock)
    return mock


@pytest.fixture
def fake_schedule_create(
    fake_fermi_telescope: sdk.Telescope, fake_fermi_week: int
) -> sdk.ScheduleCreate:
    return sdk.ScheduleCreate(
        telescope_id=fake_fermi_telescope.id,
        name=f"fermi_lat_week_{fake_fermi_week}",
        date_range=sdk.DateRange(
            begin=datetime.now(),
            end=datetime.now() + timedelta(days=7),
        ),
        status=sdk.ScheduleStatus.PLANNED,
        fidelity=sdk.ScheduleFidelity.LOW,
        observations=[],
    )


@pytest.fixture
def fake_observation_create(
    fake_fermi_telescope: sdk.Telescope, fake_fermi_week: int
) -> sdk.ObservationCreate:
    id = (
        fake_fermi_telescope.instruments[0].id
        if fake_fermi_telescope.instruments is not None
        else ""
    )

    return sdk.ObservationCreate(
        instrument_id=id,
        object_name=f"fermi_week_{fake_fermi_week}_observation_0",
        pointing_position=sdk.Coordinate(
            ra=30.83741569519043,
            dec=-26.251014709472656,
        ),
        date_range=sdk.DateRange(
            begin=datetime.fromisoformat("2025-03-05T23:58:55"),
            end=datetime.fromisoformat("2025-3-5T23:59:55"),
        ),
        external_observation_id=f"fermi_week_{fake_fermi_week}_observation_0",
        type=sdk.ObservationType.IMAGING,
        status=sdk.ObservationStatus.PLANNED,
        pointing_angle=FERMI_LAT_POINTING_ANGLE,
        exposure_time=float(60),
        bandpass=sdk.Bandpass(
            sdk.EnergyBandpass(
                filter_name="fermi_lat",
                min=FERMI_LAT_MIN_ENERGY,
                max=FERMI_LAT_MAX_ENERGY,
                unit=sdk.EnergyUnit.GEV,
            )
        ),
    )


@pytest.fixture
def fake_pointing_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "RA_SCZ": 30,
                "DEC_SCZ": 60,
                "START": 762911940.0,
                "STOP": 762912000.0,
            },
            {
                "RA_SCZ": 34,
                "DEC_SCZ": 55,
                "START": 762912000.0,
                "STOP": 762912060.0,
            },
            {
                "RA_SCZ": 38,
                "DEC_SCZ": 53,
                "START": 762912060.0,
                "STOP": 762912120.0,
            },
        ]
    )


@pytest.fixture
def fake_fermi_telescope() -> sdk.Telescope:
    return sdk.Telescope(
        id="fermi_lat_telescope_uuid",
        name="Large Area Telescope",
        short_name="fermi_lat",
        created_on=datetime.now(),
        instruments=[
            sdk.TelescopeInstrument(
                id="fermi_lat_instrument_uuid",
                name="Large Area Telescope",
                short_name="LAT",
                created_on=datetime.now(),
            )
        ],
    )


@pytest.fixture(autouse=True)
def set_sdk_data(
    mock_telescope_api: MagicMock, fake_fermi_telescope: sdk.Telescope
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_fermi_telescope]
