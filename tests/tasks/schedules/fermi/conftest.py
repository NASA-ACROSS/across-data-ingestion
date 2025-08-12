import os
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Protocol
from unittest.mock import MagicMock, patch

import pytest
from httpx import Response

from across_data_ingestion.tasks.schedules.fermi.lat_planned import (
    FERMI_LAT_MAX_ENERGY,
    FERMI_LAT_MIN_ENERGY,
    FERMI_LAT_POINTING_ANGLE,
    ScheduleFile,
)
from across_data_ingestion.util.across_server import sdk

MOCK_FILE_BASE_PATH = os.path.join(os.path.dirname(__file__), "mocks/")


@pytest.fixture(autouse=True)
def patch_current_time() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_current_time",
        return_value=datetime(2025, 3, 28, 0, 0, 0).isoformat(),
    ) as mock:
        yield mock


@pytest.fixture
def mock_base_path() -> str:
    return MOCK_FILE_BASE_PATH


class BuildPathProto(Protocol):
    def __call__(self, name: str) -> str: ...


@pytest.fixture
def build_path() -> BuildPathProto:
    def _filename(name: str) -> str:
        return f"{MOCK_FILE_BASE_PATH}{name}"

    return _filename


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
        f'<a href="FERMI_POINTING_FINAL_{fake_fermi_week+1}_2025072_2025079_00.fits">FERMI_POINTING_FINAL_{fake_fermi_week+1}_2025072_2025079_00.fits</a> 19-May-2010 13:19  970K  <img src="/icons/unknown.gif" alt="[   ]">\n'
        f'<a href="FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_00.fits">FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_00.fits</a> 19-May-2010 13:19  970K  <img src="/icons/unknown.gif" alt="[   ]">\n'
        "<hr>\n"
        "</pre>\n"
        "</body>\n"
        "</html>"
    )


@pytest.fixture
def fake_schedule_files(fake_fermi_week: int) -> list[ScheduleFile]:
    return [
        ScheduleFile(
            name=f"FERMI_POINTING_FINAL_{fake_fermi_week}_2025065_2025072_00.fits",
            fidelity="FINAL",
            week=fake_fermi_week,
            start="2025065",
            end="2025072",
        ),
        ScheduleFile(
            name=f"FERMI_POINTING_FINAL_{fake_fermi_week+1}_2025072_2025079_00.fits",
            fidelity="FINAL",
            week=fake_fermi_week,
            start="2025072",
            end="2025079",
        ),
        ScheduleFile(
            name=f"FERMI_POINTING_PRELIM_{fake_fermi_week+3}_2025086_2025093_00.fits",
            fidelity="PRELIM",
            week=fake_fermi_week + 3,
            start="2025086",
            end="2025093",
        ),
    ]


@pytest.fixture(autouse=True)
def mock_httpx(fake_fermi_html: str) -> Generator[MagicMock]:
    with patch(
        "httpx.get",
        return_value=Response(
            status_code=200,
            text=fake_fermi_html,
        ),
    ) as mock:
        yield mock


@pytest.fixture(autouse=True)
def mock_logger() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.fermi.lat_planned.logger"
    ) as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def patch_sdk(
    mock_telescope_api_cls: MagicMock, mock_schedule_api_cls: MagicMock
) -> Generator[None]:
    # Only patch the attributes you want
    with (
        patch.object(sdk, "TelescopeApi", mock_telescope_api_cls),
        patch.object(sdk, "ScheduleApi", mock_schedule_api_cls),
    ):
        yield


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
        else "id"
    )

    return sdk.ObservationCreate(
        instrument_id=id,
        object_name=f"fermi_week_{fake_fermi_week}_observation_0",
        pointing_position=sdk.Coordinate(
            ra=90,
            dec=90,
        ),
        date_range=sdk.DateRange(
            begin=datetime.now(),
            end=datetime.now() + timedelta(seconds=100),
        ),
        external_observation_id=f"fermi_week_{fake_fermi_week}_observation_0",
        type=sdk.ObservationType.IMAGING,
        status=sdk.ObservationStatus.PLANNED,
        pointing_angle=FERMI_LAT_POINTING_ANGLE,
        exposure_time=float(100),
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
def fake_fermi_telescope() -> sdk.Telescope:
    return sdk.Telescope(
        id="fermi_lat_telescope_uuid",
        name="Large Area Telescope",
        short_name="fermi_lat",
        created_on=datetime.now(),
        instruments=[
            sdk.IDNameSchema(
                id="fermi_lat_instrument_uuid",
                name="Large Area Telescope",
                short_name="LAT",
            )
        ],
    )


@pytest.fixture(autouse=True)
def fermi_data_sdk(
    mock_telescope_api: MagicMock, fake_fermi_telescope: sdk.Telescope
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_fermi_telescope]
