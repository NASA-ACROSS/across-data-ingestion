import os
from datetime import datetime
from unittest.mock import MagicMock

import bs4
import httpx
import pandas as pd
import pytest
import structlog

import across_data_ingestion.tasks.schedules.hst.low_fidelity_planned as task
from across_data_ingestion.util.across_server import sdk


@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # must be patched because it is set at runtime when the file is imported.
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)
    return mock


@pytest.fixture(autouse=True)
def set_sdk_data(
    mock_telescope_api: MagicMock,
    mock_instrument_api: MagicMock,
    fake_telescope: sdk.Telescope,
    fake_instrument: sdk.Instrument,
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_telescope]
    mock_instrument_api.get_instruments.return_value = [fake_instrument]


@pytest.fixture(autouse=True)
def set_httpx_get(
    mock_httpx_get: MagicMock,
    # fake_timeline_file_raw_data: str,
) -> None:
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.text = "some html"

    mock_httpx_get.return_value = mock_response


@pytest.fixture(autouse=True)
def set_mock_csv_files(monkeypatch: pytest.MonkeyPatch) -> None:
    def get_mock_path(file: str = "") -> str:
        return os.path.join(os.path.dirname(__file__), "mocks/", file)

    monkeypatch.setattr(
        task,
        "HST_EXPOSURE_CATALOG_URL",
        get_mock_path("mock_planned_exposure_catalog.csv"),
    )
    monkeypatch.setattr(
        task,
        "BASE_TIMELINE_URL",
        get_mock_path(),
    )


@pytest.fixture()
def mock_soup(fake_timeline_html_tags: list[MagicMock]) -> MagicMock:
    mock = MagicMock(spec=bs4.BeautifulSoup)
    mock.find_all.return_value = fake_timeline_html_tags
    return mock


@pytest.fixture(autouse=True)
def mock_soup_cls(monkeypatch: pytest.MonkeyPatch, mock_soup: MagicMock):
    mock_soup_cls = MagicMock(return_value=mock_soup)

    monkeypatch.setattr(bs4, "BeautifulSoup", mock_soup_cls)

    return mock_soup_cls


@pytest.fixture(autouse=True)
def mock_pandas(
    monkeypatch: pytest.MonkeyPatch,
    # fake_planned_exposure_catalog: pd.DataFrame,
    fake_timeline_file_df: pd.DataFrame,
) -> None:
    mock_read_csv = MagicMock(side_effect=pd.read_csv)
    mock_read_fwf = MagicMock(side_effect=pd.read_fwf)

    monkeypatch.setattr(pd, "read_csv", mock_read_csv)
    monkeypatch.setattr(pd, "read_fwf", mock_read_fwf)


@pytest.fixture
def mock_read_timeline_file(
    monkeypatch: pytest.MonkeyPatch,
    fake_timeline_file_df: pd.DataFrame,
) -> MagicMock:
    mock = MagicMock(return_value=fake_timeline_file_df)
    monkeypatch.setattr(task, "read_timeline_file", mock)
    return mock


@pytest.fixture
def fake_planned_exposure_catalog_df():
    rows = [
        {
            "object_name": "FSR2007-0584",
            "ra_h": "2",
            "ra_m": "27",
            "ra_s": "15.0",
            "dec_d": "61",
            "dec_m": "37",
            "dec_s": "28.0",
            "config": "WFC3/IR",
            "mode": "MULTIACCUM",
            "aper": "IR-FIX",
            "spec": "F110W",
            "wave": "0",
            "time": "-1",
            "prop": "17918",
            "cy": "32",
            "dataset": "PLANNED",
            "release": "---",
        },
        {
            "object_name": "FSR2007-0584",
            "ra_h": "2",
            "ra_m": "27",
            "ra_s": "15.0",
            "dec_d": "61",
            "dec_m": "37",
            "dec_s": "28.0",
            "config": "WFC3/IR",
            "mode": "MULTIACCUM",
            "aper": "IR-FIX",
            "spec": "F160W",
            "wave": "0",
            "time": "-1",
            "prop": "17918",
            "cy": "32",
            "dataset": "PLANNED",
            "release": "---",
        },
    ]

    return pd.DataFrame(rows)


@pytest.fixture
def fake_instrument_id() -> str:
    return "fake_instrument_id"


@pytest.fixture
def fake_telescope() -> sdk.Telescope:
    return sdk.Telescope(
        id="telescope_uuid",
        name="Hubble Space Telescope",
        short_name="hst",
        created_on=datetime.now(),
        instruments=[
            sdk.IDNameSchema(
                id="instrument_uuid",
                name="Wide Field Camera 3 - Infrared Channel",
                short_name="HST_WFC3_IR",
            )
        ],
    )


@pytest.fixture
def fake_instrument(
    fake_instrument_id: str,
    fake_telescope: sdk.Telescope,
    fake_filters: list[sdk.Filter],
) -> sdk.Instrument:
    return sdk.Instrument(
        id=fake_instrument_id,
        name="FAKE HST INSTRUMENT",
        short_name="HST_FAKE",
        created_on=datetime.now(),
        filters=fake_filters,
        telescope=sdk.IDNameSchema(
            id=fake_telescope.id,
            name=fake_telescope.name,
            short_name=fake_telescope.short_name,
        ),
    )


@pytest.fixture
def fake_filters(fake_instrument_id: str) -> list[sdk.Filter]:
    filters = [
        {
            "id": "fake_filter_id_1",
            "name": "FAKE HST FILTER",
            "short_name": "HST FAKE ABCD",
            "min_wavelength": 7103.999999999999,
            "max_wavelength": 15963.999999999996,
            "instrument_id": fake_instrument_id,
            "created_on": datetime.now(),
            "peak_wavelength": None,
            "reference_url": None,
            "sensitivity_depth": None,
            "sensitivity_depth_unit": None,
            "sensitivity_time_seconds": None,
            "is_operational": True,
        },
        {
            "id": "fake_filter_id_2",
            "name": "FAKE HST FILTER 2",
            "short_name": "HST FAKE WXYZ",
            "min_wavelength": 12685.999999999998,
            "max_wavelength": 18051.999999999996,
            "instrument_id": fake_instrument_id,
            "created_on": datetime.now(),
            "peak_wavelength": None,
            "reference_url": None,
            "sensitivity_depth": None,
            "sensitivity_depth_unit": None,
            "sensitivity_time_seconds": None,
            "is_operational": True,
        },
    ]
    return [sdk.Filter.model_validate(f) for f in filters]


@pytest.fixture
def fake_invalid_obs_timeline_file_df() -> pd.DataFrame:
    raw_observations = [
        {
            "date": 2025.209,
            "target_name": "FSR2007-0584",
            "mode": "ACQ",
        },
        {
            "date": 2025.209,
            "target_name": "BIAS",
            "mode": "MULTIA",
        },
    ]
    schedules = pd.DataFrame(raw_observations)
    return schedules


@pytest.fixture
def fake_timeline_file_df() -> pd.DataFrame:
    raw_observations = [
        {
            "date": 2025.209,
            "begin_time": "01:07:54",
            "end_time": "02:03:30",
            "obs_id": 1791807,
            "PI": "Loriga",
            "exposure": "07-001",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F110W",
            "exp_time": 44.11,
            "ob": 7,
            "al": 1,
            "ex": 1,
        },
        {
            "date": 2025.209,
            "begin_time": "01:07:54",
            "end_time": "02:03:30",
            "obs_id": 1791807,
            "PI": "Loriga",
            "exposure": "07-002",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F160W",
            "exp_time": 41.17,
            "ob": 7,
            "al": 1,
            "ex": 2,
        },
        {
            "date": 2025.209,
            "begin_time": "01:07:54",
            "end_time": "02:03:30",
            "obs_id": 1791807,
            "PI": "Loriga",
            "exposure": "07-003",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F110W",
            "exp_time": 44.11,
            "ob": 7,
            "al": 3,
            "ex": 1,
        },
    ]
    schedules = pd.DataFrame(raw_observations)
    return schedules


@pytest.fixture
def fake_timeline_html_tags() -> list[MagicMock]:
    dates = [
        "timeline_01_01_01",
        "timeline_01_01_25",
        "timeline_07_28_25",
    ]

    mocks = []

    for date in dates:
        mock = MagicMock(spec=bs4.Tag)
        mock.get = MagicMock(return_value=date)

        mocks.append(mock)

    return mocks


@pytest.fixture
def fake_timeline_row() -> dict:
    return {
        "date": 2025.209,
        "begin_time": "01:07:54",
        "end_time": "02:03:30",
        "obs_id": 1791807,
        "PI": "Loriga",
        "exposure": "07-001",
        "target_name": "FSR2007-0584",
        "instrument": "WFC3/IR",
        "mode": "MULTIA",
        "aperture": "IR-FIX",
        "element": "F110W",
        "exp_time": 44.11,
    }
