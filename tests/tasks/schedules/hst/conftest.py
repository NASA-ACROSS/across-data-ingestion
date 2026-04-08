import os
from collections.abc import Generator
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import bs4
import httpx
import pandas as pd
import pytest
import structlog
from astropy.table import Table  # type: ignore[import-untyped]

import across_data_ingestion.tasks.schedules.hst.as_flown as as_flown_task
import across_data_ingestion.tasks.schedules.hst.low_fidelity_planned as task
from across_data_ingestion.tasks.schedules.hst.util import InstrumentInfo
from across_data_ingestion.util.across_server import sdk


@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # must be patched because it is set at runtime when the file is imported.
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)
    return mock


@pytest.fixture
def mock_as_flown_logger() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.hst.as_flown.logger"
    ) as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def set_sdk_data(
    mock_telescope_api: MagicMock,
    mock_instrument_api: MagicMock,
    fake_telescope: sdk.Telescope,
    fake_instrument: sdk.TelescopeInstrument,
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


@pytest.fixture
def mock_read_timeline_file(
    monkeypatch: pytest.MonkeyPatch,
    fake_timeline_file_df: pd.DataFrame,
) -> MagicMock:
    mock = MagicMock(return_value=fake_timeline_file_df)
    monkeypatch.setattr(task, "read_timeline_file", mock)
    return mock


@pytest.fixture
def fake_observed_observation_data() -> dict:
    """
    Mock a dictionary of as-flown observation params
    from the MAST TAP service
    """
    return {
        "sci_pep_id": 10,
        "sci_obset_id": 28845,
        "sci_targname": "test target",
        "sci_start_time": "2026-01-29T22:23:23",
        "sci_stop_time": "2026-01-29T23:23:23",
        "sci_ra": 39.96041666666667,
        "sci_dec": -1.5856000000000001,
        "sci_instrument_config": "WFC3/UVIS",
        "sci_spec_1234": "F606W",
        "sci_pa_aper": 90.0,
        "sci_operating_mode": "ACCUM",
        "sci_actual_duration": 3600,
    }


@pytest.fixture
def fake_observed_observation_table(
    fake_observed_observation_data: dict,
) -> Table:
    """
    Fixture representing a Table of as-flown observation params
    """
    return Table([fake_observed_observation_data])


@pytest.fixture
def fake_observed_observation_row(
    fake_observed_observation_table: Table,
) -> pd.Series:
    """
    Fixture to mock a single row (corresponding to one observation)
    from a Table of as-flown observations
    """
    return fake_observed_observation_table.to_pandas().iloc[0]


@pytest.fixture
def fake_invalid_observation_data() -> list[dict]:
    """Observation data with ACQ and BIAS obs"""
    return [
        {
            "sci_pep_id": 10,
            "sci_obset_id": 28845,
            "sci_targname": "BIAS",
            "sci_start_time": "2026-01-29T22:23:23",
            "sci_stop_time": "2026-01-29T23:23:23",
            "sci_ra": 39.96041666666667,
            "sci_dec": -1.5856000000000001,
            "sci_instrument_config": "WFC3/UVIS",
            "sci_spec_1234": "F606W",
            "sci_pa_aper": 90.0,
            "sci_operating_mode": "ACCUM",
            "sci_actual_duration": 3600,
        },
        {
            "sci_pep_id": 10,
            "sci_obset_id": 28845,
            "sci_targname": "Cas A",
            "sci_start_time": "2026-01-29T22:23:23",
            "sci_stop_time": "2026-01-29T23:23:23",
            "sci_ra": 39.96041666666667,
            "sci_dec": -1.5856000000000001,
            "sci_instrument_config": "WFC3/UVIS",
            "sci_spec_1234": "F606W",
            "sci_pa_aper": 90.0,
            "sci_operating_mode": "ACQ",
            "sci_actual_duration": 3600,
        },
    ]


@pytest.fixture
def fake_invalid_observation_table(
    fake_invalid_observation_data: dict,
) -> Table:
    """
    Fixture representing a Table of invalid
    as-flown observation params
    """
    return Table(fake_invalid_observation_data)


@pytest.fixture
def mock_vo_service_query(
    fake_observed_observation_table: Table,
) -> AsyncMock:
    mock = AsyncMock()
    mock.side_effect = [fake_observed_observation_table]

    return mock


@pytest.fixture
def mock_vo_service(mock_vo_service_query: AsyncMock) -> AsyncMock:
    mock_instance = AsyncMock()
    mock_instance.query = mock_vo_service_query
    # mock the context management so it actually returns the expected instance
    mock_instance.__aenter__.return_value = mock_instance

    return mock_instance


@pytest.fixture(autouse=True)
def mock_vo_service_cls(
    mock_vo_service: AsyncMock,
) -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.hst.as_flown.VOService",
        return_value=mock_vo_service,
    ) as mock_vo_service_cls:
        yield mock_vo_service_cls


@pytest.fixture
def mock_get_observation_data_from_tap(
    monkeypatch: pytest.MonkeyPatch,
    fake_observed_observation_table: Table,
) -> AsyncMock:
    mock = AsyncMock(return_value=fake_observed_observation_table)
    monkeypatch.setattr(as_flown_task, "get_observation_data_from_tap", mock)
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
            sdk.TelescopeInstrument(
                id="instrument_uuid",
                name="Wide Field Camera 3 - Infrared Channel",
                short_name="HST_WFC3_IR",
                created_on=datetime.now(),
            )
        ],
    )


@pytest.fixture
def fake_instrument(
    fake_instrument_id: str,
    fake_telescope: sdk.Telescope,
    fake_filters: list[sdk.Filter],
) -> sdk.TelescopeInstrument:
    return sdk.TelescopeInstrument(
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
        footprints=[],
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
            "date": "2025.209",
            "target_name": "FSR2007-0584",
            "mode": "ACQ",
        },
        {
            "date": "2025.209",
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
            "date": "2025.209",
            "begin_time": "01:07:54",
            "end_time": "02:03:30",
            "obs_id": "1791807",
            "PI": "Loriga",
            "exposure": "07-001",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F110W",
            "exp_time": 44.11,
            "ob": "07",
            "al": "01",
            "ex": "01",
        },
        {
            "date": "2025.209",
            "begin_time": "01:08:54",
            "end_time": "02:04:30",
            "obs_id": "1791900",
            "PI": "Loriga",
            "exposure": "07-002",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F160W",
            "exp_time": 41.17,
            "ob": "07",
            "al": "01",
            "ex": "02",
        },
        {
            "date": "2025.209",
            "begin_time": "01:10:54",
            "end_time": "02:06:30",
            "obs_id": "1791910",
            "PI": "Loriga",
            "exposure": "07-003",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F110W",
            "exp_time": 44.11,
            "ob": "07",
            "al": "03",
            "ex": "01",
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
        "date": "2025.209",
        "begin_time": "01:07:54",
        "end_time": "02:03:30",
        "obs_id": "1791807",
        "PI": "Loriga",
        "exposure": "07-001",
        "target_name": "FSR2007-0584",
        "instrument": "WFC3/IR",
        "mode": "MULTIA",
        "aperture": "IR-FIX",
        "element": "F110W",
        "exp_time": 44.11,
    }


@pytest.fixture
def fake_instrument_info(fake_instrument: sdk.TelescopeInstrument) -> InstrumentInfo:
    return InstrumentInfo(
        id=fake_instrument.id,
        bandpass=sdk.Bandpass(
            sdk.WavelengthBandpass(
                filter_name="fake filter",
                min=0,
                max=100,
                unit=sdk.WavelengthUnit.ANGSTROM,
            )
        ),
        type=sdk.ObservationType.IMAGING,
    )
