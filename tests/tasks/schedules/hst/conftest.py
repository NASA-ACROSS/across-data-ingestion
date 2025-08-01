from collections.abc import Generator
from io import StringIO
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_log() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.hst.low_fidelity_planned.logger"
    ) as log_mock:
        yield log_mock


@pytest.fixture
def mock_soup(
    mock_timeline_html_tags: list, mock_timeline_file_raw_data: str
) -> Generator[MagicMock]:
    mock_soup = MagicMock()
    mock_soup.find_all.return_value = mock_timeline_html_tags
    mock_soup.text = mock_timeline_file_raw_data
    with patch(
        "across_data_ingestion.tasks.schedules.hst.low_fidelity_planned.BeautifulSoup",
        return_value=mock_soup,
    ) as mock_soup:
        yield mock_soup


@pytest.fixture
def mock_get() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.hst.low_fidelity_planned.httpx.get"
    ) as mock_get:
        mock_get.text = "mock response text"
        yield mock_get


@pytest.fixture
def mock_pandas_read_csv(
    mock_planned_exposure_catalog: pd.DataFrame,
) -> Generator[MagicMock]:
    with patch(
        "pandas.read_csv", return_value=mock_planned_exposure_catalog
    ) as mock_read_csv:
        yield mock_read_csv


@pytest.fixture
def mock_telescope_id() -> str:
    return "hst-mock-telescope-id"


@pytest.fixture
def mock_instrument_id() -> str:
    return "hst-wfc3-ir-mock-id"


@pytest.fixture
def mock_telescope_get(mock_telescope_id: str, mock_instrument_id: str) -> list:
    return [
        {
            "id": mock_telescope_id,
            "instruments": [
                {
                    "id": mock_instrument_id,
                    "name": "Wide Field Camera 3 - Infrared Channel",
                    "short_name": "HST_WFC3_IR",
                },
            ],
        },
    ]


@pytest.fixture
def mock_instrument_get(mock_telescope_id: str, mock_instrument_id: str) -> list:
    return [
        {
            "id": mock_instrument_id,
            "name": "Wide Field Camera 3 - Infrared Channel",
            "short_name": "HST_WFC3_IR",
            "telescope": [
                {
                    "id": mock_telescope_id,
                },
            ],
            "filters": [
                {
                    "id": "mock-wfc3-F110W-id",
                    "name": "HST WFC3 F110W",
                    "min_wavelength": 7103.999999999999,
                    "max_wavelength": 15963.999999999996,
                    "instrument_id": mock_instrument_id,
                },
                {
                    "id": "mock-wfc3-F160W-id",
                    "name": "HST WFC3 F160W",
                    "min_wavelength": 12685.999999999998,
                    "max_wavelength": 18051.999999999996,
                    "instrument_id": mock_instrument_id,
                },
            ],
        }
    ]


@pytest.fixture
def mock_schedule_post() -> Generator:
    yield Mock(return_value=None)


@pytest.fixture
def mock_read_planned_exposure_catalog(
    mock_planned_exposure_catalog: pd.DataFrame,
) -> Generator:
    with patch(
        "across_data_ingestion.tasks.schedules.hst.low_fidelity_planned.read_planned_exposure_catalog",
        return_value=mock_planned_exposure_catalog,
    ) as mock_read_exposure_catalog:
        yield mock_read_exposure_catalog


@pytest.fixture
def mock_read_timeline_file(mock_timeline_file_dataframe: pd.DataFrame) -> Generator:
    with patch(
        "across_data_ingestion.tasks.schedules.hst.low_fidelity_planned.read_timeline_file",
        return_value=mock_timeline_file_dataframe,
    ) as mock_read_timeline:
        yield mock_read_timeline


@pytest.fixture
def mock_get_latest_timeline_file() -> Generator:
    with patch(
        "across_data_ingestion.tasks.schedules.hst.low_fidelity_planned.get_latest_timeline_file",
        return_value="timeline_07_28_25",
    ) as mock_latest_timeline_file:
        yield mock_latest_timeline_file


@pytest.fixture
def mock_planned_exposure_catalog() -> pd.DataFrame:
    colnames = [
        "object_name",
        "ra_h",
        "ra_m",
        "ra_s",
        "dec_d",
        "dec_m",
        "dec_s",
        "config",
        "mode",
        "aper",
        "spec",
        "wave",
        "time",
        "prop",
        "cy",
        "dataset",
        "release",
    ]
    s = (
        "FSR2007-0584     02 27 15.00   +61 37 28.0 WFC3/IR   MULTIACCUM  IR-FIX       F110W               0    -1 17918 32 PLANNED        ---\n"
        "FSR2007-0584     02 27 15.00   +61 37 28.0 WFC3/IR   MULTIACCUM  IR-FIX       F160W               0    -1 17918 32 PLANNED        ---"
    )
    planned_exposure_catalog = pd.read_csv(StringIO(s), sep="\s+", names=colnames)
    return planned_exposure_catalog


@pytest.fixture
def mock_timeline_file_raw_data() -> str:
    return (
        "2025.209 01:07:54 02:03:30  1791807  Loriga     07-001 FSR2007-0584                   WFC3/IR  MULTIA IR-FIX       F110W           44.11  07 01 01   \n"
        "2025.209 01:07:54 02:03:30  1791807  Loriga     07-002 FSR2007-0584                   WFC3/IR  MULTIA IR-FIX       F160W           41.17  07 01 02  \n"
        "2025.209 01:07:54 02:03:30  1791807  Loriga     07-003 FSR2007-0584                   WFC3/IR  MULTIA IR-FIX       F110W           44.11  07 03 01  \n"
    )


@pytest.fixture
def mock_timeline_file_dataframe() -> pd.DataFrame:
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
            "exp_time": "44.11",
        },
        {
            "date": "2025.209",
            "begin_time": "01:07:54",
            "end_time": "02:03:30",
            "obs_id": "1791807",
            "PI": "Loriga",
            "exposure": "07-002",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F160W",
            "exp_time": "41.17",
        },
        {
            "date": "2025.209",
            "begin_time": "01:07:54",
            "end_time": "02:03:30",
            "obs_id": "1791807",
            "PI": "Loriga",
            "exposure": "07-003",
            "target_name": "FSR2007-0584",
            "instrument": "WFC3/IR",
            "mode": "MULTIA",
            "aperture": "IR-FIX",
            "element": "F110W",
            "exp_time": "44.11",
        },
    ]
    schedules = pd.DataFrame(raw_observations)
    return schedules


@pytest.fixture
def mock_timeline_html_tags() -> list:
    class MockATag:
        def __init__(self, href: str) -> None:
            self.href = href

        def get(self, key: str) -> str:
            return self.href

    return [
        MockATag("timeline_01_01_01"),
        MockATag("timeline_01_01_25"),
        MockATag("timeline_07_28_25"),
    ]
