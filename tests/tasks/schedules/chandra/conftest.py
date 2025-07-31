from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from astropy.table import Table  # type: ignore[import-untyped]


@pytest.fixture
def mock_telescope_id() -> str:
    return "chandra-mock-telescope-id"


@pytest.fixture
def mock_instrument_id() -> str:
    return "acis-mock-id"


@pytest.fixture
def mock_telescope_get(mock_telescope_id, mock_instrument_id) -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.util.across_api.telescope.get",
        return_value=[
            {
                "id": mock_telescope_id,
                "instruments": [
                    {
                        "id": mock_instrument_id,
                        "name": "Advanced CCD Imaging Spectrometer",
                        "short_name": "ACIS",
                    },
                ],
            }
        ],
    ) as mock_telescope_get:
        yield mock_telescope_get


@pytest.fixture
def mock_schedule_post() -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.util.across_api.schedule.post", return_value=None
    ) as mock_schedule_post:
        yield mock_schedule_post


@pytest.fixture
def mock_logger() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
    ) as log_mock:
        yield log_mock


@pytest.fixture
def mock_observation_data() -> dict:
    return {
        "target_name": "Abell 370",
        "ra": "39.96041666666667",
        "dec": "-1.5856000000000001",
        "start_date": "2025-06-30T22:23:23",
        "instrument": "ACIS-I",
        "grating": "NONE",
        "exposure_mode": "NONE",
        "obsid": "28845",
        "obs_id": "28845",
        "t_plan_exptime": 20000.0,
    }


@pytest.fixture
def mock_observation_table(mock_observation_data: dict) -> Table:
    return Table([mock_observation_data])


@pytest.fixture
def mock_query_vo_service(mock_observation_table: dict) -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService.query",
        return_value=mock_observation_table,
    ) as mock_vo_service:
        yield mock_vo_service


@pytest.fixture
def mock_ingest() -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.ingest",
    ) as mock_ingest:
        yield mock_ingest


@pytest.fixture
def mock_get_instrument_info_from_obs() -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.get_instrument_info_from_observation",
    ) as mock_get_instrument_info_from_obs:
        yield mock_get_instrument_info_from_obs


@pytest.fixture
def mock_instrument_info() -> list[dict]:
    return [
        {
            "short_name": "ACIS",
            "id": "acis-mock-id",
        },
        {
            "short_name": "ACIS-HETG",
            "id": "acis-hetg-mock-id",
        },
        {
            "short_name": "ACIS-LETG",
            "id": "acis-letg-mock-id",
        },
        {
            "short_name": "ACIS-CC",
            "id": "acis-cc-mock-id",
        },
        {
            "short_name": "HRC",
            "id": "hrc-mock-id",
        },
        {
            "short_name": "HRC-HETG",
            "id": "hrc-hetg-mock-id",
        },
        {
            "short_name": "HRC-LETG",
            "id": "hrc-letg-mock-id",
        },
        {
            "short_name": "HRC-Timing",
            "id": "hrc-timing-mock-id",
        },
    ]
