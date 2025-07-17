from unittest.mock import AsyncMock

import pytest
from astropy.table import Table  # type: ignore[import-untyped]


@pytest.fixture
def mock_telescope_id() -> str:
    return "chandra-mock-telescope-id"


@pytest.fixture
def mock_instrument_id() -> str:
    return "acis-mock-id"


@pytest.fixture
def mock_telescope_get(mock_telescope_id, mock_instrument_id):
    return [
        {
            "id": mock_telescope_id,
            "instruments": [
                {
                    "id": mock_instrument_id,
                    "name": "Advanced CCD Imaging Spectrometer",
                },
            ],
        }
    ]


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
def mock_query_vo_service(mock_observation_table: dict) -> AsyncMock:
    return AsyncMock(return_value=mock_observation_table)


@pytest.fixture
def mock_query_vo_service_for_exposure_times(mock_observation_table: dict) -> AsyncMock:
    """
    Return the mock observation table the first time query is called
    and None the second time
    """
    return AsyncMock(side_effect=[mock_observation_table, None])


@pytest.fixture
def mock_instrument_info() -> list[dict]:
    return [
        {
            "name": "Advanced CCD Imaging Spectrometer",
            "id": "acis-mock-id",
        },
        {
            "name": "Advanced CCD Imaging Spectrometer - High Energy Transmission Grating",
            "id": "acis-hetg-mock-id",
        },
        {
            "name": "Advanced CCD Imaging Spectrometer - Low Energy Transmission Grating",
            "id": "acis-letg-mock-id",
        },
        {
            "name": "Advanced CCD Imaging Spectrometer - Continuous Clocking Mode",
            "id": "acis-cc-mock-id",
        },
        {
            "name": "High Resolution Camera",
            "id": "hrc-mock-id",
        },
        {
            "name": "High Resolution Camera - High Energy Transmission Grating",
            "id": "hrc-hetg-mock-id",
        },
        {
            "name": "High Resolution Camera - Low Energy Transmission Grating",
            "id": "hrc-letg-mock-id",
        },
        {
            "name": "High Resolution Camera - Timing Mode",
            "id": "hrc-timing-mock-id",
        },
    ]
