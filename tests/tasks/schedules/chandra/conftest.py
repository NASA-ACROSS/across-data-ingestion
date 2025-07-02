from unittest.mock import AsyncMock, Mock

import pytest
from astropy.table import Row, Table  # type: ignore[import-untyped]


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
def mock_query_vo_service(mock_observation_data: dict) -> AsyncMock:
    return AsyncMock(return_value=mock_observation_data)


@pytest.fixture
def mock_format_vo_table(mock_observation_table: Table) -> Mock:
    return Mock(return_value=mock_observation_table)


@pytest.fixture
def mock_observation_configuration_table() -> Table:
    return Table(
        [
            {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "NONE"},
            {"instrument": "ACIS", "grating": "HETG", "exposure_mode": "NONE"},
            {"instrument": "ACIS", "grating": "LETG", "exposure_mode": "NONE"},
            {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "CC"},
            {"instrument": "HRC", "grating": "NONE", "exposure_mode": ""},
            {"instrument": "HRC", "grating": "HETG", "exposure_mode": ""},
            {"instrument": "HRC", "grating": "LETG", "exposure_mode": ""},
            {"instrument": "HRC", "grating": "NONE", "exposure_mode": "TIMING"},
            {"instrument": "ACIS", "grating": "BADGRATING", "exposure_mode": ""},
            {"instrument": "HRC", "grating": "BADGRATING", "exposure_mode": ""},
            {"instrument": "BADINSTRUMENT", "grating": "", "exposure_mode": ""},
        ]
    )


@pytest.fixture
def mock_acis_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[0]


@pytest.fixture
def mock_acis_hetg_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[1]


@pytest.fixture
def mock_acis_letg_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[2]


@pytest.fixture
def mock_acis_cc_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[3]


@pytest.fixture
def mock_hrc_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[4]


@pytest.fixture
def mock_hrc_hetg_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[5]


@pytest.fixture
def mock_hrc_letg_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[6]


@pytest.fixture
def mock_hrc_timing_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[7]


@pytest.fixture
def mock_bad_acis_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[8]


@pytest.fixture
def mock_bad_hrc_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[9]


@pytest.fixture
def mock_bad_instrument_observation_row(mock_observation_configuration_table) -> Row:
    return mock_observation_configuration_table[10]


@pytest.fixture
def mock_instrument_info() -> dict:
    return {
        "Advanced CCD Imaging Spectrometer": "acis-mock-id",
        "Advanced CCD Imaging Spectrometer - High Energy Transmission Grating": "acis-hetg-mock-id",
        "Advanced CCD Imaging Spectrometer - Low Energy Transmission Grating": "acis-letg-mock-id",
        "Advanced CCD Imaging Spectrometer - Continuous Clocking Mode": "acis-cc-mock-id",
        "High Resolution Camera": "hrc-mock-id",
        "High Resolution Camera - High Energy Transmission Grating": "hrc-hetg-mock-id",
        "High Resolution Camera - Low Energy Transmission Grating": "hrc-letg-mock-id",
        "High Resolution Camera - Timing Mode": "hrc-timing-mock-id",
    }
