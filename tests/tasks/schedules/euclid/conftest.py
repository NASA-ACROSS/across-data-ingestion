from unittest.mock import MagicMock

import pandas as pd
import pytest
import structlog


@pytest.fixture(autouse=True)
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.euclid.low_fidelity_planned.logger",
        mock,
    )

    return mock


@pytest.fixture(autouse=True)
def mock_util_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.euclid.util.logger",
        mock,
    )

    return mock


@pytest.fixture(autouse=True)
def mock_schedule_handler(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock_handler = MagicMock()
    mock_handler.run = MagicMock(return_value=None)

    mock = MagicMock(return_value=mock_handler)

    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.euclid.low_fidelity_planned.EuclidScheduleHandler",
        mock,
    )

    return mock_handler


@pytest.fixture()
def mock_retrieve_schedule_file(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(return_value="/fake_schedule_file_url")
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.euclid.low_fidelity_planned.retrieve_schedule_file",
        mock,
    )

    return mock


@pytest.fixture()
def fake_pointing_file_data() -> list[dict]:
    return [
        {
            "utc": "2024-06-01T12:00:00Z",
            "center_lon": 123.45,
            "center_lat": -54.321,
            "obs_id": "1",
            "obs_tag": "WIDE",
            "pos_angle": 45.0,
            "grism": "RED",
        },
    ]


@pytest.fixture()
def mock_parse_pointing_file(
    fake_pointing_file_data: list[dict], monkeypatch: pytest.MonkeyPatch
) -> MagicMock:
    mock = MagicMock(return_value=pd.DataFrame(fake_pointing_file_data))
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.euclid.low_fidelity_planned.parse_pointing_file",
        mock,
    )

    return mock


@pytest.fixture()
def fake_euclid_telescope_id() -> str:
    return "euclid_telescope_uuid"


@pytest.fixture()
def fake_vis_instrument_id() -> str:
    return "vis_instrument_uuid"


@pytest.fixture()
def fake_nisp_instrument_id() -> str:
    return "nisp_instrument_uuid"


@pytest.fixture()
def fake_pointing_file() -> str:
    return """
        #
        START                         8810.00043  2024-02-14T00:00:00Z                                   +0.000000 +0.000000 -0.269745 +0.962932  254.351 +90.000  +105.649
        SLEW     LARGE                8810.00043  2024-02-14T00:00:00Z  1805         146.9   1.542   0.020
        POINTING WIDE                 8810.02132  2024-02-14T00:30:05Z       996      1       1  0      1  -0.898582 +0.333443 -0.087653 +0.271445   51.745 -56.853  -92.463   +89.377  -4.481    52.430 -57.224  52.355 -56.448  51.075 -56.478  51.122 -57.255  0.47968 RED
        """


@pytest.fixture()
def fake_pointing_file_bad_data() -> str:
    return """
        #
        START                         8810.00043  2024-02-14T00:00:00Z                                   +0.000000 +0.000000 -0.269745 +0.962932  254.351 +90.000  +105.649
        SLEW     LARGE                8810.00043  2024-02-14T00:00:00Z  1805         146.9   1.542   0.020
        POINTING WIDE                 8810.02132  2024-02-14T00:30:05Z       996      1       1  0      1  -0.898582 +0.333443 -0.087653 +0.271445   51.745 -56.853  -92.463   +89.377  -4.481    52.430 -57.224  52.355 -56.448  51.075 -56.478  51.122 -57.255
        """
