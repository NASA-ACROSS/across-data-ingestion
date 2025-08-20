from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_telescope_get() -> list:
    return [
        {
            "id": "f2ae30ec-cd64-41b1-a951-4da29aa9f4ab",
            "instruments": [
                {
                    "id": "ab55be45-9796-40da-8125-e512f446ac96",
                    "name": "European Photon Imaging Camera - MOS",
                    "short_name": "EPIC-MOS",
                },
                {
                    "id": "b9c82f10-1292-462b-85b3-1c1c2292806c",
                    "name": "European Photon Imaging Camera - pn",
                    "short_name": "EPIC-PN",
                },
                {
                    "id": "eb40e1c6-6c03-4474-9462-73d273efa3ac",
                    "name": "Reflection Grating Spectrometer",
                    "short_name": "RGS",
                },
                {
                    "id": "e523b7de-8ea0-46eb-ba42-dff6f336130c",
                    "name": "Optical Monitor",
                    "short_name": "OM",
                },
            ],
        },
    ]


@pytest.fixture
def mock_log() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.xmm_newton.low_fidelity_planned.logger"
    ) as log_mock:
        yield log_mock


@pytest.fixture
def mock_planned_schedule_table() -> pd.DataFrame:
    return pd.read_csv(
        "tests/tasks/schedules/xmm_newton/mocks/planned_schedule_table.csv"
    )


@pytest.fixture
def mock_revolution_timeline_file() -> pd.DataFrame:
    return pd.read_csv(
        "tests/tasks/schedules/xmm_newton/mocks/revolution_timeline_file.csv"
    )


@pytest.fixture(autouse=True)
def mock_read_planned_schedule_table(
    mock_planned_schedule_table: pd.DataFrame,
) -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.xmm_newton.low_fidelity_planned.read_planned_schedule_table",
        return_value=mock_planned_schedule_table,
    ) as mock:
        yield mock


@pytest.fixture(autouse=True)
def mock_read_revolution_timeline_file(
    mock_revolution_timeline_file: pd.DataFrame,
) -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.xmm_newton.low_fidelity_planned.read_revolution_timeline_file",
        return_value=mock_revolution_timeline_file,
    ) as mock:
        yield mock
