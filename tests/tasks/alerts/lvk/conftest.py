from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
import structlog

import across_data_ingestion.tasks.alerts.lvk.util as task
from across_data_ingestion.util.across_server import sdk


@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # must be patched because it is set at runtime when the file is imported.
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)
    return mock


@pytest.fixture
def fake_broker_received_on() -> datetime:
    return datetime(2026, 1, 1, tzinfo=task.UTC)


@pytest.fixture(autouse=True)
def mock_datetime(
    monkeypatch: pytest.MonkeyPatch, fake_broker_received_on: datetime
) -> MagicMock:
    mock = MagicMock(wraps=datetime)
    mock.now = MagicMock(return_value=fake_broker_received_on)
    monkeypatch.setattr(task, "datetime", mock)
    return mock


@pytest.fixture
def fake_alert() -> dict[str, Any]:
    return {
        "alert_type": "preliminary",
        "superevent_id": "S240101a",
        "event": {
            "time": "2026-01-01T00:00:00Z",
            "skymap": "ZmFrZS1za3ltYXAtYnl0ZXM=",
        },
    }


@pytest.fixture
def fake_localizations() -> list[sdk.LocalizationCreate]:
    return [
        sdk.LocalizationCreate(
            contours=[
                sdk.LocalizationContourBase(
                    contour=[sdk.Point(x=1.0, y=2.0), sdk.Point(x=3.0, y=4.0)]
                )
            ],
            probability_enclosed=0.5,
        ),
        sdk.LocalizationCreate(
            contours=[
                sdk.LocalizationContourBase(
                    contour=[sdk.Point(x=5.0, y=6.0), sdk.Point(x=7.0, y=8.0)]
                )
            ],
            probability_enclosed=0.9,
        ),
    ]


@pytest.fixture(autouse=True)
def mock_process_contours(
    monkeypatch: pytest.MonkeyPatch, fake_localizations: list[sdk.LocalizationCreate]
) -> MagicMock:
    mock = MagicMock(return_value=fake_localizations)
    monkeypatch.setattr(task, "process_contours", mock)
    return mock
