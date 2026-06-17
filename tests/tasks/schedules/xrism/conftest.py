from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
import structlog

from across_data_ingestion.util.across_server import sdk


@pytest.fixture(autouse=True)
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.xrism.low_fidelity_planned.logger",
        mock,
    )

    return mock


@pytest.fixture(autouse=True)
def mock_pandas_html(
    mock_pandas: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> MagicMock:
    mock_pandas.read_html = MagicMock(return_value=[])
    monkeypatch.setattr(pd, "read_html", mock_pandas.read_html)

    return mock_pandas


@pytest.fixture
def fake_short_term_schedule_table() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Mnv start time (UT)": ["2026-06-04T02:21:00"],
            "Seq": ["12345"],
            "TargetName": ["Cygnus X-1"],
            "Ra": [299.590],
            "Dec": [35.201],
        }
    )


@pytest.fixture
def fake_xrism_proposal_table() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "observation_id": ["12345"],
            "proposal_type": ["CAL"],
            "awarded_exposure": [50],
        }
    )


@pytest.fixture
def fake_xrism_too_table() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Seq": ["12345"],
            "Exp. (ks)": [50],
        }
    )


@pytest.fixture
def fake_observation_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "start_time": ["2026-06-04T02:21:00"],
            "observation_id": ["12345"],
            "TargetName": ["Cygnus X-1"],
            "Ra": [299.590],
            "Dec": [35.201],
            "exposure_time": [50],
        }
    )


@pytest.fixture(autouse=True)
def mock_schedule_handler(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock_handler = MagicMock()
    mock_handler.run = MagicMock(return_value=None)

    mock = MagicMock(return_value=mock_handler)

    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.xrism.low_fidelity_planned.XRISMScheduleHandler",
        mock,
    )

    return mock_handler


@pytest.fixture()
def mock_parse_short_term_schedule_page(
    fake_short_term_schedule_table: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> MagicMock:
    mock = MagicMock(return_value=fake_short_term_schedule_table)
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.xrism.low_fidelity_planned.parse_short_term_schedule_page",
        mock,
    )

    return mock


@pytest.fixture()
def mock_query_proposal_table(
    fake_xrism_proposal_table: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> MagicMock:
    mock = MagicMock(return_value=fake_xrism_proposal_table)
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.xrism.low_fidelity_planned.query_proposal_table",
        mock,
    )

    return mock


@pytest.fixture()
def mock_parse_too_table(
    fake_xrism_too_table: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> MagicMock:
    mock = MagicMock(return_value=fake_xrism_too_table)
    monkeypatch.setattr(
        "across_data_ingestion.tasks.schedules.xrism.low_fidelity_planned.parse_too_table",
        mock,
    )

    return mock


@pytest.fixture()
def fake_xrism_instrument() -> sdk.TelescopeInstrument:
    return sdk.TelescopeInstrument(
        id="resolve_instrument_uuid",
        name="Resolve",
        short_name="Resolve",
        created_on=datetime.now(),
        footprints=[],
    )


@pytest.fixture
def fake_xrism_telescope(
    fake_xrism_instrument: sdk.TelescopeInstrument,
) -> sdk.Telescope:
    return sdk.Telescope(
        id="xma_telescope_id",
        created_on=datetime.now(),
        name="XMA",
        short_name="XMA",
        instruments=[fake_xrism_instrument],
    )
