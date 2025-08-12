from collections.abc import Generator
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import structlog

from across_data_ingestion.util.across_server import sdk

from . import mocks


## SET DATA FROM TOP-LEVEL FIXTURES ##
@pytest.fixture(autouse=True)
def set_telescope(
    mock_telescope_api: MagicMock,
    fake_tess_telescope: sdk.Telescope,
    mock_instrument_api: MagicMock,
    fake_tess_instrument: sdk.Instrument,
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_tess_telescope]
    mock_instrument_api.get_instruments.return_value = [fake_tess_instrument]


## MOCK BEHAVIOR ##
@pytest.fixture
def mock_logger() -> Generator[MagicMock]:
    # must be patched because it is set at runtime when the file is imported.
    with patch(
        "across_data_ingestion.tasks.schedules.tess.low_fidelity_planned.logger",
        MagicMock(spec=structlog.stdlib.BoundLogger),
    ) as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def mock_pandas(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock]:
    mock = MagicMock()
    # Not mocking the default behavior
    mock.read_csv = MagicMock(side_effect=pd.read_csv)

    monkeypatch.setattr(pd, "read_csv", mock.read_csv)

    return mock


## FAKE DATA ##
@pytest.fixture
def fake_tess_telescope(fake_telescope: sdk.Telescope) -> sdk.Telescope:
    fake_telescope.id = "telescope_id"

    for i in fake_telescope.instruments or []:
        i.id = "instrument_id"

    return fake_telescope


@pytest.fixture
def fake_tess_instrument() -> sdk.Instrument:
    instrument = sdk.Instrument(
        id="instrument_id",
        telescope=sdk.IDNameSchema(
            id="telescope_id", name="TESS Telescope", short_name="TESS"
        ),
        created_on=datetime.now(),
        name="TESS instrument",
        short_name="FAKE_INSTRUMENT",
    )

    return instrument


@pytest.fixture
def fake_create_many_schedules(
    fake_tess_telescope: sdk.Telescope,
) -> dict[str, sdk.ScheduleCreateMany]:
    return {
        "placeholder_observations": sdk.ScheduleCreateMany(
            schedules=mocks.placeholder_observations.ACROSS_schedule_output.expected,
            telescope_id=fake_tess_telescope.id,
        ),
        "orbit_observations": sdk.ScheduleCreateMany(
            schedules=mocks.orbit_observations.ACROSS_schedule_output.expected,
            telescope_id=fake_tess_telescope.id,
        ),
    }
