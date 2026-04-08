from collections.abc import Generator
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import structlog

from across_data_ingestion.util.across_server import sdk


## SET DATA FROM TOP-LEVEL FIXTURES ##
@pytest.fixture(autouse=True)
def set_telescope(
    mock_telescope_api: MagicMock,
    fake_rubin_telescope: sdk.Telescope,
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_rubin_telescope]


## MOCK BEHAVIOR ##
@pytest.fixture
def mock_logger() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.rubin.low_fidelity_planned.logger",
        MagicMock(spec=structlog.stdlib.BoundLogger),
    ) as mock_logger:
        yield mock_logger


## FAKE DATA ##
@pytest.fixture
def fake_rubin_telescope(
    fake_telescope: sdk.Telescope, fake_rubin_instruments: list[sdk.TelescopeInstrument]
) -> sdk.Telescope:
    fake_telescope.id = "lsst_telescope_id"

    fake_telescope.instruments = fake_rubin_instruments
    return fake_telescope


@pytest.fixture
def fake_rubin_instruments() -> list[sdk.TelescopeInstrument]:
    return [
        sdk.TelescopeInstrument(
            id="lsst_instrument_id",
            name="LSST Camera",
            short_name="LSST_CAM",
            created_on=datetime.now(),
            footprints=[],
        )
    ]
