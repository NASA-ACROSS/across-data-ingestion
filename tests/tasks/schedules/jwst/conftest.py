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
    fake_jwst_telescope: sdk.Telescope,
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_jwst_telescope]


## MOCK BEHAVIOR ##
@pytest.fixture
def mock_logger() -> Generator[MagicMock]:
    # must be patched because it is set at runtime when the file is imported.
    with patch(
        "across_data_ingestion.tasks.schedules.jwst.low_fidelity_planned.logger",
        MagicMock(spec=structlog.stdlib.BoundLogger),
    ) as mock_logger:
        yield mock_logger


## FAKE DATA ##
@pytest.fixture
def fake_jwst_telescope(
    fake_telescope: sdk.Telescope, fake_jwst_instruments: list[sdk.TelescopeInstrument]
) -> sdk.Telescope:
    fake_telescope.id = "jwst_telescope_id"

    fake_telescope.instruments = fake_jwst_instruments

    return fake_telescope


@pytest.fixture
def fake_jwst_instruments() -> list[sdk.TelescopeInstrument]:
    return [
        sdk.TelescopeInstrument(
            id="miri_instrument_id",
            name="MIRI",
            short_name="JWST_MIRI",
            created_on=datetime.now(),
        ),
        sdk.TelescopeInstrument(
            id="nircam_instrument_id",
            name="NIRCAM",
            short_name="JWST_NIRCAM",
            created_on=datetime.now(),
        ),
        sdk.TelescopeInstrument(
            id="niriss_instrument_id",
            name="NIRISS",
            short_name="JWST_NIRISS",
            created_on=datetime.now(),
        ),
        sdk.TelescopeInstrument(
            id="nirspec_instrument_id",
            name="NIRSPEC",
            short_name="JWST_NIRSPEC",
            created_on=datetime.now(),
        ),
    ]
