import os
from collections.abc import Generator
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import structlog
from astropy.io import ascii  # type: ignore[import-untyped]
from astropy.table import Table  # type: ignore[import-untyped]
from astroquery.heasarc import Heasarc  # type: ignore[import-untyped]

from across_data_ingestion.util.across_server import sdk


## SET DATA FROM TOP-LEVEL FIXTURES ##
@pytest.fixture(autouse=True)
def set_telescope(
    mock_telescope_api: MagicMock,
    fake_nustar_telescope: sdk.Telescope,
    mock_instrument_api: MagicMock,
    fake_nustar_instrument: sdk.Instrument,
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_nustar_telescope]
    mock_instrument_api.get_instruments.return_value = [fake_nustar_instrument]


MOCK_FILE_BASE_PATH = os.path.join(os.path.dirname(__file__), "mocks/")
MOCK_OBSERVATION_TABLE_NAME = "NUMASTER_mock_table.ascii"
MOCK_SCHEDULE_OUTPUT_JSON = "nustar_as_flown_mock_schedule_output.json"


## MOCK BEHAVIOR ##
@pytest.fixture
def mock_logger() -> Generator[MagicMock]:
    # must be patched because it is set at runtime when the file is imported.
    with patch(
        "across_data_ingestion.tasks.schedules.nustar.as_flown.logger",
        MagicMock(spec=structlog.stdlib.BoundLogger),
    ) as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def mock_heasarc_query_tap(
    monkeypatch: pytest.MonkeyPatch, fake_observation_table: Table
) -> Generator[MagicMock]:
    mock_res = MagicMock()
    mock_res.to_table = MagicMock(return_value=fake_observation_table)

    mock_heasarc_query_tap = MagicMock(return_value=mock_res)

    monkeypatch.setattr(Heasarc, "query_tap", mock_heasarc_query_tap)

    return mock_heasarc_query_tap


## FAKE DATA ##
@pytest.fixture
def fake_observation_table() -> Table:
    return Table(ascii.read(MOCK_FILE_BASE_PATH + MOCK_OBSERVATION_TABLE_NAME))


@pytest.fixture
def fake_nustar_telescope(fake_telescope: sdk.Telescope) -> sdk.Telescope:
    fake_telescope.id = "telescope_id"

    for i in fake_telescope.instruments or []:
        i.id = "instrument_id"

    return fake_telescope


@pytest.fixture
def fake_nustar_instrument() -> sdk.Instrument:
    instrument = sdk.Instrument(
        id="instrument_id",
        telescope=sdk.IDNameSchema(
            id="telescope_id", name="NUSTAR Telescope", short_name="NUSTAR"
        ),
        created_on=datetime.now(),
        name="nustar instrument",
        short_name="FAKE_INSTRUMENT",
    )

    return instrument
