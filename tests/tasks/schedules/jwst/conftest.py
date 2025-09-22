from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
import structlog

from across_data_ingestion.util.across_server import sdk


## SET DATA FROM TOP-LEVEL FIXTURES ##
@pytest.fixture(autouse=True)
def set_telescope(
    mock_telescope_api: MagicMock,
    fake_jwst_telescope: sdk.Telescope,
    mock_instrument_api: MagicMock,
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
    fake_telescope: sdk.Telescope, fake_jwst_instruments: list[sdk.IDNameSchema]
) -> sdk.Telescope:
    fake_telescope.id = "jwst_telescope_id"

    fake_telescope.instruments = fake_jwst_instruments

    return fake_telescope


@pytest.fixture
def fake_jwst_instruments() -> list[sdk.IDNameSchema]:
    return [
        sdk.IDNameSchema(
            id="miri_instrument_id",
            name="MIRI",
            short_name="JWST_MIRI",
        ),
        sdk.IDNameSchema(
            id="nircam_instrument_id",
            name="NIRCAM",
            short_name="JWST_NIRCAM",
        ),
        sdk.IDNameSchema(
            id="niriss_instrument_id",
            name="NIRISS",
            short_name="JWST_NIRISS",
        ),
        sdk.IDNameSchema(
            id="nirspec_instrument_id",
            name="NIRSPEC",
            short_name="JWST_NIRSPEC",
        ),
    ]


# @pytest.fixture
# def fake_create_many_schedules(
#     fake_tess_telescope: sdk.Telescope,
# ) -> dict[str, sdk.ScheduleCreateMany]:
#     return {
#         "placeholder_observations": sdk.ScheduleCreateMany(
#             schedules=mocks.placeholder_observations.ACROSS_schedule_output.expected,
#             telescope_id=fake_tess_telescope.id,
#         ),
#         "orbit_observations": sdk.ScheduleCreateMany(
#             schedules=mocks.orbit_observations.ACROSS_schedule_output.expected,
#             telescope_id=fake_tess_telescope.id,
#         ),
#     }
